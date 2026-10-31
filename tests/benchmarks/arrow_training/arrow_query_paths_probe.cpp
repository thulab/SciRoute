// Read-only, non-training Arrow A/B/C/D attribution probe. Every timed query
// consumes owned Float32 + byte-validity columns. Hashing is outside timing.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"
#include "iotdbfs/query/core/QueryModule.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/message.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <openssl/sha.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace {
using namespace iotdbfs;
using Clock = std::chrono::steady_clock;
using Json = boost::property_tree::ptree;
constexpr std::uint64_t cache_budget = 1024ULL * 1024 * 1024;
void require(bool ok, const std::string& why) { if (!ok) throw std::runtime_error(why); }
std::uint64_t ns(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}
void checked(const arrow::Status& status) { require(status.ok(), status.ToString()); }
template<class T> T checked(arrow::Result<T> result) {
  require(result.ok(), result.status().ToString()); return std::move(result).ValueOrDie();
}
std::string quote(const std::string& value) {
  static constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') { result += '\\'; result += c; }
    else if (c < 32) { result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
    else result += c;
  }
  return result + '"';
}
std::string trim(std::string value) {
  const auto first = value.find_first_not_of(" \r\n\t");
  return first == std::string::npos ? "" :
    value.substr(first, value.find_last_not_of(" \r\n\t") - first + 1);
}
std::map<std::string, std::string> properties(const std::string& path) {
  std::ifstream file(path); require(bool(file), "cannot read config");
  std::map<std::string, std::string> result; std::string line;
  while (std::getline(file, line)) {
    line = trim(line); if (line.empty() || line[0] == '#') continue;
    const auto pos = line.find('=');
    if (pos != std::string::npos) result[trim(line.substr(0,pos))] = trim(line.substr(pos+1));
  }
  return result;
}
std::string setting(const std::map<std::string, std::string>& props,
    const std::string& key, const char* env, const char* fallback = nullptr) {
  auto found = props.find(key); if (found != props.end()) return found->second;
  if (env) { const char* value = std::getenv(env); if (value) return value; }
  if (fallback) return fallback;
  throw std::runtime_error("missing config property " + key);
}
std::uint64_t number(const std::string& text) {
  require(!text.empty() && text.find_first_not_of("0123456789") == std::string::npos,
    "expected nonnegative decimal integer");
  return std::stoull(text);
}
struct Case {
  struct Filter { std::uint64_t child; float threshold; };
  std::string id, field;
  std::uint64_t batch, row, begin, length;
  std::vector<std::uint64_t> children;
  std::vector<std::uint64_t> client_batches;
  std::optional<Filter> filter;
};
std::vector<Case> load_cases(const std::string& path) {
  Json root; boost::property_tree::read_json(path, root);
  const auto wrapped = root.get_child_optional("cases");
  const auto& rows = wrapped ? *wrapped : root;
  std::vector<Case> result;
  std::set<std::string> ids;
  for (const auto& item : rows) {
    require(item.first.empty(), "cases must be a JSON array"); const auto& node = item.second;
    Case c;
    c.id=node.get<std::string>("id"); c.field=node.get<std::string>("field");
    c.batch=number(node.get<std::string>("batch")); c.row=number(node.get<std::string>("row"));
    c.begin=number(node.get<std::string>("begin")); c.length=number(node.get<std::string>("length"));
    require(!c.id.empty() && ids.insert(c.id).second && !c.field.empty() && c.length &&
      c.begin <= std::numeric_limits<std::uint64_t>::max() - c.length, "invalid case");
    for (const auto& child : node.get_child("children")) {
      require(child.first.empty(), "children must be an array");
      c.children.push_back(number(child.second.get_value<std::string>()));
    }
    if (const auto filter=node.get_child_optional("filter")) {
      require(filter->get<std::string>("op")=="gt", "probe supports only gt filters");
      c.filter=Case::Filter{number(filter->get<std::string>("child")), filter->get<float>("threshold")};
      require(std::isfinite(c.filter->threshold),"filter threshold must be finite Float32");
      require(!c.children.empty()||c.filter->child==0,"single-list filter child must be zero");
    }
    require(std::set<std::uint64_t>(c.children.begin(),c.children.end()).size()==c.children.size(),"duplicate projected child");
    if (const auto batches=node.get_child_optional("client_batches"))
      for (const auto& item:*batches) {
        require(item.first.empty(),"client_batches must be an array");
        c.client_batches.push_back(number(item.second.get_value<std::string>()));
      }
    require(c.children.size() <= 4096 && c.length <= 100000000, "case exceeds probe limits");
    result.push_back(std::move(c));
  }
  require(!result.empty(), "no cases"); return result;
}
struct Counts { std::uint64_t manifests=0, payload_calls=0, payload_ranges=0, payload_bytes=0, layout_calls=0, layout_bytes=0; };
class ReadBackend final : public StructuredBlobStorageBackend {
public:
  ReadBackend(StructuredBlobManifest manifest, BinaryValue layout, std::string path,
      std::shared_ptr<IoTDBStructuredBlobBackend> remote)
    : manifest_(std::move(manifest)), layout_(std::move(layout)), remote_(std::move(remote)) {
    if (!remote_) {
      fd_ = ::open(path.c_str(), O_RDONLY); require(fd_ >= 0, "cannot open local payload");
      struct stat stat{}; require(::fstat(fd_, &stat) == 0 && stat.st_size >= 0 &&
        static_cast<std::uint64_t>(stat.st_size) == manifest_.payload_size, "local source size differs from manifest");
    }
  }
  ~ReadBackend() override { if (fd_ >= 0) ::close(fd_); }
  Counts counts() const { return {manifests_, payload_calls_, payload_ranges_, payload_bytes_, layout_calls_, layout_bytes_}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    ++manifests_;
    if (remote_) return remote_->load_manifest(key);
    if (!(key == manifest_.key)) return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound,"source absent");
    return StructuredBlobResult<StructuredBlobManifest>::success(manifest_);
  }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    ++payload_calls_; ++payload_ranges_; payload_bytes_ += length;
    if (remote_) return remote_->read_payload_range(key,offset,length);
    if (!(key == manifest_.key) || offset > manifest_.payload_size || length > manifest_.payload_size-offset)
      return failed("invalid local payload range");
    StructuredBlobBytes bytes(length); std::size_t done = 0;
    while (done < bytes.size()) {
      const auto read = ::pread(fd_, bytes.data()+done, bytes.size()-done, static_cast<off_t>(offset+done));
      if (read <= 0) return failed("short local payload read"); done += read;
    }
    return StructuredBlobResult<StructuredBlobBytes>::success(std::move(bytes));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    ++layout_calls_; layout_bytes_ += length;
    if (remote_) return remote_->read_layout_range(key,offset,length);
    if (!(key == manifest_.key) || offset > layout_.size() || length > layout_.size()-offset)
      return failed("invalid local layout range");
    return StructuredBlobResult<StructuredBlobBytes>::success({layout_.begin()+offset, layout_.begin()+offset+length});
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return readonly(); }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&,std::uint64_t,const StructuredBlobManifest&) override { return readonly(); }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,std::uint64_t,const StructuredBlobBytes&) override { return readonly(); }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,const StructuredBlobBytes&) override { return readonly(); }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,const MetadataMap&) override { return readonly(); }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&,std::uint64_t) override { return readonly(); }
private:
  static StructuredBlobStatus readonly() { return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,"read-only probe backend"); }
  static StructuredBlobResult<StructuredBlobBytes> failed(const char* message) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::BackendFailure,message);
  }
  StructuredBlobManifest manifest_; BinaryValue layout_;
  std::shared_ptr<IoTDBStructuredBlobBackend> remote_; int fd_{-1};
  mutable std::atomic<std::uint64_t> manifests_{0},payload_calls_{0},payload_ranges_{0},payload_bytes_{0},layout_calls_{0},layout_bytes_{0};
};
struct Output {
  std::vector<std::uint64_t> index;
  std::vector<std::vector<float>> values;
  std::vector<std::vector<std::uint8_t>> valid;
};
struct NativeSpan { std::shared_ptr<arrow::Array> values; std::int64_t begin=0, length=0; bool valid=false; };
NativeSpan span(const std::shared_ptr<arrow::Array>& array, std::uint64_t row) {
  require(row < static_cast<std::uint64_t>(array->length()), "native row/child index out of bounds");
  if (array->type_id() == arrow::Type::LIST) {
    const auto list=std::static_pointer_cast<arrow::ListArray>(array);
    return {list->values(),list->value_offset(row),list->value_length(row),list->IsValid(row)};
  }
  if (array->type_id() == arrow::Type::LARGE_LIST) {
    const auto list=std::static_pointer_cast<arrow::LargeListArray>(array);
    return {list->values(),list->value_offset(row),list->value_length(row),list->IsValid(row)};
  }
  throw std::runtime_error("native field is not List/LargeList");
}
class NativeReader {
public:
  NativeReader(const std::string& path, const std::string& io, std::uint64_t memory_budget)
    : memory_budget_(memory_budget) {
    if (io == "mmap") input_=checked(arrow::io::MemoryMappedFile::Open(path,arrow::io::FileMode::READ));
    else input_=checked(arrow::io::ReadableFile::Open(path));
    auto stream=checked(arrow::ipc::RecordBatchStreamReader::Open(input_)); schema_=stream->schema();
    checked(input_->Seek(0));
    const auto size=checked(input_->GetSize());
    for (;;) {
      // Index framing metadata only. In read mode, scanning complete messages
      // would unnecessarily read the entire payload before the first case.
      const auto offset=checked(input_->Tell());
      auto prefix=checked(input_->Read(4));
      if (prefix->size()==0) break;
      require(prefix->size()==4,"truncated native IPC prefix");
      auto u32=[](const std::shared_ptr<arrow::Buffer>& bytes) {
        const auto* p=bytes->data();return std::uint32_t(p[0])|(std::uint32_t(p[1])<<8)|
          (std::uint32_t(p[2])<<16)|(std::uint32_t(p[3])<<24);
      };
      auto length=u32(prefix);
      if (length==0xffffffffU) {prefix=checked(input_->Read(4));require(prefix->size()==4,"truncated native IPC continuation");length=u32(prefix);}
      if (!length) break;
      require(length<=64ULL*1024*1024,"native IPC metadata exceeds bound");
      auto metadata=checked(input_->Read(length));require(metadata->size()==length,"truncated native IPC metadata");
      auto message=checked(arrow::ipc::Message::Open(metadata,nullptr));
      require(message->type() != arrow::ipc::MessageType::DICTIONARY_BATCH,"native dictionary streams unsupported");
      if (message->type()==arrow::ipc::MessageType::RECORD_BATCH) offsets_.push_back(offset);
      const auto body=checked(input_->Tell());
      require(message->body_length()>=0&&body<=size&&message->body_length()<=size-body,"native IPC body exceeds file");
      checked(input_->Seek(body+message->body_length()));
    }
    require(!offsets_.empty(),"native stream has no record batches");
  }
  void reset() { cached_.reset(); cached_index_=std::numeric_limits<std::uint64_t>::max(); }
  std::shared_ptr<arrow::RecordBatch> read(std::uint64_t index, bool& hit, std::uint64_t& body_bytes) {
    require(index<offsets_.size(),"native batch index out of bounds");
    hit = cached_ && cached_index_==index; body_bytes=0;
    if (hit) return cached_;
    checked(input_->Seek(offsets_[index])); auto message=checked(arrow::ipc::ReadMessage(input_.get()));
    require(bool(message),"native batch message disappeared");
    body_bytes=message->body() ? message->body()->size() : 0;
    require(body_bytes<=memory_budget_,"native record batch exceeds per-client cache bound");
    arrow::ipc::DictionaryMemo memo;
    auto batch=checked(arrow::ipc::ReadRecordBatch(*message,schema_,&memo,arrow::ipc::IpcReadOptions::Defaults()));
    checked(batch->ValidateFull());
    // The experimental sources are uncompressed; reject compressed bodies
    // whose decoded allocation would exceed the shared cache budget too.
    std::uint64_t bytes=0;
    std::set<const arrow::Buffer*> seen;
    std::function<void(const std::shared_ptr<arrow::ArrayData>&)> count = [&](const auto& data) {
      for (const auto& buffer : data->buffers) if (buffer && seen.insert(buffer.get()).second) {
        require(static_cast<std::uint64_t>(buffer->size())<=memory_budget_-bytes,"native decoded batch exceeds per-client cache bound");
        bytes += buffer->size();
      }
      for (const auto& child:data->child_data) count(child);
    };
    for (const auto& column:batch->columns()) count(column->data());
    cached_=std::move(batch); cached_index_=index; return cached_;
  }
private:
  std::shared_ptr<arrow::io::RandomAccessFile> input_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::int64_t> offsets_;
  std::shared_ptr<arrow::RecordBatch> cached_;
  std::uint64_t cached_index_{std::numeric_limits<std::uint64_t>::max()};
  std::uint64_t memory_budget_;
};
Output consume_native(const std::shared_ptr<arrow::RecordBatch>& batch,const Case& c) {
  const auto array=batch->GetColumnByName(c.field); require(bool(array),"native field absent");
  auto parent=span(array,c.row); std::vector<NativeSpan> leaves;
  auto resolve = [&](std::uint64_t child) -> NativeSpan {
    if (!parent.valid) return {{},0,0,false};
    require(child<static_cast<std::uint64_t>(parent.length),"native child out of bounds");
    return span(parent.values,parent.begin+child);
  };
  if (c.children.empty()) leaves.push_back(parent);
  else for (auto child:c.children) leaves.push_back(resolve(child));
  const auto projected=leaves.size();
  std::size_t filter_column=0;
  if (c.filter && !c.children.empty()) {
    const auto found=std::find(c.children.begin(),c.children.end(),c.filter->child);
    if (found==c.children.end()) { filter_column=leaves.size(); leaves.push_back(resolve(c.filter->child)); }
    else filter_column=static_cast<std::size_t>(found-c.children.begin());
  }
  std::optional<std::uint64_t> length;
  for (const auto& leaf:leaves) if (leaf.valid) {
    require(leaf.values->type_id()==arrow::Type::FLOAT,"native leaf not Float32");
    if (length) require(*length==static_cast<std::uint64_t>(leaf.length),"ragged native children");
    length=leaf.length;
  }
  const auto n=length && *length>c.begin ? std::min(c.length,*length-c.begin) : 0;
  Output output; output.index.reserve(n); output.values.resize(projected); output.valid.resize(projected);
  for (std::uint64_t i=0;i<n;++i) {
    if (c.filter) {
      const auto& leaf=leaves[filter_column]; if (!leaf.valid) continue;
      const auto values=std::static_pointer_cast<arrow::FloatArray>(leaf.values);
      const auto row=leaf.begin+c.begin+i;
      // SQL WHERE keeps true only: null and unordered NaN comparisons drop.
      if (!values->IsValid(row) || !std::isfinite(values->Value(row)) ||
          !(values->Value(row)>c.filter->threshold)) continue;
    }
    output.index.push_back(c.begin+i);
  }
  for (std::size_t channel=0;channel<projected;++channel) {
    auto& values=output.values[channel]; auto& valid=output.valid[channel];
    values.resize(output.index.size());valid.resize(output.index.size());
    const auto& leaf=leaves[channel]; if (!leaf.valid) continue;
    const auto values_array=std::static_pointer_cast<arrow::FloatArray>(leaf.values);
    for (std::size_t i=0;i<output.index.size();++i) {
      const auto row=leaf.begin+output.index[i];valid[i]=values_array->IsValid(row);
      if(valid[i])values[i]=values_array->Value(row);
    }
  }
  return output;
}
QueryRequest request(const StructuredBlobManifest& manifest,const Case& c) {
  QueryRequest request; request.sources.paths={{PathScopeKind::Exact,manifest.key.path}};
  request.sources.versions=VersionSelection::exact_version(manifest.key.version);
  request.sources.formats.adapters=AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids={"arrow"};request.sources.formats.include_byte_passthrough=false;
  QueryFragment fragment; std::vector<ArrayValuePath> paths;
  if(c.children.empty()) paths.push_back({{}}); else for(auto child:c.children) paths.push_back({{child}});
  if(c.filter && !c.children.empty() &&
      std::find(c.children.begin(),c.children.end(),c.filter->child)==c.children.end())
    paths.push_back({{c.filter->child}});
  fragment.physical_array=PhysicalArraySubject{c.field,c.batch,c.row,paths};
  if(c.filter) fragment.predicate=make_comparison(
    {c.children.empty()?"value":"value."+std::to_string(c.filter->child)},
    ComparisonOperator::Greater,LogicalValue::float32(c.filter->threshold));
  fragment.projection={{"index"}};
  if(c.children.empty()) fragment.projection.push_back({"value"});
  else for(auto child:c.children) fragment.projection.push_back({"value."+std::to_string(child)});
  fragment.slices={{{"index"},DomainBound{LogicalValue::uint64(c.begin),BoundKind::Inclusive},DomainBound{LogicalValue::uint64(c.begin+c.length),BoundKind::Exclusive}}};
  request.subset=std::move(fragment); return request;
}
void append(Output& output,const QueryBatch& batch) {
  require(batch.columns().size()==output.values.size()+1,"unexpected output schema");
  const auto& positions=std::get<std::vector<std::uint64_t>>(batch.columns()[0].values);
  output.index.insert(output.index.end(),positions.begin(),positions.end());
  for(std::size_t c=0;c<output.values.size();++c) {
    const auto& array=batch.columns()[c+1]; const auto& values=std::get<std::vector<float>>(array.values);
    auto& out=output.values[c]; auto& valid=output.valid[c]; const auto before=out.size();
    out.resize(before+batch.row_count());valid.resize(before+batch.row_count());
    for(std::size_t i=0;i<batch.row_count();++i) {
      valid[before+i]=array.validity.empty() || (array.validity.at(i/8)&(1u<<(i%8)));
      if(valid[before+i]) out[before+i]=values[i];
    }
  }
}
std::string finish(SHA256_CTX& state) {
  unsigned char hash[SHA256_DIGEST_LENGTH];SHA256_Final(hash,&state);
  static constexpr char hex[]="0123456789abcdef"; std::string result;
  for(auto v:hash) {result+=hex[v>>4];result+=hex[v&15];}return result;
}
void little(SHA256_CTX& state,std::uint64_t value,unsigned count) {
  unsigned char bytes[8];for(unsigned i=0;i<count;++i) bytes[i]=static_cast<unsigned char>(value>>(8*i));SHA256_Update(&state,bytes,count);
}
std::pair<std::string,std::string> digest(const Output& output,const Case& c) {
  SHA256_CTX values,index;SHA256_Init(&values);SHA256_Init(&index);
  for(std::size_t i=0;i<output.index.size();++i) {
    require(output.index[i]>=c.begin && output.index[i]<c.begin+c.length &&
      (!i || output.index[i]>output.index[i-1]) && (c.filter || output.index[i]==c.begin+i),
      "returned index differs from requested position/order");little(index,output.index[i],8);
  }
  for(std::size_t channel=0;channel<output.values.size();++channel) {
    require(output.values[channel].size()==output.index.size()&&output.valid[channel].size()==output.index.size(),"output shape mismatch");
    for(float value:output.values[channel]) {std::uint32_t bits;std::memcpy(&bits,&value,4);little(values,bits,4);}
    SHA256_Update(&values,output.valid[channel].data(),output.valid[channel].size());
  }
  return {finish(values),finish(index)};
}
void write_counts(std::ostream& out,const Counts& now,const Counts& before) {
  out<<"\"manifest_calls\":"<<now.manifests-before.manifests<<",\"payload_calls\":"<<now.payload_calls-before.payload_calls
    <<",\"payload_ranges\":"<<now.payload_ranges-before.payload_ranges<<",\"payload_bytes\":"<<now.payload_bytes-before.payload_bytes
    <<",\"layout_calls\":"<<now.layout_calls-before.layout_calls<<",\"layout_bytes\":"<<now.layout_bytes-before.layout_bytes;
}
void write_stats(std::ostream& out,const QueryRuntimeStats& now,const QueryRuntimeStats& before) {
#define COUNTER(name) out<<",\"" #name "\":"<<now.name-before.name
  COUNTER(hits);COUNTER(misses);COUNTER(waits);COUNTER(producer_count);COUNTER(decode_count);COUNTER(decoded_bytes);COUNTER(decompressions);
  COUNTER(read_bytes);COUNTER(read_ranges);COUNTER(copy_bytes);COUNTER(evictions);COUNTER(redecodes);
  COUNTER(payload_read_calls);COUNTER(payload_read_nanoseconds);COUNTER(fragment_load_nanoseconds);
#undef COUNTER
  out<<",\"cache_owned_bytes\":"<<now.cache_owned_bytes<<",\"live_bytes\":"<<now.live_bytes<<",\"peak_bytes\":"<<now.peak_bytes;
}
struct Observation {
  Output output;
  std::uint64_t prepare_ns=0,execute_ns=0,consume_ns=0,wall_ns=0,native_body_bytes=0;
  bool native_hit=false;
  Clock::time_point started,finished;
  std::string error;
};
Observation run_query(const std::shared_ptr<const QueryModule>& module,
    NativeReader* native,const StructuredBlobManifest& manifest,const Case& c) {
  Observation result;result.started=Clock::now();
  try {
    if(native) {
      const auto execution=Clock::now();auto batch=native->read(c.batch,result.native_hit,result.native_body_bytes);result.execute_ns=ns(execution);
      const auto consumption=Clock::now();result.output=consume_native(batch,c);result.consume_ns=ns(consumption);
    } else {
      const auto planning=Clock::now();auto prepared=module->prepare(request(manifest,c));require(prepared.ok(),prepared.status().message());result.prepare_ns=ns(planning);
      const auto execution=Clock::now();auto cursor=module->execute(prepared.value());require(cursor.ok(),cursor.status().message());result.execute_ns=ns(execution);
      result.output.values.resize(c.children.empty()?1:c.children.size());result.output.valid.resize(result.output.values.size());
      for(;;) {const auto pull=Clock::now();auto batch=cursor.value()->next();result.execute_ns+=ns(pull);require(batch.ok(),batch.status().message());if(!batch.value())break;
        const auto consumption=Clock::now();append(result.output,*batch.value());result.consume_ns+=ns(consumption);}
    }
  } catch(const std::exception& error) {result.error=error.what();}
  result.finished=Clock::now();
  result.wall_ns=std::chrono::duration_cast<std::chrono::nanoseconds>(result.finished-result.started).count();
  return result;
}
} // namespace

int main(int argc,char** argv) {
  try {
    std::map<std::string,std::string> args;
    for(int i=1;i<argc;i+=2) { require(i+1<argc,"options need values");require(args.emplace(argv[i],argv[i+1]).second,"duplicate option"); }
    auto arg=[&](const char* key)->std::string {auto it=args.find(key);require(it!=args.end(),std::string("missing ")+key);return it->second;};
    const auto path=arg("--path");require(path=="A"||path=="B"||path=="C"||path=="D","path must be A/B/C/D");
    const auto io=args.count("--native-io")?args.at("--native-io"):"mmap";require(io=="mmap"||io=="read","native-io must be mmap/read");
    const auto repeats=args.count("--repeats")?number(args.at("--repeats")):10;require(repeats>=1&&repeats<=1000,"invalid repeats");
    const auto clients=args.count("--clients")?number(args.at("--clients")):1;
    require(clients==1||clients==2||clients==4,"clients must be 1, 2 or 4");
    const auto cases=load_cases(arg("--cases"));
    for(const auto& c:cases)require(c.client_batches.empty()||c.client_batches.size()>=clients,"client_batches lacks per-client batch");std::ofstream out(arg("--output"));require(bool(out),"cannot open output");
    const auto setup=Clock::now();const auto props=properties(arg("--config"));IoTDBConfig config;
    config.host=setting(props,"iotdb.host","IOTDB_HOST","127.0.0.1");config.port=std::stoi(setting(props,"iotdb.port","IOTDB_PORT","6667"));
    config.user=setting(props,"iotdb.user","IOTDB_USER","root");config.pass=props.count("iotdb.pass")?props.at("iotdb.pass"):setting(props,"iotdb.password","IOTDB_PASS");
    config.db=setting(props,"iotdb.db",nullptr);IoTDBConnector::instance().initialize(config,clients);
    auto remote=std::make_shared<IoTDBStructuredBlobBackend>();const StructuredBlobVersionKey key{arg("--logical"),std::stoll(arg("--version"))};
    auto loaded=remote->load_manifest(key);require(loaded.ok(),"cannot load exact manifest");const auto manifest=loaded.value();
    require(manifest.published&&manifest.phase==StructuredBlobPhase::Published&&manifest.format_id==std::optional<std::string>{"arrow"},"source is not published Arrow");
    auto layout=remote->read_layout_range(key,0,manifest.layout_size);require(layout.ok()&&layout.value().size()==manifest.layout_size,"cannot read exact layout");
    auto parsed=parse_arrow_layout(layout.value(),manifest.payload_size);require(parsed.ok(),"invalid stored Arrow layout");
    // Shared exact catalog deliberately excludes live catalog scan cost. Each
    // C prepare/execute still opens and verifies actual remote manifests.
    auto catalog=make_in_memory_published_version_catalog({manifest});
    std::vector<std::unique_ptr<NativeReader>> native;std::shared_ptr<ReadBackend> backend;std::shared_ptr<StructuredBlobStorage> storage;
    if(path=="A"||path=="D") {
      for(std::uint64_t client=0;client<clients;++client)
        native.push_back(std::make_unique<NativeReader>(arg("--source"),io,cache_budget/clients));
    }
    else {backend=std::make_shared<ReadBackend>(manifest,std::move(layout.value()),arg("--source"),path=="C"?remote:nullptr);storage=std::make_shared<StructuredBlobStorage>(backend);}
    out<<"{\"type\":\"setup\",\"path\":"<<quote(path)<<",\"native_io\":"<<quote(io)<<",\"setup_ns\":"<<ns(setup)
      <<",\"payload_size\":"<<manifest.payload_size<<",\"layout_size\":"<<manifest.layout_size<<",\"revision\":"<<manifest.revision
      <<",\"version\":"<<manifest.key.version<<",\"cache_budget_bytes\":"<<cache_budget
      <<",\"clients\":"<<clients<<",\"native_per_client_cache_budget_bytes\":"<<cache_budget/clients
      <<",\"catalog\":\"static_exact_published_manifest\",\"cold_definition\":\"reader_or_decoded_cache_only\"}\n";out.flush();
    bool failures=false;
    for(const auto& c:cases) {
      std::shared_ptr<QueryRuntime> runtime;std::shared_ptr<const QueryModule> module;
      const auto reset=Clock::now();
      if(!native.empty()) {for(auto& reader:native)reader->reset();} else {
        QueryRuntimeConfig runtime_config;runtime_config.memory_budget_bytes=cache_budget;runtime_config.max_entries=16384;runtime_config.max_inflight=256;runtime_config.worker_threads=4;runtime_config.wait_timeout_ms=30000;
        runtime=std::make_shared<QueryRuntime>(runtime_config);ArrowDecodeConfig decode;decode.tile_elements=4096;
        auto created=QueryModule::create(catalog,storage,{std::make_shared<ArrowFormatQueryAdapter>(decode,runtime)});
        require(created.ok(),created.status().message());module=std::move(created.value());
      }
      const auto reset_ns=ns(reset);
      std::vector<Case> selected(clients,c);
      for(std::size_t client=0;client<clients;++client)
        if(!c.client_batches.empty())selected[client].batch=c.client_batches[client];
      for(std::uint64_t repeat=0;repeat<=repeats;++repeat) {
        const auto before=backend?backend->counts():Counts{};
        const auto stats_before=runtime?runtime->stats():QueryRuntimeStats{};
        std::vector<Observation> observations(clients);std::vector<std::thread> threads;
        std::mutex barrier_mutex;std::condition_variable barrier_ready;
        std::size_t ready=0;bool go=false;Clock::time_point wave_start;
        const auto thread_setup=Clock::now();
        for(std::size_t client=0;client<clients;++client) threads.emplace_back([&,client] {
          {std::unique_lock<std::mutex> lock(barrier_mutex);++ready;barrier_ready.notify_all();barrier_ready.wait(lock,[&]{return go;});}
          observations[client]=run_query(module,native.empty()?nullptr:native[client].get(),manifest,selected[client]);
        });
        std::uint64_t thread_setup_ns;
        {std::unique_lock<std::mutex> lock(barrier_mutex);barrier_ready.wait(lock,[&]{return ready==clients;});
          thread_setup_ns=ns(thread_setup);wave_start=Clock::now();go=true;}
        barrier_ready.notify_all();for(auto& thread:threads)thread.join();
        auto wave_end=wave_start;std::uint64_t rows=0,logical_bytes=0,native_reads=0,native_hits=0;
        bool wave_ok=true;
        for(std::size_t client=0;client<clients;++client) {
          const auto& result=observations[client];wave_end=std::max(wave_end,result.finished);
          rows+=result.output.index.size();logical_bytes+=result.native_body_bytes;
          native_hits+=result.native_hit;native_reads+=!native.empty()&&!result.native_hit;
          if(!result.error.empty()) {failures=true;wave_ok=false;out<<"{\"type\":\"error\",\"path\":"<<quote(path)<<",\"case\":"<<quote(c.id)
            <<",\"repeat\":"<<repeat<<",\"client_id\":"<<client<<",\"error\":"<<quote(result.error)<<"}\n";continue;}
          // Hash only after every request has completed, outside wave timing.
          const auto hashes=digest(result.output,selected[client]);
          out<<"{\"type\":\"query\",\"path\":"<<quote(path)<<",\"native_io\":"<<quote(io)<<",\"case\":"<<quote(c.id)
            <<",\"temperature\":"<<quote(repeat?"warm":"cold")<<",\"repeat\":"<<repeat<<",\"client_id\":"<<client<<",\"clients\":"<<clients
            <<",\"batch\":"<<selected[client].batch<<",\"wall_ns\":"<<result.wall_ns<<",\"prepare_ns\":"<<result.prepare_ns
            <<",\"execute_ns\":"<<result.execute_ns<<",\"consume_ns\":"<<result.consume_ns
            <<",\"start_delay_ns\":"<<std::chrono::duration_cast<std::chrono::nanoseconds>(result.started-wave_start).count()
            <<",\"finish_offset_ns\":"<<std::chrono::duration_cast<std::chrono::nanoseconds>(result.finished-wave_start).count()
            <<",\"rows\":"<<result.output.index.size()<<",\"columns\":"<<result.output.values.size()<<",\"digest\":"<<quote(hashes.first)
            <<",\"index_digest\":"<<quote(hashes.second)<<",\"native_batch_hit\":"<<(result.native_hit?"true":"false")
            <<",\"native_message_body_bytes\":"<<result.native_body_bytes<<"}\n";
        }
        out<<"{\"type\":\"wave\",\"path\":"<<quote(path)<<",\"case\":"<<quote(c.id)<<",\"repeat\":"<<repeat
          <<",\"temperature\":"<<quote(repeat?"warm":"cold")<<",\"clients\":"<<clients<<",\"ok\":"<<(wave_ok?"true":"false")
          <<",\"wave_wall_ns\":"<<std::chrono::duration_cast<std::chrono::nanoseconds>(wave_end-wave_start).count()
          <<",\"thread_setup_ns\":"<<thread_setup_ns<<",\"reset_ns\":"<<(repeat?0:reset_ns)<<",\"rows\":"<<rows
          <<",\"native_message_body_bytes\":"<<logical_bytes<<",\"native_batch_reads\":"<<native_reads<<",\"native_batch_hits\":"<<native_hits<<',';
        write_counts(out,backend?backend->counts():Counts{},before);write_stats(out,runtime?runtime->stats():QueryRuntimeStats{},stats_before);
        out<<"}\n";out.flush();if(!wave_ok)break;
      }
    }
    return failures?1:0;
  } catch(const std::exception& error) {std::cerr<<"probe failed: "<<error.what()<<'\n';return 1;}
}
