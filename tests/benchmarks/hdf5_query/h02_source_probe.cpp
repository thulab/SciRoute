// Diagnostic source-backed native H5Dread. The only local-file access is this
// harness's bounded pread callback; the production driver receives no pathname.
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include <cerrno>
#include <charconv>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

using namespace iotdbfs;
namespace {
std::string quote(const std::string& value) {
  std::string out = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') { out += '\\'; out += c; }
    else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
    else out += c;
  }
  return out + '"';
}
std::vector<std::uint64_t> numbers(const std::string& text) {
  std::vector<std::uint64_t> out;
  std::size_t begin = 0;
  do {
    auto end = text.find(',', begin);
    if (end == std::string::npos) end = text.size();
    std::uint64_t value{};
    auto parsed = std::from_chars(text.data()+begin, text.data()+end, value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data()+end || end == begin)
      throw std::runtime_error("invalid unsigned coordinate");
    out.push_back(value); begin = end + 1;
  } while (begin <= text.size());
  return out;
}
struct File { int fd{-1}; ~File() { if (fd >= 0) ::close(fd); } };
template<class T> void write_values(std::ostream& out, const std::vector<T>& values) {
  const std::uint16_t one = 1;
  const bool little = *reinterpret_cast<const std::uint8_t*>(&one) != 0;
  for (const auto& value : values) {
    const auto* bytes = reinterpret_cast<const char*>(&value);
    if (little) out.write(bytes, sizeof(T));
    else for (std::size_t i=sizeof(T); i>0; --i) out.put(bytes[i-1]);
  }
}
}
int main(int argc, char** argv) {
  try {
    std::map<std::string,std::string> args;
    for (int i=1; i<argc; i+=2) {
      if (i+1==argc || !args.emplace(argv[i],argv[i+1]).second)
        throw std::runtime_error("invalid or repeated argument");
    }
    for (const char* key : {"--file","--dataset","--start","--count","--output","--values-output"})
      if (!args.count(key)) throw std::runtime_error(std::string("missing ")+key);
    if (args.size()!=6) throw std::runtime_error("unknown argument");
    File file; file.fd=::open(args.at("--file").c_str(),O_RDONLY|O_CLOEXEC);
    struct stat st{};
    if (file.fd<0 || ::fstat(file.fd,&st)!=0 || st.st_size<0 || !S_ISREG(st.st_mode))
      throw std::runtime_error("cannot open regular source file");
    FormatSource source; source.size=static_cast<std::uint64_t>(st.st_size);
    std::uint64_t callback_calls=0, callback_bytes=0;
    source.read=[&](std::uint64_t offset,std::uint64_t length)->std::optional<std::vector<std::uint8_t>> {
      if (offset>source.size || length>source.size-offset) return std::nullopt;
      ++callback_calls; callback_bytes+=length;
      std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length));
      std::size_t done=0;
      while (done<bytes.size()) {
        const auto n=::pread(file.fd,bytes.data()+done,bytes.size()-done,static_cast<off_t>(offset+done));
        if (n<0 && errno==EINTR) continue;
        if (n<=0) return std::nullopt;
        done+=static_cast<std::size_t>(n);
      }
      return bytes;
    };
    auto origin=numbers(args.at("--start")), shape=numbers(args.at("--count"));
    Hdf5SourceTrace trace;
    std::uint64_t elements=0;
    std::string dtype;
    Hdf5SourceOptions options;
    auto status=with_hdf5_ingestion_source(source,options,[&](const Hdf5SourceDriver& driver) {
      auto result=driver.read(args.at("--dataset"),origin,shape);
      trace=driver.trace();
      if (!result.ok()) return result.status();
      std::ofstream values(args.at("--values-output"),std::ios::binary|std::ios::trunc);
      if (!values) throw std::runtime_error("cannot create values output");
      const auto& array=result.value().values;
      elements=logical_array_length(array);
      if (array.type.id==LogicalTypeId::Float32) { dtype="<f4"; write_values(values,std::get<std::vector<float>>(array.values)); }
      else { dtype="<f8"; write_values(values,std::get<std::vector<double>>(array.values)); }
      values.close();
      if (!values) throw std::runtime_error("cannot write values output");
      return QueryStatus::success();
    });
    std::ofstream report(args.at("--output"),std::ios::trunc);
    report << "{\"schema_version\":\"iotdbfs-hdf5-h02-source-probe/1\",\"status\":" << quote(status.ok()?"ok":"error")
      << ",\"error_code\":" << quote(status.code()==QueryError::UnsupportedCapability?"UnsupportedCapability":status.code()==QueryError::CorruptPayload?"CorruptPayload":status.ok()?"None":"Other")
      << ",\"error\":" << quote(status.message()) << ",\"source_bytes\":" << source.size
      << ",\"output_elements\":" << elements << ",\"dtype\":" << quote(dtype)
      << ",\"callback_calls\":" << callback_calls << ",\"callback_bytes\":" << callback_bytes
      << ",\"metadata_bytes\":" << trace.metadata_bytes << ",\"raw_data_bytes\":" << trace.raw_data_bytes
      << ",\"metadata_cache_bytes\":" << trace.metadata_cache_bytes
      << ",\"raw_chunk_cache_bytes\":" << trace.raw_chunk_cache_bytes
      << ",\"sieve_buffer_bytes\":" << trace.sieve_buffer_bytes
      << ",\"conversion_buffer_bytes\":" << trace.conversion_buffer_bytes << ",\"reads\":[";
    for (std::size_t i=0;i<trace.reads.size();++i) {
      const auto& r=trace.reads[i]; if(i) report << ',';
      report << "{\"offset\":" << r.offset << ",\"length\":" << r.length
        << ",\"raw_data\":" << (r.raw_data?"true":"false") << ",\"success\":" << (r.success?"true":"false") << '}';
    }
    report << "]}\n"; report.close();
    if (!report) throw std::runtime_error("cannot write report");
    return status.ok()?0:1;
  } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 2; }
}
