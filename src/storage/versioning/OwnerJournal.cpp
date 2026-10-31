#include "iotdbfs/storage/versioning/VersionDirectory.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace iotdbfs::versioning {
namespace {
[[noreturn]] void failed(const char* operation) {
  throw Error(ErrorCode::Unavailable,std::string("owner journal ")+operation+": "+std::strerror(errno));
}
void require(bool condition,const char* message) {
  if (!condition) throw Error(ErrorCode::Corrupt,message);
}
void private_file(int fd) {
  struct stat info{};
  if (::fstat(fd,&info)!=0) failed("stat");
  require(S_ISREG(info.st_mode) && info.st_nlink==1 && info.st_uid==::geteuid() &&
    !(info.st_mode&0077),"owner journal must be a private, singly linked regular file");
}
void append_u32(Bytes& out,std::uint32_t value) {
  for (unsigned i=4;i;--i) out.push_back(value>>((i-1)*8));
}
void append_text(Bytes& out,const std::string& text) {
  append_u32(out,text.size()); out.insert(out.end(),text.begin(),text.end());
}
void append_head(Bytes& out,const std::optional<Bytes>& bytes) {
  out.push_back(bool(bytes));
  if (bytes) {
    require(bytes->size()<=kHeadBytes,"journal head exceeds limit");
    append_u32(out,bytes->size()); out.insert(out.end(),bytes->begin(),bytes->end());
  }
}
Bytes encode(const JournalState& state) {
  require(valid_uuid(state.identity.namespace_id) && valid_uuid(state.identity.incarnation),"invalid journal identity");
  Bytes result{'V','D','J','1'};
  append_text(result,state.identity.namespace_id); append_text(result,state.identity.incarnation);
  append_head(result,state.committed); append_head(result,state.pending);
  const auto digest=content_id("owner-journal","iotdbfs-owner-journal/1",result);
  result.insert(result.end(),digest.begin(),digest.end()); return result;
}
JournalState decode(const Bytes& bytes) {
  require(bytes.size()>=4+8+72+2+64 && bytes.size()<=2*kHeadBytes+256,"invalid owner journal length");
  const Bytes body(bytes.begin(),bytes.end()-64);
  const std::string digest(bytes.end()-64,bytes.end());
  require(content_id("owner-journal","iotdbfs-owner-journal/1",body)==digest,"owner journal digest mismatch");
  std::size_t at=0;
  auto number=[&](unsigned width) {
    require(width<=body.size()-at,"truncated journal field");
    std::uint32_t result=0; while(width--) result=(result<<8)|body[at++]; return result;
  };
  auto text=[&](std::size_t limit) {
    auto size=number(4); require(size<=limit && size<=body.size()-at,"invalid journal string");
    std::string result(body.begin()+at,body.begin()+at+size); at+=size; return result;
  };
  auto head=[&]() -> std::optional<Bytes> {
    auto present=number(1); require(present<=1,"invalid journal optional");
    if (!present) return {};
    auto size=number(4); require(size<=kHeadBytes && size<=body.size()-at,"invalid journal head length");
    Bytes result(body.begin()+at,body.begin()+at+size); at+=size;
    (void)decode_head(result); return result;
  };
  require(number(4)==0x56444a31,"unsupported owner journal protocol");
  JournalState state; state.identity={text(36),text(36)};
  state.committed=head(); state.pending=head();
  require(at==body.size() && valid_uuid(state.identity.namespace_id) && valid_uuid(state.identity.incarnation),
    "invalid journal identity or trailing bytes");
  for (const auto& value:{state.committed,state.pending}) if (value)
    require(decode_head(*value).identity==state.identity,"journal head identity mismatch");
  return state;
}
int private_directory(const std::string& path) {
  require(!path.empty() && path.front()=='/' && path.back()!='/',"owner directory must be an absolute canonical path");
  int fd=::open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);
  if (fd<0) failed("open root");
  try {
    std::size_t begin=1;
    while (begin<path.size()) {
      const auto end=path.find('/',begin);
      const auto part=path.substr(begin,end==std::string::npos?std::string::npos:end-begin);
      require(!part.empty() && part!="." && part!="..","owner directory has a noncanonical component");
      if (::mkdirat(fd,part.c_str(),0700)!=0 && errno!=EEXIST) failed("mkdir");
      const int next=::openat(fd,part.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
      if (next<0) failed("open directory");
      ::close(fd); fd=next;
      if (end==std::string::npos) break;
      begin=end+1;
    }
    struct stat info{};
    if (::fstat(fd,&info)!=0) failed("stat directory");
    require(info.st_uid==::geteuid() && !(info.st_mode&0077),"owner directory must be private and owned by the daemon user");
    return fd;
  } catch (...) { ::close(fd); throw; }
}
} // namespace

FileJournal::FileJournal(const std::string& directory):directory_(directory) {
  directory_fd_=private_directory(directory_);
  try {
    lock_fd_=::openat(directory_fd_,"owner.lock",O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600);
    if (lock_fd_<0) failed("open lock");
    private_file(lock_fd_);
    if (::flock(lock_fd_,LOCK_EX|LOCK_NB)!=0) failed("acquire exclusive lock");
    if (::fsync(directory_fd_)!=0) failed("sync directory");
  } catch (...) {
    if (lock_fd_>=0) ::close(lock_fd_);
    ::close(directory_fd_); throw;
  }
}
FileJournal::~FileJournal() {
  if (lock_fd_>=0) ::close(lock_fd_);
  if (directory_fd_>=0) ::close(directory_fd_);
}
std::optional<JournalState> FileJournal::load() {
  const int fd=::openat(directory_fd_,"state",O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
  if (fd<0) { if (errno==ENOENT) return {}; failed("open state"); }
  try {
    private_file(fd); struct stat info{};
    if (::fstat(fd,&info)!=0) failed("stat state");
    require(info.st_size>=0 && static_cast<std::uint64_t>(info.st_size)<=2*kHeadBytes+256,"owner journal size exceeds limit");
    Bytes bytes(static_cast<std::size_t>(info.st_size)); std::size_t at=0;
    while(at<bytes.size()) {
      auto count=::read(fd,bytes.data()+at,bytes.size()-at);
      if (count<0 && errno==EINTR) continue;
      if (count<0) failed("read state");
      require(count>0,"short owner journal read"); at+=count;
    }
    auto state=decode(bytes); ::close(fd); return state;
  } catch (...) { ::close(fd); throw; }
}
void FileJournal::save(const JournalState& state) {
  const auto bytes=encode(state); const auto name=".state-"+random_uuid();
  const int fd=::openat(directory_fd_,name.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600);
  if (fd<0) failed("create state");
  bool open=true;
  try {
    std::size_t at=0;
    while(at<bytes.size()) {
      auto count=::write(fd,bytes.data()+at,bytes.size()-at);
      if (count<0 && errno==EINTR) continue;
      if (count<=0) failed("write state");
      at+=count;
    }
    if (::fsync(fd)!=0) failed("sync state");
    ::close(fd); open=false;
    if (::renameat(directory_fd_,name.c_str(),directory_fd_,"state")!=0) failed("replace state");
    if (::fsync(directory_fd_)!=0) failed("sync directory");
  } catch (...) {
    if (open) ::close(fd);
    ::unlinkat(directory_fd_,name.c_str(),0); throw;
  }
}
} // namespace iotdbfs::versioning
