#include "iotdbfs/storage/NativeFileStore.h"

#include <openssl/evp.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/vfs.h>
#else
#include <sys/mount.h>
#endif

namespace iotdbfs {
namespace {
using versioning::Error;
using versioning::ErrorCode;
using versioning::Bytes;
constexpr std::size_t kChunk = 1024 * 1024;
const std::string kPrefix = std::string(1, '\1') + "native-file/";

struct Fd {
  int value{-1};
  explicit Fd(int v = -1) : value(v) {}
  ~Fd() { if (value >= 0) ::close(value); }
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  int release() { return std::exchange(value, -1); }
};
void require(bool okay, const char* message) {
  if (!okay) throw Error(ErrorCode::Corrupt, message);
}
void system_check(bool okay, const char* operation) {
  if (!okay) throw Error(ErrorCode::Unavailable,
    std::string("native file ") + operation + ": " + std::strerror(errno));
}
struct Encoder {
  Bytes bytes;
  void number(std::uint64_t n) {
    for (int i = 7; i >= 0; --i) bytes.push_back(static_cast<std::uint8_t>(n >> (8 * i)));
  }
  void text(const std::string& s) {
    require(s.size() <= 65536, "native locator text too long");
    number(s.size()); bytes.insert(bytes.end(), s.begin(), s.end());
  }
};
struct Decoder {
  const Bytes& bytes; std::size_t pos{0};
  std::uint64_t number() {
    require(bytes.size() - pos >= 8, "truncated native locator");
    std::uint64_t n = 0;
    for (int i = 0; i < 8; ++i) n = (n << 8) | bytes[pos++];
    return n;
  }
  std::string text() {
    auto n = number();
    require(n <= 65536 && n <= bytes.size() - pos, "invalid native locator text");
    std::string result(bytes.begin() + pos, bytes.begin() + pos + n);
    pos += n; return result;
  }
};
void validate_identity(const NativeFileLocation& l) {
  require(versioning::valid_uuid(l.namespace_identity.namespace_id) &&
    versioning::valid_uuid(l.namespace_identity.incarnation), "invalid native namespace identity");
  (void)versioning::exact_key(l.key.path, l.key.version);
  require(versioning::valid_content_id(l.descriptor_id) &&
    versioning::valid_content_id(l.seal_id) &&
    versioning::valid_content_id(l.payload_root), "invalid native descriptor binding");
  // A per-request transport budget is not a limit on the full stored file.
  require(l.size <= static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()),
    "native file exceeds filesystem offset range");
}
void validate_location(const NativeFileLocation& l) {
  validate_identity(l);
  require(versioning::valid_content_id(l.sha256) &&
    l.relative_path == l.sha256 + ".blob", "invalid native object name");
}
std::uint64_t mtime(const struct stat& s) {
#ifdef __APPLE__
  return std::uint64_t(s.st_mtimespec.tv_sec) * 1000000000 + s.st_mtimespec.tv_nsec;
#else
  return std::uint64_t(s.st_mtim.tv_sec) * 1000000000 + s.st_mtim.tv_nsec;
#endif
}
std::uint64_t ctime(const struct stat& s) {
#ifdef __APPLE__
  return std::uint64_t(s.st_ctimespec.tv_sec) * 1000000000 + s.st_ctimespec.tv_nsec;
#else
  return std::uint64_t(s.st_ctim.tv_sec) * 1000000000 + s.st_ctim.tv_nsec;
#endif
}
bool matches(const struct stat& s, const NativeFileLocation& l) {
  return S_ISREG(s.st_mode) && s.st_nlink == 1 && s.st_uid == ::geteuid() &&
    (s.st_mode & 07777) == 0444 && s.st_size >= 0 &&
    std::uint64_t(s.st_size) == l.size && std::uint64_t(s.st_dev) == l.device &&
    std::uint64_t(s.st_ino) == l.inode && mtime(s) == l.mtime_ns && ctime(s) == l.ctime_ns;
}
void assign_stat(NativeFileLocation& l, const struct stat& s) {
  l.device = s.st_dev; l.inode = s.st_ino;
  l.mtime_ns = mtime(s); l.ctime_ns = ctime(s);
}
class Digest {
public:
  Digest() : ctx_(EVP_MD_CTX_new(), EVP_MD_CTX_free) {
    require(ctx_ && EVP_DigestInit_ex(ctx_.get(), EVP_sha256(), nullptr) == 1,
      "cannot initialize native file hash");
  }
  void add(const Bytes& b) {
    require(EVP_DigestUpdate(ctx_.get(), b.data(), b.size()) == 1, "cannot hash native file");
  }
  std::string finish() {
    unsigned char out[32]; unsigned size = 0;
    require(EVP_DigestFinal_ex(ctx_.get(), out, &size) == 1 && size == 32,
      "cannot finish native file hash");
    constexpr char hex[] = "0123456789abcdef";
    std::string result; result.reserve(64);
    for (auto b : out) { result.push_back(hex[b >> 4]); result.push_back(hex[b & 15]); }
    return result;
  }
private:
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx_;
};
Bytes pread_exact(int fd, std::uint64_t offset, std::size_t size) {
  Bytes result(size); std::size_t done = 0;
  while (done < size) {
    auto count = ::pread(fd, result.data() + done, size - done, offset + done);
    if (count < 0 && errno == EINTR) continue;
    system_check(count >= 0, "pread");
    require(count != 0, "short native file read"); done += count;
  }
  return result;
}
void write_all(int fd, const Bytes& bytes) {
  std::size_t done = 0;
  while (done < bytes.size()) {
    auto count = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (count < 0 && errno == EINTR) continue;
    system_check(count > 0, "write"); done += count;
  }
}
int open_root(const std::string& root) {
  require(root.size() > 1 && root.front() == '/' && root.back() != '/' &&
    root.find('\0') == std::string::npos, "native root must be an absolute canonical directory");
  Fd current(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  system_check(current.value >= 0, "open root ancestor");
  std::size_t start = 1;
  while (start < root.size()) {
    auto end = root.find('/', start);
    if (end == std::string::npos) end = root.size();
    auto component = root.substr(start, end - start);
    require(!component.empty() && component != "." && component != "..", "noncanonical native root");
    bool sync_created_directory = false;
    int next = ::openat(current.value, component.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (next < 0 && errno == ENOENT) {
      system_check(::mkdirat(current.value, component.c_str(), 0700) == 0 || errno == EEXIST, "mkdir");
      // Sync even when another creator won the mkdir race: its directory
      // entry must be durable before this store can publish any locator.
      system_check(::fsync(current.value) == 0, "sync created directory parent");
      sync_created_directory = true;
      next = ::openat(current.value, component.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    }
    system_check(next >= 0, "open directory without symlinks");
    Fd opened(next);
    if (sync_created_directory)
      system_check(::fsync(opened.value) == 0, "sync created directory");
    ::close(current.value); current.value = opened.release(); start = end + 1;
  }
  struct stat st{}; system_check(::fstat(current.value, &st) == 0, "stat root");
  require(st.st_uid == ::geteuid() && (st.st_mode & 07777) == 0700, "native root must be private mode 0700");
  struct statfs fs{}; system_check(::fstatfs(current.value, &fs) == 0, "stat filesystem");
#ifdef __linux__
  require(static_cast<unsigned long>(fs.f_type) != 0x65735546UL, "native root cannot use FUSE");
#else
  require(std::string(fs.f_fstypename).find("fuse") == std::string::npos, "native root cannot use FUSE");
#endif
  return current.release();
}
template<class T> StructuredBlobResult<T> failure(const std::exception& e) {
  return StructuredBlobResult<T>::failure(StructuredBlobError::BackendFailure, e.what());
}
} // namespace

Bytes encode_native_file_location(const NativeFileLocation& l) {
  validate_location(l); Encoder e;
  e.text("iotdbfs-native-file-location/1");
  e.text(l.namespace_identity.namespace_id); e.text(l.namespace_identity.incarnation);
  e.text(l.key.path); e.number(l.key.version);
  e.text(l.descriptor_id); e.text(l.seal_id); e.text(l.payload_root);
  e.text(l.relative_path); e.text(l.sha256); e.number(l.size);
  e.number(l.device); e.number(l.inode); e.number(l.mtime_ns); e.number(l.ctime_ns);
  return std::move(e.bytes);
}
NativeFileLocation decode_native_file_location(const Bytes& bytes) {
  require(bytes.size() <= versioning::kPageBytes, "native locator too large");
  Decoder d{bytes}; NativeFileLocation l;
  require(d.text() == "iotdbfs-native-file-location/1", "unsupported native locator protocol");
  l.namespace_identity.namespace_id = d.text(); l.namespace_identity.incarnation = d.text();
  l.key.path = d.text(); auto version = d.number();
  require(version <= std::uint64_t(std::numeric_limits<std::int64_t>::max()), "invalid native version");
  l.key.version = version;
  l.descriptor_id = d.text(); l.seal_id = d.text(); l.payload_root = d.text();
  l.relative_path = d.text(); l.sha256 = d.text(); l.size = d.number();
  l.device = d.number(); l.inode = d.number(); l.mtime_ns = d.number(); l.ctime_ns = d.number();
  require(d.pos == bytes.size(), "trailing native locator bytes");
  validate_location(l); require(encode_native_file_location(l) == bytes, "noncanonical native locator");
  return l;
}
std::string native_file_location_key(const StructuredBlobVersionKey& key) {
  return kPrefix + versioning::exact_key(key.path, key.version);
}
bool is_native_file_location_key(const std::string& key) {
  return key.compare(0, kPrefix.size(), kPrefix) == 0;
}

struct NativeFileStore::Impl {
  explicit Impl(int value) : fd(value) {}
  Fd fd;
};
struct NativeFileHandle::Impl {
  Impl(int root, int file, NativeFileLocation l) : root_fd(root), fd(file), location(std::move(l)) {}
  Fd root_fd, fd;
  NativeFileLocation location;
};
NativeFileHandle::NativeFileHandle(std::shared_ptr<Impl> impl,
    std::function<StructuredBlobStatus()> guard)
  : impl_(std::move(impl)), guard_(std::move(guard)) {}
NativeFileHandle::~NativeFileHandle() = default;
std::uint64_t NativeFileHandle::size() const { return impl_->location.size; }
std::shared_ptr<const NativeFileDescriptor> NativeFileHandle::cache_descriptor() const {
  return std::shared_ptr<const NativeFileDescriptor>(new NativeFileDescriptor(impl_));
}
std::shared_ptr<const NativeFileHandle> NativeFileHandle::with_read_guard(
    std::function<StructuredBlobStatus()> guard) const {
  if (guard_ && guard) {
    guard = [previous = guard_, added = std::move(guard)] {
      auto status = previous();
      return status.ok() ? added() : status;
    };
  } else if (guard_) {
    guard = guard_;
  }
  return std::shared_ptr<const NativeFileHandle>(
    new NativeFileHandle(impl_, std::move(guard)));
}
StructuredBlobStatus NativeFileHandle::validate() const {
  if (guard_) {
    try {
      auto status = guard_();
      if (!status.ok()) return status;
    } catch (const std::exception& e) {
      return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure, e.what());
    } catch (...) {
      return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
        "native file read guard failed");
    }
  }
  struct stat fd{}, entry{};
  if (::fstat(impl_->fd.value, &fd) != 0 ||
      ::fstatat(impl_->root_fd.value, impl_->location.relative_path.c_str(), &entry, AT_SYMLINK_NOFOLLOW) != 0 ||
      !matches(fd, impl_->location) || !matches(entry, impl_->location))
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "managed native file is missing, replaced, or changed");
  return StructuredBlobStatus::success();
}
StructuredBlobResult<StructuredBlobBytes> NativeFileHandle::read(std::uint64_t offset, std::uint64_t length) const {
  if (offset > size() || length > size() - offset || length > kMaxSharedPayloadReadBytes)
    return StructuredBlobResult<Bytes>::failure(StructuredBlobError::RangeError, "native range outside file");
  auto before = validate();
  if (!before.ok()) return StructuredBlobResult<Bytes>::failure(before.code(), before.message());
  try {
    auto bytes = pread_exact(impl_->fd.value, offset, length);
    auto after = validate();
    if (!after.ok()) return StructuredBlobResult<Bytes>::failure(after.code(), after.message());
    return StructuredBlobResult<Bytes>::success(std::move(bytes));
  } catch (const std::exception& e) { return failure<Bytes>(e); }
}
NativeFileStore::NativeFileStore(const std::string& root) : impl_(std::make_shared<Impl>(open_root(root))) {}
NativeFileStore::~NativeFileStore() = default;

StructuredBlobResult<NativeFileLocation> NativeFileStore::install(NativeFileLocation l, const Reader& reader) const {
  std::string temporary;
  try {
    validate_identity(l); require(bool(reader), "missing native installation reader");
    require(l.sha256.empty() || versioning::valid_content_id(l.sha256), "invalid expected native hash");
    temporary = ".install-" + versioning::random_uuid();
    Fd fd(::openat(impl_->fd.value, temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    system_check(fd.value >= 0, "create temporary");
    Digest digest;
    for (std::uint64_t offset = 0; offset < l.size;) {
      auto length = std::min<std::uint64_t>(kChunk, l.size - offset);
      auto bytes = reader(offset, length);
      if (!bytes.ok()) {
        ::unlinkat(impl_->fd.value, temporary.c_str(), 0);
        return StructuredBlobResult<NativeFileLocation>::failure(bytes.status().code(), bytes.status().message());
      }
      require(bytes.value().size() == length, "short native installation reader");
      digest.add(bytes.value()); write_all(fd.value, bytes.value()); offset += length;
    }
    auto hash = digest.finish();
    require(l.sha256.empty() || l.sha256 == hash, "native installation content hash mismatch");
    l.sha256 = hash; l.relative_path = hash + ".blob";
    system_check(::fchmod(fd.value, 0444) == 0, "protect completed file");
    system_check(::fsync(fd.value) == 0, "sync completed file");
    int installed = ::linkat(impl_->fd.value, temporary.c_str(), impl_->fd.value, l.relative_path.c_str(), 0);
    require(installed == 0 || errno == EEXIST, "cannot atomically install native file");
    system_check(::unlinkat(impl_->fd.value, temporary.c_str(), 0) == 0, "remove temporary link");
    temporary.clear();
    Fd completed(::openat(impl_->fd.value, l.relative_path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    system_check(completed.value >= 0, "open completed file");
    struct stat st{}; system_check(::fstat(completed.value, &st) == 0, "stat completed file");
    assign_stat(l, st); require(matches(st, l), "invalid immutable native file");
    // Existing content-addressed files are never trusted by name alone.
    if (installed != 0) {
      Digest existing;
      for (std::uint64_t offset = 0; offset < l.size;) {
        auto length = std::min<std::uint64_t>(kChunk, l.size - offset);
        existing.add(pread_exact(completed.value, offset, length)); offset += length;
      }
      require(existing.finish() == l.sha256, "existing native content differs from hash");
    }
    struct stat end{}, entry{};
    system_check(::fstat(completed.value, &end) == 0 &&
      ::fstatat(impl_->fd.value, l.relative_path.c_str(), &entry, AT_SYMLINK_NOFOLLOW) == 0, "verify installed entry");
    require(matches(end, l) && matches(entry, l), "native file changed during installation");
    system_check(::fsync(impl_->fd.value) == 0, "sync directory");
    return StructuredBlobResult<NativeFileLocation>::success(std::move(l));
  } catch (const std::exception& e) {
    if (!temporary.empty()) ::unlinkat(impl_->fd.value, temporary.c_str(), 0);
    return failure<NativeFileLocation>(e);
  }
}
StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> NativeFileStore::open(const NativeFileLocation& l) const {
  using Result = StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>;
  try {
    validate_location(l);
    Fd fd(::openat(impl_->fd.value, l.relative_path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    if (fd.value < 0 && errno == ENOENT)
      return Result::failure(StructuredBlobError::NotFound, "managed native file not found");
    system_check(fd.value >= 0, "open immutable file");
    Fd root(::fcntl(impl_->fd.value, F_DUPFD_CLOEXEC, 0));
    system_check(root.value >= 0, "pin native root");
    auto state = std::make_shared<NativeFileHandle::Impl>(root.value, fd.value, l);
    root.release(); fd.release();
    std::shared_ptr<const NativeFileHandle> handle(new NativeFileHandle(std::move(state)));
    auto status = handle->validate();
    if (!status.ok()) return Result::failure(status.code(), status.message());
    return Result::success(std::move(handle));
  } catch (const std::exception& e) { return failure<std::shared_ptr<const NativeFileHandle>>(e); }
}
StructuredBlobResult<std::string> NativeFileStore::cache_identity(const NativeFileLocation& l) const {
  try {
    auto bytes = encode_native_file_location(l);
    struct stat root{};
    system_check(::fstat(impl_->fd.value, &root) == 0, "inspect cache root");
    Encoder encoded;
    encoded.text("iotdbfs-native-handle/1");
    encoded.number(root.st_dev); encoded.number(root.st_ino);
    encoded.bytes.insert(encoded.bytes.end(), bytes.begin(), bytes.end());
    return StructuredBlobResult<std::string>::success(
      std::string(encoded.bytes.begin(), encoded.bytes.end()));
  } catch (const std::exception& e) { return failure<std::string>(e); }
}
StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> NativeFileStore::reuse(
    const NativeFileLocation& l, const std::shared_ptr<const NativeFileDescriptor>& cached) const {
  using Result = StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>;
  try {
    require(bool(cached), "missing cached native handle");
    require(encode_native_file_location(l) == encode_native_file_location(cached->impl_->location),
      "cached native handle identity differs");
    struct stat root{}, previous{};
    system_check(::fstat(impl_->fd.value, &root) == 0 &&
      ::fstat(cached->impl_->root_fd.value, &previous) == 0, "inspect reused root");
    require(root.st_dev == previous.st_dev && root.st_ino == previous.st_ino,
      "cached native handle belongs to another store");
    // The newly admitted owner will attach its own guard. Never invoke or
    // retain the previous request's guard as authorization for this request.
    std::shared_ptr<const NativeFileHandle> handle(new NativeFileHandle(cached->impl_));
    auto status = handle->validate();
    if (!status.ok()) return Result::failure(status.code(), status.message());
    return Result::success(std::move(handle));
  } catch (const std::exception& e) { return failure<std::shared_ptr<const NativeFileHandle>>(e); }
}
} // namespace iotdbfs
