#pragma once

#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/storage/versioning/VersionDirectory.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace iotdbfs {

// A locator grants no authorization. The owner must bind it to an admitted,
// exact descriptor/seal before exposing the corresponding read capability.
struct NativeFileLocation {
  versioning::NamespaceIdentity namespace_identity;
  StructuredBlobVersionKey key;
  std::string descriptor_id, seal_id, payload_root;
  std::string relative_path, sha256;
  std::uint64_t size{0};
  std::uint64_t device{0}, inode{0}, mtime_ns{0}, ctime_ns{0};
};

versioning::Bytes encode_native_file_location(const NativeFileLocation&);
NativeFileLocation decode_native_file_location(const versioning::Bytes&);
std::string native_file_location_key(const StructuredBlobVersionKey&);
bool is_native_file_location_key(const std::string&);

class NativeFileHandle {
public:
  ~NativeFileHandle();
  std::uint64_t size() const;
  // Opaque, non-authorizing descriptor pin for a cache. It has no read API.
  std::shared_ptr<const NativeFileDescriptor> cache_descriptor() const;
  // Adds a live capability check without replacing any guard already attached
  // to this handle. The immutable file descriptors remain shared.
  std::shared_ptr<const NativeFileHandle> with_read_guard(
    std::function<StructuredBlobStatus()>) const;
  StructuredBlobStatus validate() const;
  StructuredBlobResult<StructuredBlobBytes> read(std::uint64_t offset,
    std::uint64_t length) const;
private:
  struct Impl;
  explicit NativeFileHandle(std::shared_ptr<Impl>,
    std::function<StructuredBlobStatus()> guard = {});
  std::shared_ptr<Impl> impl_;
  std::function<StructuredBlobStatus()> guard_;
  friend class NativeFileStore;
  friend class NativeFileDescriptor;
};

class NativeFileDescriptor final {
private:
  explicit NativeFileDescriptor(std::shared_ptr<NativeFileHandle::Impl> impl)
    : impl_(std::move(impl)) {}
  std::shared_ptr<NativeFileHandle::Impl> impl_;
  friend class NativeFileHandle;
  friend class NativeFileStore;
};

class NativeFileStore {
public:
  using Reader = std::function<StructuredBlobResult<StructuredBlobBytes>(
    std::uint64_t offset, std::uint64_t length)>;
  // The root is absolute, private, owned by this process's uid, and must be
  // on an ordinary filesystem. Components and object names cannot be symlinks.
  explicit NativeFileStore(const std::string& root);
  ~NativeFileStore();
  NativeFileStore(const NativeFileStore&) = delete;
  NativeFileStore& operator=(const NativeFileStore&) = delete;
  // Input identity/size are authoritative. Storage fields are generated here.
  // Completes fsync + immutable atomic installation before returning a locator.
  StructuredBlobResult<NativeFileLocation> install(NativeFileLocation identity,
    const Reader&) const;
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open(
    const NativeFileLocation&) const;
  // Canonical locator plus this pinned store root, without opening the file.
  // This is cache identity only; it grants no read authorization.
  StructuredBlobResult<std::string> cache_identity(const NativeFileLocation&) const;
  // Rebind the same pinned immutable descriptors to a fresh capability. Only
  // this store can remove an old guard; callers must add their current guard.
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> reuse(
    const NativeFileLocation&, const std::shared_ptr<const NativeFileDescriptor>&) const;
private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace iotdbfs
