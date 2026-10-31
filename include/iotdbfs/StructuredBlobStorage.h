#pragma once

#include "iotdbfs/MetadataConfig.h"
#include "iotdbfs/DatasetCatalog.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {

// A process-local identity allocator shared by storage backends and standalone
// QuerySources. IDs are never recycled, including after object destruction.
namespace detail { std::uint64_t allocate_storage_namespace(); }

using StructuredBlobBytes = std::vector<std::uint8_t>;
// Synchronous borrowed chunks: neither the backend nor the lifecycle operation
// retains these references after return. Existing OBJECT transfer sizes stay
// unchanged; only manifest validation/frontier commits are batched.
using StructuredBlobPayloadWriteChunks =
  std::vector<std::reference_wrapper<const StructuredBlobBytes>>;
inline constexpr std::size_t kMaxPayloadWriteChunksPerBatch = 64;
inline constexpr std::uint64_t kMaxPayloadWriteBytesPerBatch = 8ULL * 1024 * 1024;

struct StructuredBlobPayloadRange {
  std::uint64_t offset{ 0 };
  std::uint64_t length{ 0 };
};

using StructuredBlobPayloadRanges = std::vector<StructuredBlobPayloadRange>;
using StructuredBlobPayloadBatch = std::vector<StructuredBlobBytes>;
inline constexpr std::size_t kMaxPayloadRangesPerBatch = 64;
inline constexpr std::uint64_t kMaxPayloadBytesPerBatch = 8ULL * 1024 * 1024;
inline constexpr std::uint64_t kMaxSharedPayloadReadBytes = 1ULL << 30;

struct StructuredBlobVersionKey {
  std::string path;
  std::int64_t version{ 0 };

  friend bool operator==(const StructuredBlobVersionKey& left,
    const StructuredBlobVersionKey& right) {
    return left.path == right.path && left.version == right.version;
  }

  friend bool operator<(const StructuredBlobVersionKey& left,
    const StructuredBlobVersionKey& right) {
    if (left.path != right.path) return left.path < right.path;
    return left.version < right.version;
  }
};

// The phase is durable recovery state. Published is deliberately represented
// separately because it is the sole visibility gate for readers.
enum class StructuredBlobPhase {
  PayloadOpen,
  PayloadSealed,
  LayoutStored,
  Described,
  Published,
};

class StructuredBlobPayloadReader;
class NativeFileHandle;
class NativeFileDescriptor;

// Small, format-independent state for one immutable file version. Layout bytes
// and payload bytes are stored separately and are never embedded here.
struct StructuredBlobManifest {
  StructuredBlobVersionKey key;
  std::optional<std::string> format_id;
  MetadataMap metadata;
  std::uint64_t payload_size{ 0 };
  std::uint64_t layout_size{ 0 };
  StructuredBlobPhase phase{ StructuredBlobPhase::PayloadOpen };
  bool published{ false };
  std::uint64_t revision{ 0 };
  // In-process source catalogs may expose identity/permissions before content
  // authorization. Such skeletons must be materialized before metadata use.
  bool metadata_materialized{ true };
  // Owner-issued process capability only: never encoded, hashed, or used in
  // persistent equality. Authorization remains at execute/new-open boundaries.
  std::shared_ptr<const StructuredBlobPayloadReader> payload_reader;
};

enum class StructuredBlobError {
  None,
  InvalidArgument,
  NotFound,
  AlreadyExists,
  InvalidState,
  RangeError,
  BackendFailure,
};

class StructuredBlobStatus {
public:
  StructuredBlobStatus() = default;

  static StructuredBlobStatus success() { return {}; }
  static StructuredBlobStatus failure(StructuredBlobError code,
    std::string message) {
    return StructuredBlobStatus(code, std::move(message));
  }

  bool ok() const { return code_ == StructuredBlobError::None; }
  StructuredBlobError code() const { return code_; }
  const std::string& message() const { return message_; }

private:
  StructuredBlobStatus(StructuredBlobError code, std::string message)
    : code_(code), message_(std::move(message)) {}

  StructuredBlobError code_{ StructuredBlobError::None };
  std::string message_;
};

template<typename T>
class StructuredBlobResult {
public:
  static StructuredBlobResult success(T value) {
    StructuredBlobResult result;
    result.value_ = std::move(value);
    return result;
  }

  static StructuredBlobResult failure(StructuredBlobError code,
    std::string message) {
    StructuredBlobResult result;
    result.status_ = StructuredBlobStatus::failure(code, std::move(message));
    return result;
  }

  bool ok() const { return status_.ok(); }
  const StructuredBlobStatus& status() const { return status_; }
  const T& value() const { return *value_; }
  T& value() { return *value_; }

private:
  StructuredBlobStatus status_;
  std::optional<T> value_;
};

// A fixed, verified published-artifact closure. It carries no namespace
// selection or authorization; execution leases and frontend admission govern
// its use. The owner implementation pins payload root plus layout/proof data.
class StructuredBlobPayloadReader {
public:
  virtual ~StructuredBlobPayloadReader() = default;
  virtual StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
    const StructuredBlobPayloadRanges&) const = 0;
  virtual StructuredBlobResult<StructuredBlobBytes> read_layout_range(
    std::uint64_t offset, std::uint64_t length) const = 0;
  virtual StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const = 0;
  // Optional exact-version filesystem capability. This does not fall back to
  // database payload reads when a native file has not been explicitly bound.
  virtual StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const {
    return StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>::failure(
      StructuredBlobError::NotFound, "no filesystem file is bound to this exact version");
  }
  virtual StructuredBlobResult<std::optional<std::string>> native_file_cache_identity() const {
    return StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
  }
  virtual StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>&) const {
    return open_native_file();
  }
};

// Persistence seam used by StructuredBlobStorage. An IoTDB adapter implements
// these coarse operations; callers never manipulate backend rows, SQL, chunks,
// or format-specific index records. Artifact writes must be idempotent. The
// module advances the durable manifest frontier with a subsequent CAS, so a
// failed CAS may leave harmless, invisible artifact data for retry or cleanup.
// A key has one lifecycle writer at a time; revision checks reject stale
// transitions. load_manifest must reconstruct only state at the durable phase,
// ignoring any artifact data written ahead of that phase. write_metadata
// replaces the complete document (an empty map clears orphan metadata).
// erase removes visible state and artifacts. If the erased Manifest was ever
// published, the Adapter must retain a durable identity tombstone so the same
// (path, version) can never be created again; aborting an unpublished version
// does not retire its key.
class StructuredBlobStorageBackend {
public:
  StructuredBlobStorageBackend();
  StructuredBlobStorageBackend(const StructuredBlobStorageBackend&);
  StructuredBlobStorageBackend& operator=(const StructuredBlobStorageBackend&) = delete;
  virtual ~StructuredBlobStorageBackend() = default;

  std::uint64_t storage_namespace() const { return storage_namespace_; }

  virtual StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) = 0;
  virtual StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const = 0;
  virtual StructuredBlobStatus replace_manifest(
    const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
    const StructuredBlobManifest& next) = 0;
  virtual StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) = 0;
  // One lifecycle writer owns a key. The default uses ordinary artifact writes
  // and CAS; adapters may hold their writer lock across one fresh validation,
  // all chunk writes, and one durable frontier update. A partial artifact or
  // failed manifest write leaves only unpublished data for retry/abort. An
  // ambiguous commit acknowledgement may have advanced the frontier; retries
  // must load it afresh and overwrite the same exact offsets.
  virtual StructuredBlobStatus write_payload_and_advance(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobPayloadWriteChunks& chunks);
  virtual StructuredBlobStatus write_layout(
    const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) = 0;
  virtual StructuredBlobStatus write_dataset_catalog(
    const StructuredBlobVersionKey&, const DatasetCatalogArtifact&);
  virtual StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
    const StructuredBlobVersionKey&) const;
  virtual StructuredBlobStatus write_metadata(
    const StructuredBlobVersionKey& key,
    const MetadataMap& metadata) = 0;
  virtual StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const = 0;
  // A supported backend returns all ranges in input order through one storage
  // request. The default reports an unavailable capability; it never loops
  // through scalar reads and misrepresents them as batching.
  virtual bool supports_payload_ranges() const { return false; }
  // Explicit owner storage mode: public reads share one metadata/demand budget
  // across namespace lookup, verified binding and immutable content access.
  virtual bool uses_shared_payload() const { return false; }
  virtual StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
    const StructuredBlobVersionKey& key,
    const StructuredBlobPayloadRanges& ranges) const;
  virtual StructuredBlobResult<StructuredBlobBytes> read_layout_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const = 0;
  // Internal validated content key, never a client-supplied proof. No identity
  // means this backend retains the ordinary exact-version cache discipline.
  virtual StructuredBlobResult<std::optional<std::string>> verified_layout_identity(
    const StructuredBlobVersionKey&) const;
  // A null capability selects the strict backend's ordinary range path.
  // Owner backends validate the internal capability issuer and manifest binding.
  virtual StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
    bind_published_payload(const StructuredBlobManifest&) const;
  virtual StructuredBlobStatus erase(
    const StructuredBlobVersionKey& key,
    std::uint64_t expected_revision) = 0;

protected:
  static StructuredBlobResult<std::optional<StructuredBlobManifest>> plan_payload_write(
    const StructuredBlobManifest& current, const StructuredBlobVersionKey& key,
    std::uint64_t offset, const StructuredBlobPayloadWriteChunks& chunks);

private:
  const std::uint64_t storage_namespace_;
};

// Immutable read capability for one File Version whose publication state was
// validated when the handle was opened. Published versions never change, so
// bounded artifact reads reuse the retained Manifest instead of re-querying
// and re-deserializing it for every range.
class StructuredBlobReadHandle {
public:
  const StructuredBlobManifest& manifest() const { return manifest_; }
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const {
    if (payload_reader_) return payload_reader_->open_native_file();
    return StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>::failure(
      StructuredBlobError::NotFound, "this backend has no exact-version filesystem binding");
  }
  StructuredBlobResult<std::optional<std::string>> native_file_cache_identity() const {
    return payload_reader_ ? payload_reader_->native_file_cache_identity() :
      StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
  }
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>& handle) const {
    return payload_reader_ ? payload_reader_->reuse_native_file(handle) : open_native_file();
  }
  StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const {
    return payload_reader_ ? payload_reader_->verified_layout_identity() :
      backend_->verified_layout_identity(manifest_.key);
  }
  std::uint64_t storage_namespace() const {
    return backend_->storage_namespace();
  }

  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    std::uint64_t offset, std::uint64_t length) const;
  bool supports_payload_ranges() const { return payload_reader_ || backend_->supports_payload_ranges(); }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
    const StructuredBlobPayloadRanges& ranges) const;
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
    std::uint64_t offset, std::uint64_t length) const;

private:
  friend class StructuredBlobStorage;

  StructuredBlobReadHandle(
    std::shared_ptr<const StructuredBlobStorageBackend> backend,
    StructuredBlobManifest manifest, std::shared_ptr<const StructuredBlobPayloadReader> payload_reader)
    : backend_(std::move(backend)), manifest_(std::move(manifest)),
      payload_reader_(std::move(payload_reader)) {}

  std::shared_ptr<const StructuredBlobStorageBackend> backend_;
  StructuredBlobManifest manifest_;
  std::shared_ptr<const StructuredBlobPayloadReader> payload_reader_;
};

// Owns the durable lifecycle and visibility invariants for Structured Blobs:
// payload B -> sealed B -> adapter-private layout L -> typed Sigma/identity ->
// publication. A missing format identity is the byte pass-through baseline and
// therefore requires empty metadata and an empty layout.
class StructuredBlobStorage {
public:
  explicit StructuredBlobStorage(
    std::shared_ptr<StructuredBlobStorageBackend> backend);
  std::uint64_t storage_namespace() const {
    return backend_ ? backend_->storage_namespace() : 0;
  }

  StructuredBlobStatus begin(const StructuredBlobVersionKey& key);
  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes);
  StructuredBlobStatus write_payload_batch(
    const StructuredBlobVersionKey& key, std::uint64_t base_offset,
    const std::vector<StructuredBlobBytes>& chunks);
  StructuredBlobStatus seal_payload(const StructuredBlobVersionKey& key);
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout);
  // A catalog adds a source-bound publication seal. On successful description
  // the optional output receives the final metadata for namespace preparation;
  // callers must not overwrite it with the adapter's pre-seal document.
  StructuredBlobStatus write_metadata_and_format_identity(
    const StructuredBlobVersionKey& key,
    std::optional<std::string> format_id, MetadataMap metadata,
    std::optional<DatasetCatalogArtifact> dataset_catalog = std::nullopt,
    MetadataMap* described_metadata = nullptr);
  StructuredBlobStatus publish(const StructuredBlobVersionKey& key);
  // abort removes an unpublished attempt and leaves its key reusable. remove
  // hides a version and permanently retires the key if it was published.
  StructuredBlobStatus abort(const StructuredBlobVersionKey& key);
  StructuredBlobStatus remove(const StructuredBlobVersionKey& key);

  // Reader operations expose published versions only. An unpublished or
  // aborted key is reported as NotFound so partial ingestion is not observable.
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const;
  StructuredBlobResult<StructuredBlobReadHandle> open_published_version(
    const StructuredBlobVersionKey& key) const;
  // Opens a manifest from a fresh, trusted catalog scan of this backend.
  // This is an internal read capability, never a client-supplied manifest.
  StructuredBlobResult<StructuredBlobReadHandle> open_catalog_version(
    const StructuredBlobVersionKey& key, const StructuredBlobManifest& manifest,
    std::uint64_t catalog_namespace) const;
  StructuredBlobResult<DatasetCatalogArtifact> load_dataset_catalog(
    const StructuredBlobVersionKey& key) const;
  StructuredBlobResult<StructuredBlobBytes> load_layout(
    const StructuredBlobVersionKey& key) const;
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const;
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const;

private:
  StructuredBlobResult<StructuredBlobManifest> load_for_write(
    const StructuredBlobVersionKey& key) const;
  StructuredBlobResult<StructuredBlobManifest> load_visible(
    const StructuredBlobVersionKey& key) const;
  static StructuredBlobStatus validate_key(
    const StructuredBlobVersionKey& key);
  static StructuredBlobStatus require_phase(
    const StructuredBlobManifest& manifest, StructuredBlobPhase expected,
    const char* operation);

  std::shared_ptr<StructuredBlobStorageBackend> backend_;
};

} // namespace iotdbfs
