#include "iotdbfs/storage/versioning/detail/PayloadBlockFlights.h"
#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <limits>
#include <utility>

namespace iotdbfs {
namespace {
std::unique_ptr<ScopedQueryMetadataBudget> shared_read_budget(
    const StructuredBlobStorageBackend* backend) {
  if (backend && backend->uses_shared_payload() &&
      !current_query_metadata_budget() && active_query_metadata_trace == nullptr)
    return std::make_unique<ScopedQueryMetadataBudget>();
  return {};
}
} // namespace
StructuredBlobResult<std::optional<std::string>> StructuredBlobStorageBackend::verified_layout_identity(
    const StructuredBlobVersionKey&) const {
  return StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
}
StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
StructuredBlobStorageBackend::bind_published_payload(const StructuredBlobManifest&) const {
  return StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>::success(nullptr);
}
namespace detail {
std::uint64_t allocate_storage_namespace() {
  static std::atomic<std::uint64_t> next{1};
  auto value = next.load(std::memory_order_relaxed);
  for (;;) {
    if (value == std::numeric_limits<std::uint64_t>::max())
      throw std::overflow_error("process-local storage namespace exhausted");
    if (next.compare_exchange_weak(value, value + 1,
        std::memory_order_relaxed)) return value;
  }
}
} // namespace detail

StructuredBlobStorageBackend::StructuredBlobStorageBackend()
  : storage_namespace_(detail::allocate_storage_namespace()) {}
StructuredBlobStorageBackend::StructuredBlobStorageBackend(
    const StructuredBlobStorageBackend&)
  : StructuredBlobStorageBackend() {}

StructuredBlobResult<std::optional<StructuredBlobManifest>>
StructuredBlobStorageBackend::plan_payload_write(
    const StructuredBlobManifest& current, const StructuredBlobVersionKey& key,
    std::uint64_t offset, const StructuredBlobPayloadWriteChunks& chunks) {
  using Result = StructuredBlobResult<std::optional<StructuredBlobManifest>>;
  if (!(current.key == key))
    return Result::failure(StructuredBlobError::BackendFailure,
      "backend returned a manifest for a different version key");
  if (current.phase != StructuredBlobPhase::PayloadOpen || current.published)
    return Result::failure(StructuredBlobError::InvalidState,
      "payload write is not valid in the current lifecycle phase");
  if (offset > current.payload_size)
    return Result::failure(StructuredBlobError::RangeError,
      "payload writes must not leave an unwritten gap");
  std::uint64_t end = offset;
  for (const auto& chunk : chunks) {
    if (chunk.get().size() > std::numeric_limits<std::uint64_t>::max() - end)
      return Result::failure(StructuredBlobError::RangeError,
        "payload write range overflows uint64");
    end += chunk.get().size();
  }
  if (end == offset) return Result::success(std::nullopt);
  if (current.revision == std::numeric_limits<std::uint64_t>::max())
    return Result::failure(StructuredBlobError::InvalidState,
      "structured blob manifest revision is exhausted");
  auto next = current;
  next.revision = current.revision + 1;
  next.payload_size = std::max(current.payload_size, end);
  return Result::success(std::move(next));
}

StructuredBlobStatus StructuredBlobStorageBackend::write_payload_and_advance(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobPayloadWriteChunks& chunks) {
  auto current = load_manifest(key);
  if (!current.ok()) return current.status();
  auto planned = plan_payload_write(current.value(), key, offset, chunks);
  if (!planned.ok()) return planned.status();
  if (!planned.value()) return StructuredBlobStatus::success();
  auto cursor = offset;
  for (const auto& chunk : chunks) {
    if (chunk.get().empty()) continue;
    auto written = write_payload_range(key, cursor, chunk.get());
    if (!written.ok()) return written;
    cursor += chunk.get().size();
  }
  return replace_manifest(key, current.value().revision, *planned.value());
}

StructuredBlobStatus StructuredBlobStorageBackend::write_dataset_catalog(
    const StructuredBlobVersionKey&, const DatasetCatalogArtifact&) {
  return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
    "backend does not support dataset catalog persistence");
}
StructuredBlobResult<DatasetCatalogArtifact> StructuredBlobStorageBackend::read_dataset_catalog(
    const StructuredBlobVersionKey&) const {
  return StructuredBlobResult<DatasetCatalogArtifact>::failure(StructuredBlobError::NotFound,
    "dataset catalog is unavailable");
}

StructuredBlobResult<StructuredBlobPayloadBatch>
StructuredBlobStorageBackend::read_payload_ranges(
    const StructuredBlobVersionKey&,
    const StructuredBlobPayloadRanges&) const {
  return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
    StructuredBlobError::BackendFailure,
    "storage backend does not support batched payload reads");
}

namespace {

StructuredBlobStatus backend_unavailable() {
  return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
    "structured blob storage backend is not configured");
}

template<typename T>
StructuredBlobResult<T> backend_unavailable_result() {
  return StructuredBlobResult<T>::failure(
    StructuredBlobError::BackendFailure,
    "structured blob storage backend is not configured");
}

bool next_revision(const StructuredBlobManifest& current,
  StructuredBlobManifest& next) {
  if (current.revision == std::numeric_limits<std::uint64_t>::max())
    return false;
  next = current;
  next.revision = current.revision + 1;
  return true;
}

StructuredBlobStatus revision_exhausted() {
  return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
    "structured blob manifest revision is exhausted");
}

StructuredBlobResult<StructuredBlobManifest> invisible() {
  return StructuredBlobResult<StructuredBlobManifest>::failure(
    StructuredBlobError::NotFound, "structured blob version is not published");
}

StructuredBlobResult<StructuredBlobBytes> bytes_failure(
  const StructuredBlobStatus& status) {
  return StructuredBlobResult<StructuredBlobBytes>::failure(
    status.code(), status.message());
}

} // namespace

StructuredBlobStorage::StructuredBlobStorage(
  std::shared_ptr<StructuredBlobStorageBackend> backend)
  : backend_(std::move(backend)) {}

StructuredBlobStatus StructuredBlobStorage::validate_key(
  const StructuredBlobVersionKey& key) {
  if (key.path.empty()) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::InvalidArgument,
      "structured blob path must not be empty");
  }
  if (key.version < 0) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::InvalidArgument,
      "structured blob version must not be negative");
  }
  return StructuredBlobStatus::success();
}

StructuredBlobStatus StructuredBlobStorage::require_phase(
  const StructuredBlobManifest& manifest, StructuredBlobPhase expected,
  const char* operation) {
  if (manifest.phase == expected && !manifest.published)
    return StructuredBlobStatus::success();
  return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
    std::string(operation) + " is not valid in the current lifecycle phase");
}

StructuredBlobResult<StructuredBlobManifest>
StructuredBlobStorage::load_for_write(
  const StructuredBlobVersionKey& key) const {
  const auto valid = validate_key(key);
  if (!valid.ok()) {
    return StructuredBlobResult<StructuredBlobManifest>::failure(
      valid.code(), valid.message());
  }
  if (!backend_) return backend_unavailable_result<StructuredBlobManifest>();
  auto loaded = backend_->load_manifest(key);
  if (!loaded.ok()) return loaded;
  if (!(loaded.value().key == key)) {
    return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::BackendFailure,
      "backend returned a manifest for a different version key");
  }
  return loaded;
}

StructuredBlobResult<StructuredBlobManifest>
StructuredBlobStorage::load_visible(
  const StructuredBlobVersionKey& key) const {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded;
  const auto& manifest = loaded.value();
  if (!manifest.published ||
      manifest.phase != StructuredBlobPhase::Published)
    return invisible();
  if ((manifest.format_id && manifest.format_id->empty()) ||
      (!manifest.format_id &&
       (!manifest.metadata.empty() || manifest.layout_size != 0))) {
    return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::BackendFailure,
      "published structured blob manifest violates format invariants");
  }
  return loaded;
}

StructuredBlobStatus StructuredBlobStorage::begin(
  const StructuredBlobVersionKey& key) {
  const auto valid = validate_key(key);
  if (!valid.ok()) return valid;
  if (!backend_) return backend_unavailable();

  StructuredBlobManifest manifest;
  manifest.key = key;
  return backend_->create_manifest(manifest);
}

StructuredBlobStatus StructuredBlobStorage::write_payload_range(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  const StructuredBlobBytes& bytes) {
  const auto valid = validate_key(key);
  if (!valid.ok()) return valid;
  if (!backend_) return backend_unavailable();
  return backend_->write_payload_and_advance(key, offset, {std::cref(bytes)});
}

StructuredBlobStatus StructuredBlobStorage::write_payload_batch(
    const StructuredBlobVersionKey& key, std::uint64_t base_offset,
    const std::vector<StructuredBlobBytes>& chunks) {
  const auto valid = validate_key(key);
  if (!valid.ok()) return valid;
  if (!backend_) return backend_unavailable();
  if (chunks.size() > kMaxPayloadWriteChunksPerBatch)
    return StructuredBlobStatus::failure(StructuredBlobError::RangeError,
      "payload write batch exceeds the chunk limit");
  std::uint64_t total = 0;
  StructuredBlobPayloadWriteChunks borrowed;
  borrowed.reserve(chunks.size());
  for (const auto& chunk : chunks) {
    if (chunk.size() > kMaxPayloadWriteBytesPerBatch - total)
      return StructuredBlobStatus::failure(StructuredBlobError::RangeError,
        "payload write batch exceeds the byte limit");
    total += chunk.size();
    borrowed.push_back(std::cref(chunk));
  }
  return backend_->write_payload_and_advance(key, base_offset, borrowed);
}

StructuredBlobStatus StructuredBlobStorage::seal_payload(
  const StructuredBlobVersionKey& key) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  const auto phase = require_phase(loaded.value(),
    StructuredBlobPhase::PayloadOpen, "payload seal");
  if (!phase.ok()) return phase;

  StructuredBlobManifest next;
  if (!next_revision(loaded.value(), next)) return revision_exhausted();
  next.phase = StructuredBlobPhase::PayloadSealed;
  return backend_->replace_manifest(key, loaded.value().revision, next);
}

StructuredBlobStatus StructuredBlobStorage::write_layout(
  const StructuredBlobVersionKey& key,
  const StructuredBlobBytes& layout) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  const auto phase = require_phase(loaded.value(),
    StructuredBlobPhase::PayloadSealed, "layout write");
  if (!phase.ok()) return phase;

  StructuredBlobManifest next;
  if (!next_revision(loaded.value(), next)) return revision_exhausted();
  next.layout_size = static_cast<std::uint64_t>(layout.size());
  next.phase = StructuredBlobPhase::LayoutStored;
  if (layout.empty()) {
    return backend_->replace_manifest(key, loaded.value().revision, next);
  }
  auto written = backend_->write_layout(key, layout);
  if (!written.ok()) return written;
  return backend_->replace_manifest(key, loaded.value().revision, next);
}

StructuredBlobStatus
StructuredBlobStorage::write_metadata_and_format_identity(
  const StructuredBlobVersionKey& key,
  std::optional<std::string> format_id, MetadataMap metadata,
  std::optional<DatasetCatalogArtifact> dataset_catalog,
  MetadataMap* described_metadata) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  const auto phase = require_phase(loaded.value(),
    StructuredBlobPhase::LayoutStored, "metadata write");
  if (!phase.ok()) return phase;

  if (format_id && format_id->empty()) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::InvalidArgument,
      "format identity must be absent or non-empty");
  }
  if (!format_id &&
      (!metadata.empty() || loaded.value().layout_size != 0)) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::InvalidArgument,
      "byte pass-through versions cannot have metadata or layout bytes");
  }

  if (dataset_catalog) {
    auto valid = validate_dataset_catalog(*dataset_catalog);
    if (!valid.ok() || !format_id || dataset_catalog->format_id != *format_id)
      return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,
        valid.ok() ? "dataset catalog format identity mismatch" : valid.error());
    auto stored = backend_->write_dataset_catalog(key, *dataset_catalog);
    if (!stored.ok()) return stored;
    metadata["__iotdbfs.dataset_catalog.protocol"] = MetadataValue(dataset_catalog->protocol);
    metadata["__iotdbfs.dataset_catalog.layout_digest"] = MetadataValue(dataset_catalog->layout_digest);
  } else if (metadata.count("__iotdbfs.dataset_catalog.protocol") ||
             metadata.count("__iotdbfs.dataset_catalog.layout_digest")) {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,
      "dataset catalog marker requires a matching catalog artifact");
  }
  if (!dataset_catalog) {
    for (const auto& item : metadata)
      if (item.first.compare(0, 24, "__iotdbfs.dataset_index.") == 0)
        return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,
          "dataset index publication seal requires a matching catalog artifact");
  }
  StructuredBlobManifest next;
  if (!next_revision(loaded.value(), next)) return revision_exhausted();
  next.format_id = std::move(format_id);
  next.metadata = std::move(metadata);
  next.phase = StructuredBlobPhase::Described;
  if (dataset_catalog) {
    auto sealed = seal_dataset_substructure_index(next, *dataset_catalog);
    if (!sealed.ok()) return sealed;
  }
  auto written = backend_->write_metadata(key, next.metadata);
  if (!written.ok()) return written;
  auto replaced = backend_->replace_manifest(key, loaded.value().revision, next);
  if (replaced.ok() && described_metadata) *described_metadata = next.metadata;
  return replaced;
}

StructuredBlobStatus StructuredBlobStorage::publish(
  const StructuredBlobVersionKey& key) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  const auto phase = require_phase(loaded.value(),
    StructuredBlobPhase::Described, "publish");
  if (!phase.ok()) return phase;

  const auto& current = loaded.value();
  if (!current.format_id &&
      (!current.metadata.empty() || current.layout_size != 0)) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::InvalidState,
      "byte pass-through manifest has format-derived state");
  }

  const auto marker = current.metadata.find("__iotdbfs.dataset_catalog.protocol");
  if (marker != current.metadata.end()) {
    auto artifact = backend_->read_dataset_catalog(key);
    if (!artifact.ok()) return artifact.status();
    const auto digest = current.metadata.find("__iotdbfs.dataset_catalog.layout_digest");
    if (marker->second.type() != MetadataValueType::String ||
        digest == current.metadata.end() || digest->second.type() != MetadataValueType::String ||
        artifact.value().protocol != std::get<std::string>(marker->second.value()) ||
        artifact.value().layout_digest != std::get<std::string>(digest->second.value()))
      return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
        "dataset catalog publication identity is missing or inconsistent");
    auto sealed = validate_dataset_substructure_index(current, artifact.value());
    if (!sealed.ok()) return sealed;
  } else {
    for (const auto& item : current.metadata)
      if (item.first.compare(0, 24, "__iotdbfs.dataset_index.") == 0)
        return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
          "dataset index publication seal requires its catalog marker");
  }
  StructuredBlobManifest next;
  if (!next_revision(current, next)) return revision_exhausted();
  next.phase = StructuredBlobPhase::Published;
  next.published = true;
  return backend_->replace_manifest(key, current.revision, next);
}

StructuredBlobStatus StructuredBlobStorage::abort(
  const StructuredBlobVersionKey& key) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  if (loaded.value().published ||
      loaded.value().phase == StructuredBlobPhase::Published) {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "a published structured blob version cannot be aborted");
  }
  return backend_->erase(key, loaded.value().revision);
}

StructuredBlobStatus StructuredBlobStorage::remove(
  const StructuredBlobVersionKey& key) {
  auto loaded = load_for_write(key);
  if (!loaded.ok()) return loaded.status();
  return backend_->erase(key, loaded.value().revision);
}

StructuredBlobResult<StructuredBlobManifest>
StructuredBlobStorage::load_manifest(
  const StructuredBlobVersionKey& key) const {
  auto budget = shared_read_budget(backend_.get());
  return load_visible(key);
}

StructuredBlobResult<StructuredBlobReadHandle>
StructuredBlobStorage::open_published_version(
    const StructuredBlobVersionKey& key) const {
  auto budget = shared_read_budget(backend_.get());
  auto visible = load_visible(key);
  if (!visible.ok()) {
    return StructuredBlobResult<StructuredBlobReadHandle>::failure(
      visible.status().code(), visible.status().message());
  }
  auto payload_reader = backend_->bind_published_payload(visible.value());
  if (!payload_reader.ok()) return StructuredBlobResult<StructuredBlobReadHandle>::failure(
    payload_reader.status().code(), payload_reader.status().message());
  return StructuredBlobResult<StructuredBlobReadHandle>::success(
    StructuredBlobReadHandle(backend_, std::move(visible.value()), std::move(payload_reader.value())));
}

StructuredBlobResult<StructuredBlobReadHandle>
StructuredBlobStorage::open_catalog_version(
    const StructuredBlobVersionKey& key, const StructuredBlobManifest& manifest,
    std::uint64_t catalog_namespace) const {
  auto budget = shared_read_budget(backend_.get());
  using Result = StructuredBlobResult<StructuredBlobReadHandle>;
  const auto valid = validate_key(key);
  if (!valid.ok()) return Result::failure(valid.code(), valid.message());
  if (!backend_) return backend_unavailable_result<StructuredBlobReadHandle>();
  if (!catalog_namespace || catalog_namespace != storage_namespace() ||
      !(manifest.key == key))
    return Result::failure(StructuredBlobError::BackendFailure,
      "catalog manifest namespace or version key mismatch");
  if (!manifest.published || manifest.phase != StructuredBlobPhase::Published)
    return Result::failure(StructuredBlobError::NotFound,
      "catalog manifest is not published");
  if (!manifest.metadata_materialized) {
    auto fresh = open_published_version(key);
    if (!fresh.ok()) return fresh;
    const auto& actual = fresh.value().manifest();
    if (!actual.metadata_materialized || actual.format_id != manifest.format_id ||
        actual.payload_size != manifest.payload_size || actual.layout_size != manifest.layout_size ||
        actual.revision != manifest.revision)
      return Result::failure(StructuredBlobError::BackendFailure,
        "catalog skeleton differs from exact content manifest");
    return fresh;
  }
  if ((manifest.format_id && manifest.format_id->empty()) ||
      (!manifest.format_id && (!manifest.metadata.empty() || manifest.layout_size != 0)))
    return Result::failure(StructuredBlobError::BackendFailure,
      "published structured blob manifest violates format invariants");
  auto payload_reader = backend_->bind_published_payload(manifest);
  if (!payload_reader.ok()) return Result::failure(payload_reader.status().code(), payload_reader.status().message());
  return Result::success(StructuredBlobReadHandle(backend_, manifest, std::move(payload_reader.value())));
}

StructuredBlobResult<StructuredBlobBytes>
StructuredBlobReadHandle::read_payload_range(std::uint64_t offset,
    std::uint64_t length) const {
  const auto size = manifest_.payload_size;
  if (offset > size || length > size - offset ||
      length > static_cast<std::uint64_t>(
        std::numeric_limits<std::size_t>::max())) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::RangeError,
      "payload read range is outside the published payload");
  }
  if (length == 0)
    return StructuredBlobResult<StructuredBlobBytes>::success({});
  versioning::detail::PayloadBlockFlights::ReuseScope payload_reuse;
  auto bytes = [&]() -> StructuredBlobResult<StructuredBlobBytes> {
    if (!payload_reader_) return backend_->read_payload_range(manifest_.key, offset, length);
    if (length > kMaxSharedPayloadReadBytes)
      return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::RangeError,
        "shared payload scalar read exceeds the request byte limit");
    // A scalar request can span several bounded native batches while keeping
    // the same verified root and cumulative budget throughout the read. Shared
    // cache producers have a neutral trace instead of any waiter's ledger.
    std::unique_ptr<ScopedQueryMetadataBudget> scope;
    if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
      scope = std::make_unique<ScopedQueryMetadataBudget>();
    StructuredBlobBytes result;
    result.reserve(static_cast<std::size_t>(length));
    for (std::uint64_t done = 0; done < length;) {
      const auto count = std::min(length - done, kMaxPayloadBytesPerBatch);
      auto batch = payload_reader_->read_payload_ranges({{offset + done, count}});
      if (!batch.ok()) return StructuredBlobResult<StructuredBlobBytes>::failure(
        batch.status().code(), batch.status().message());
      if (batch.value().size() != 1 || batch.value()[0].size() != count)
        return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::BackendFailure,
          "pinned payload reader returned an unexpected byte count");
      // Keep the eager reserve above (including its failure point), but avoid
      // copying a complete, validated result when no concatenation is needed.
      if (count == length)
        result = std::move(batch.value()[0]);
      else
        result.insert(result.end(), batch.value()[0].begin(), batch.value()[0].end());
      done += count;
    }
    return StructuredBlobResult<StructuredBlobBytes>::success(std::move(result));
  }();
  if (!bytes.ok()) return bytes;
  if (bytes.value().size() != length) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure,
      "backend payload read returned an unexpected byte count");
  }
  return bytes;
}

StructuredBlobResult<StructuredBlobPayloadBatch>
StructuredBlobReadHandle::read_payload_ranges(
    const StructuredBlobPayloadRanges& ranges) const {
  if (ranges.empty() || ranges.size() > kMaxPayloadRangesPerBatch)
    return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
      StructuredBlobError::RangeError,
      "payload batch range count is outside the supported bounds");

  std::uint64_t total = 0;
  for (const auto& range : ranges) {
    if (range.length == 0 || range.offset > manifest_.payload_size ||
        range.length > manifest_.payload_size - range.offset ||
        range.length > static_cast<std::uint64_t>(
          std::numeric_limits<std::size_t>::max()) ||
        range.length > kMaxPayloadBytesPerBatch - total) {
      return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
        StructuredBlobError::RangeError,
        "payload batch range is outside the published payload or batch budget");
    }
    total += range.length;
  }

  auto bytes = payload_reader_ ? payload_reader_->read_payload_ranges(ranges) :
    backend_->read_payload_ranges(manifest_.key, ranges);
  if (!bytes.ok()) return bytes;
  if (bytes.value().size() != ranges.size()) {
    return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
      StructuredBlobError::BackendFailure,
      "backend payload batch returned an unexpected range count");
  }
  for (std::size_t i = 0; i < ranges.size(); ++i) {
    if (bytes.value()[i].size() != ranges[i].length) {
      return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
        StructuredBlobError::BackendFailure,
        "backend payload batch returned an unexpected byte count");
    }
  }
  return bytes;
}

StructuredBlobResult<StructuredBlobBytes>
StructuredBlobReadHandle::read_layout_range(std::uint64_t offset,
    std::uint64_t length) const {
  const auto size = manifest_.layout_size;
  if (offset > size || length > size - offset ||
      length > static_cast<std::uint64_t>(
        std::numeric_limits<std::size_t>::max())) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::RangeError,
      "layout read range is outside the published layout");
  }
  if (length == 0)
    return StructuredBlobResult<StructuredBlobBytes>::success({});
  auto layout = payload_reader_ ? payload_reader_->read_layout_range(offset, length) :
    backend_->read_layout_range(manifest_.key, offset, length);
  if (!layout.ok()) return layout;
  if (layout.value().size() != length) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure,
      "backend layout range returned an unexpected byte count");
  }
  return layout;
}

StructuredBlobResult<DatasetCatalogArtifact>
StructuredBlobStorage::load_dataset_catalog(const StructuredBlobVersionKey& key) const {
  auto budget = shared_read_budget(backend_.get());
  auto manifest = load_visible(key);
  if (!manifest.ok()) return StructuredBlobResult<DatasetCatalogArtifact>::failure(
    manifest.status().code(), manifest.status().message());
  const auto& metadata = manifest.value().metadata;
  auto protocol = metadata.find("__iotdbfs.dataset_catalog.protocol");
  auto digest = metadata.find("__iotdbfs.dataset_catalog.layout_digest");
  if (protocol == metadata.end() || digest == metadata.end() ||
      protocol->second.type() != MetadataValueType::String ||
      digest->second.type() != MetadataValueType::String)
    return StructuredBlobResult<DatasetCatalogArtifact>::failure(StructuredBlobError::NotFound,
      "published version has no dataset catalog; reingest it");
  auto artifact = backend_->read_dataset_catalog(key);
  if (!artifact.ok()) return artifact;
  if (artifact.value().protocol != std::get<std::string>(protocol->second.value()) ||
      artifact.value().layout_digest != std::get<std::string>(digest->second.value()) ||
      artifact.value().format_id != manifest.value().format_id.value_or(""))
    return StructuredBlobResult<DatasetCatalogArtifact>::failure(StructuredBlobError::InvalidState,
      "dataset catalog differs from published identity");
  return artifact;
}

StructuredBlobResult<StructuredBlobBytes>
StructuredBlobStorage::load_layout(
  const StructuredBlobVersionKey& key) const {
  auto budget = shared_read_budget(backend_.get());
  auto reader = open_published_version(key);
  if (!reader.ok()) return bytes_failure(reader.status());
  if (reader.value().manifest().layout_size >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure,
      "layout size cannot be represented by this process");
  }
  return reader.value().read_layout_range(0,
    reader.value().manifest().layout_size);
}

StructuredBlobResult<StructuredBlobBytes>
StructuredBlobStorage::read_payload_range(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  std::uint64_t length) const {
  auto budget = shared_read_budget(backend_.get());
  auto reader = open_published_version(key);
  if (!reader.ok()) return bytes_failure(reader.status());
  return reader.value().read_payload_range(offset, length);
}

StructuredBlobResult<StructuredBlobBytes>
StructuredBlobStorage::read_layout_range(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  std::uint64_t length) const {
  auto budget = shared_read_budget(backend_.get());
  auto reader = open_published_version(key);
  if (!reader.ok()) return bytes_failure(reader.status());
  return reader.value().read_layout_range(offset, length);
}

} // namespace iotdbfs
