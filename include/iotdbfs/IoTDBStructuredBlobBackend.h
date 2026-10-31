#pragma once

#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/IoTDBDatasetCatalogCache.h"
#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "iotdbfs/NamespaceWriterLock.h"
#include "iotdbfs/HotQueryCache.h"
#include "iotdbfs/storage/NativeFileStore.h"

#include <memory>
#include <cstddef>
#include <map>
#include <atomic>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

class SessionDataSet;
class Tablet;

namespace iotdbfs {

class PublishedVersionCatalog;
class IoTDBStructuredBlobBackend;
class VersionDirectoryReadContext;
struct VersionDirectorySnapshot;
struct HotCatalogBuild;
struct VersionDirectoryConfig {
  std::string table_prefix;
  std::string owner_directory;
  bool initialize{false};
  versioning::LayoutValidator artifact_validator;
  std::string native_file_root;
  std::size_t reader_pool_size{4};
  bool payload_read_coalescing{true};
  std::size_t payload_cache_bytes{8ULL * 1024 * 1024};
};
class NamespaceMutation {
public:
  ~NamespaceMutation();
  bool commit();
private:
  friend class IoTDBStructuredBlobBackend;
  explicit NamespaceMutation(IoTDBStructuredBlobBackend&);
  IoTDBStructuredBlobBackend& backend_;
  std::unique_lock<std::recursive_mutex> lock_;
  bool finished_{false};
};

// IoTDB persistence adapter for the Structured Blob Storage Module. It owns
// the physical table layout; filesystem and format-adapter code only deals in
// manifests, payload ranges, adapter-private layout bytes, and namespace
// inodes.
class IoTDBStructuredBlobBackend final : public StructuredBlobStorageBackend,
    public std::enable_shared_from_this<IoTDBStructuredBlobBackend> {
public:
  IoTDBStructuredBlobBackend() = default;
  bool configure_version_directory(const VersionDirectoryConfig&);
  // Explicitly supplement an already-published exact version. Scientific
  // content and its descriptor/seal remain unchanged.
  StructuredBlobStatus bind_native_file(const StructuredBlobVersionKey&);
  // Privileged maintenance audit: binds the complete committed scientific and
  // POSIX exact record, independently of supplemental filesystem locators.
  StructuredBlobResult<std::string> native_binding_source_identity(
    const StructuredBlobVersionKey&) const;
  bool version_directory_enabled() const { return static_cast<bool>(owner_); }
  bool uses_shared_payload() const override { return static_cast<bool>(owner_); }
  versioning::ContentArtifactStats content_artifact_stats() const;
  QueryResult<std::shared_ptr<HotQueryCache::Binding>> prepare_selector_binding() const;
  std::shared_ptr<HotQueryCache> hot_query_cache() const {return hot_cache_;}
  HotQueryCacheStats hot_query_cache_stats() const {return hot_cache_?hot_cache_->stats():HotQueryCacheStats{};}
  std::uint64_t staging_cleanup_failures() const { return staging_cleanup_failures_.load(); }
  StructuredBlobResult<std::optional<std::string>> verified_layout_identity(const StructuredBlobVersionKey&) const override;
  StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
    bind_published_payload(const StructuredBlobManifest&) const override;
  std::unique_ptr<NamespaceMutation> begin_namespace_mutation();
  StructuredBlobResult<std::vector<Inode>> enumerate_namespace_subtree(const std::string&) const;
  bool ensure_schema();

  // Owner-mode namespace lookups expose identity and POSIX fields only.
  // File metadata comes from load_manifest after caller authorization.
  std::optional<Inode> load_inode(const std::string& path,
    std::optional<long long> version = std::nullopt) const;
  std::vector<Inode> load_children(const std::string& directory,
    bool latest_only = true) const;
  bool upsert_inode(const Inode& inode, bool publish_namespace_entry = false);
  bool remove_path(const std::string& path,
    std::optional<long long> version = std::nullopt);
  bool remove_tree(const std::string& directory);
  // Inventory/migration scans intentionally retain all published versions.
  StructuredBlobResult<std::vector<StructuredBlobManifest>>
  load_published_manifests() const;
  // Query scans apply conservative SQL selection before parsing metadata;
  // latest and residual predicates are still finalized by the catalog.
  StructuredBlobResult<std::vector<StructuredBlobManifest>>
  load_published_manifests(const SourceSelector&) const;
  // Owner snapshots deliberately omit file metadata/content dereferences.
  // Hydrate authorized exact keys with load_manifest before content planning.
  StructuredBlobResult<PublishedVersionSnapshot>
  load_published_snapshot(const SourceSelector&, bool include_ancestors = true) const;
  // Reusable immutable owner candidate domain. This selects identities only;
  // callers still authorize/admit every candidate before reading its content.
  QueryResult<CatalogSelection> scan_published_versions(
    const SourceSelector&, std::size_t max_sources,
    const std::shared_ptr<HotQueryCache::Binding>& binding = {}) const;
  StructuredBlobResult<std::shared_ptr<const SourcePermissionSnapshot>>
  load_latest_inode_permissions(const std::vector<std::string>& paths) const;
  std::uint64_t metadata_generation() const {
    // An authorization boundary must observe a completed mutation, never its
    // early invalidation stamp while the mutable SQL is still in flight.
    if (owner_) return owner_->view().head.sequence;
    std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
    return metadata_generation_.load(std::memory_order_acquire);
  }

  StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) override;
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const override;
  StructuredBlobStatus replace_manifest(
    const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
    const StructuredBlobManifest& next) override;
  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override;
  StructuredBlobStatus write_payload_and_advance(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobPayloadWriteChunks& chunks) override;
  StructuredBlobStatus write_layout(
    const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) override;
  StructuredBlobStatus write_dataset_catalog(
    const StructuredBlobVersionKey&, const DatasetCatalogArtifact&) override;
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
    const StructuredBlobVersionKey&) const override;
  QueryResult<DatasetCatalogResult> discover_datasets(
    const DatasetCatalogRequest&, const DatasetCatalogAuthorizer&,
    const std::shared_ptr<HotQueryCache::Binding>& binding = {}) const;
  StructuredBlobStatus write_metadata(
    const StructuredBlobVersionKey& key,
    const MetadataMap& metadata) override;
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override;
  bool supports_payload_ranges() const override { return true; }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
    const StructuredBlobVersionKey& key,
    const StructuredBlobPayloadRanges& ranges) const override;
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override;
  StructuredBlobStatus erase(
    const StructuredBlobVersionKey& key,
    std::uint64_t expected_revision) override;

private:
  friend class NamespaceMutation;
  friend class VersionDirectoryReadContext;
  QueryResult<DatasetCatalogResult> discover_datasets_impl(
    const DatasetCatalogRequest&,const DatasetCatalogAuthorizer&,HotCatalogBuild*) const;
  std::shared_ptr<const VersionDirectorySnapshot> version_snapshot(const SourceSelector&) const;
  std::shared_ptr<const VersionDirectorySnapshot> parsed_directory_snapshot(const versioning::View&) const;
  mutable std::mutex snapshot_mu_;
  mutable std::shared_ptr<const VersionDirectorySnapshot> snapshot_cache_;
  bool execute_nonquery(const std::string&) const;
  std::shared_ptr<SessionDataSet> execute_query(const std::string&, bool metadata = true) const;
  bool insert_tablet(Tablet&) const;
  std::string physical_sql(const std::string&) const;
  bool commit_namespace_unlocked();
  void abort_namespace_unlocked();
  void put_namespace_record_unlocked(const Inode&, const StructuredBlobManifest&);
  versioning::NamespaceRecord decode_namespace_record_unlocked(const versioning::Bytes&) const;
  std::shared_ptr<const versioning::VerifiedFileContent> verified_content_unlocked(
    const versioning::NamespaceRecord&) const;
  std::shared_ptr<const StructuredBlobPayloadReader> make_payload_reader_unlocked(
    const StructuredBlobManifest&, std::shared_ptr<const versioning::VerifiedFileContent>) const;
  StructuredBlobBytes read_staging_payload_unlocked(const StructuredBlobVersionKey&,
    std::uint64_t offset, std::uint64_t length) const;
  StructuredBlobBytes read_staging_layout_unlocked(const StructuredBlobVersionKey&, std::uint64_t) const;
  StructuredBlobResult<DatasetCatalogArtifact> read_staging_catalog_unlocked(const StructuredBlobVersionKey&) const;
  void cleanup_staging_artifacts_unlocked(const StructuredBlobVersionKey&) const noexcept;
  std::optional<versioning::Bytes> namespace_record_unlocked(const StructuredBlobVersionKey&, bool pending) const;
  std::vector<versioning::Bytes> namespace_records_unlocked(bool pending = false) const;
  std::optional<Inode> load_staging_inode_unlocked(const StructuredBlobVersionKey&) const;
  std::shared_ptr<versioning::Owner> owner_;
  std::shared_ptr<HotQueryCache> hot_cache_;
  std::shared_ptr<versioning::ContentArtifacts> content_artifacts_;
  std::shared_ptr<versioning::PayloadArtifacts> payload_artifacts_;
  std::shared_ptr<NativeFileStore> native_file_store_;
  mutable std::atomic<std::uint64_t> staging_cleanup_failures_{0};
  std::unique_ptr<versioning::Transaction> pending_tx_;
  std::map<std::string, std::optional<versioning::Bytes>> pending_records_;
  std::set<StructuredBlobVersionKey> pending_artifact_cleanup_;
  std::optional<NamespaceWriterLock> version_owner_lock_;
  std::string table_prefix_;
  StructuredBlobResult<StructuredBlobManifest> load_manifest_unlocked(
    const StructuredBlobVersionKey& key) const;
  StructuredBlobStatus persist_manifest_unlocked(
    const StructuredBlobVersionKey& key, const StructuredBlobManifest& current,
    const StructuredBlobManifest& next);
  StructuredBlobStatus require_write_phase_unlocked(
    const StructuredBlobVersionKey&, StructuredBlobPhase, const char*) const;
  StructuredBlobStatus write_payload_range_unlocked(
    const StructuredBlobVersionKey&, std::uint64_t, const StructuredBlobBytes&);
  StructuredBlobStatus validate_dataset_secondary_rows_unlocked(
    const StructuredBlobVersionKey&, const DatasetCatalogArtifact&) const;
  StructuredBlobStatus validate_dataset_publication_unlocked(
    const StructuredBlobManifest&) const;
  struct DatasetCatalogVerification {
    std::vector<DatasetCatalogMarker> markers;
    // Marker errors are returned only after source/seal/time checks, preserving
    // the separate-reader error priority while sharing the physical read.
    QueryStatus marker_status;
  };
  QueryResult<DatasetCatalogVerification> read_dataset_verification_unlocked(
    const std::vector<StructuredBlobManifest>& manifests,
    const std::vector<StructuredBlobVersionKey>& marker_keys,
    std::uint64_t& sql_queries) const;
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog_object_unlocked(
    const StructuredBlobVersionKey& key, std::uint64_t size) const;
  StructuredBlobStatus append_payload_eof(
    const StructuredBlobVersionKey& key, std::uint64_t size);
  StructuredBlobStatus replace_metadata_set_members(
    const StructuredBlobVersionKey& key, const MetadataMap& metadata);
  StructuredBlobResult<bool> version_key_retired_unlocked(
    const StructuredBlobVersionKey& key) const;
  bool retire_published_versions_unlocked(const std::string& predicate);

  // IoTDB has no conditional row update. The filesystem is a single-writer
  // process, so this mutex makes the load/compare/insert CAS honest within the
  // supported deployment boundary. Artifact writes remain idempotent.
  mutable std::recursive_mutex mutation_mu_;
  // Process-local invalidation only; never stored in manifests/fingerprints.
  std::atomic<std::uint64_t> metadata_generation_{0};
  // Instance-local parsed content, synchronized by mutation_mu_.
  mutable DatasetCatalogParsedCache dataset_catalog_cache_;
};

// Live catalog Adapter: every scan loads a new published-manifest snapshot
// from IoTDB, while QueryModule::prepare fixes the selected exact versions.
std::shared_ptr<const PublishedVersionCatalog>
make_iotdb_published_version_catalog(
  std::shared_ptr<const IoTDBStructuredBlobBackend> backend);

} // namespace iotdbfs
