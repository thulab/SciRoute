#pragma once

#include "iotdbfs/Ingestion.h"
#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/migration/MigrationPolicy.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

struct MigrationInventoryEntry {
  StructuredBlobVersionKey source;
  std::uint64_t bytes{0};
  std::string sha256;
  MigrationKind kind{MigrationKind::Error};
  std::string detail;
  std::string profile_json{"{}"};
  bool partially_queryable{false};
  std::string layout_sha256;
};

struct MigrationResult {
  StructuredBlobVersionKey source;
  StructuredBlobVersionKey destination;
  std::string source_sha256;
  std::string destination_sha256;
  // Frozen before writes; recovery also re-derives it from the exact source.
  std::string layout_sha256;
  std::uint64_t bytes{0};
  std::uint64_t layout_bytes{0};
  std::string adapter_id;
  std::string layout_version;
  std::string state;
  std::string error;
  bool ok() const { return state == "verified"; }
};

// The IoTDB implementation must hold one cross-process namespace writer lock
// for the entire run. These callbacks are a test seam, not a second writer.
struct ReingestionNamespace {
  std::function<StructuredBlobResult<std::vector<StructuredBlobManifest>>()>
    published_manifests;
  std::function<std::optional<Inode>(const std::string&)> latest_inode;
  std::function<std::optional<Inode>(const StructuredBlobVersionKey&)> exact_inode;
  std::function<bool(const Inode&)> prepare_inode;
  std::function<std::int64_t(std::int64_t)> next_version;
};

class ReingestionService {
public:
  using BeforeIngest = std::function<bool(const MigrationResult&)>;
  ReingestionService(StructuredBlobStorage& storage,
    const FormatRegistry& formats, ReingestionNamespace name_space,
    std::shared_ptr<const ReingestionPolicy> policy);

  // Inspect only each path's latest published version. Historical entries are
  // explicitly retained, with no full payload reads or automatic migration.
  StructuredBlobResult<std::vector<MigrationInventoryEntry>> inventory(
    const std::optional<std::string>& path = std::nullopt) const;
  // BeforeIngest must durably record the exact destination before any DB write.
  MigrationResult migrate(const MigrationInventoryEntry& entry,
    BeforeIngest before_ingest) const;
  // Re-derive source layout; validate both exact byte hashes, target layout and
  // namespace inode. A published verification failure never rolls latest back.
  MigrationResult reconcile(const MigrationResult& journal) const;

private:
  MigrationInventoryEntry inspect(const StructuredBlobManifest& manifest) const;
  StructuredBlobResult<std::string> hash_payload(
    const StructuredBlobReadHandle& source) const;
  StructuredBlobStorage& storage_;
  const FormatRegistry& formats_;
  ReingestionNamespace namespace_;
  const std::shared_ptr<const ReingestionPolicy> policy_;
};

} // namespace iotdbfs
