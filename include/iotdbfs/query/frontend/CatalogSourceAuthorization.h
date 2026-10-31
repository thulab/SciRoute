#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/PathUtils.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace iotdbfs {

inline std::vector<std::string> query_source_parent_paths(const std::string& path) {
  std::vector<std::string> parents;
  for (auto parent = path_utils::parent_of(path_utils::norm(path));;
      parent = path_utils::parent_of(parent)) {
    parents.push_back(parent);
    if (parent == "/") break;
  }
  std::reverse(parents.begin(), parents.end());
  return parents;
}

inline bool fresh_source_permission_snapshot(const SourcePermissionSnapshot* snapshot,
    std::uint64_t storage_namespace, std::uint64_t metadata_generation,
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
  return snapshot && storage_namespace &&
    snapshot->storage_namespace == storage_namespace &&
    snapshot->metadata_generation == metadata_generation &&
    now >= snapshot->observed_at &&
    now - snapshot->observed_at <= std::chrono::seconds(1);
}

// Permissions are mutable inode data, not immutable manifest state. A catalog
// observation can replace a source read only during its immediate registration
// and only while this backend's inode mutation generation remains unchanged.
inline const CatalogSourcePermission* validated_catalog_source_permission(
    const CatalogSelection* selection, const StructuredBlobManifest* manifest,
    const QuerySourceReference& source, std::uint64_t storage_namespace,
    std::uint64_t metadata_generation,
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
  if (!selection || !manifest || !selection->source_permissions ||
      !storage_namespace || selection->storage_namespace != storage_namespace)
    return nullptr;
  const auto& snapshot = *selection->source_permissions;
  if (!fresh_source_permission_snapshot(&snapshot, storage_namespace,
        metadata_generation, now) ||
      manifest->key.path != source.path || manifest->key.version != source.version ||
      manifest->format_id.value_or("") != source.format_id ||
      !manifest->published || manifest->phase != StructuredBlobPhase::Published)
    return nullptr;
  const auto found = snapshot.sources.find(manifest->key);
  if (found == snapshot.sources.end()) return nullptr;
  const auto& fact = found->second;
  if (fact.format_id != source.format_id ||
      fact.manifest_revision != manifest->revision ||
      fact.payload_size != manifest->payload_size ||
      fact.layout_size != manifest->layout_size ||
      fact.mode < 0 || (fact.mode & ~(S_IFMT | 07777)) != 0 ||
      fact.uid < 0 || fact.gid < 0)
    return nullptr;
  return &fact;
}

inline int check_query_posix_access(int mode, int owner_uid, int owner_gid,
    int mask, uid_t uid, gid_t gid, const std::vector<gid_t>& groups = {}) {
  if (mode < 0 || owner_uid < 0 || owner_gid < 0)
    return -EACCES;
  if (uid == 0) return 0;
  auto bits = mode & 0777;
  if (uid == static_cast<uid_t>(owner_uid)) bits >>= 6;
  else if (gid == static_cast<gid_t>(owner_gid) ||
      std::find(groups.begin(), groups.end(), static_cast<gid_t>(owner_gid)) != groups.end())
    bits >>= 3;
  if ((mask & R_OK) && !(bits & 4)) return -EACCES;
  if ((mask & W_OK) && !(bits & 2)) return -EACCES;
  if ((mask & X_OK) && !(bits & 1)) return -EACCES;
  return 0;
}

struct ResolvedQueryParents {
  std::map<std::string, std::optional<CatalogParentPermission>> observations;
  std::size_t catalog_reuse{0};
};

// Map absence means no observation. A present null is an observed missing or
// malformed newest inode and must never trigger an older-row fallback.
template <class BulkLoader>
QueryResult<ResolvedQueryParents> resolve_query_parent_permissions(
    const SourcePermissionSnapshot* hint, const std::vector<std::string>& paths,
    std::uint64_t storage_namespace, std::uint64_t metadata_generation,
    BulkLoader&& load,
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
  ResolvedQueryParents resolved;
  std::vector<std::string> missing;
  const bool fresh = fresh_source_permission_snapshot(hint, storage_namespace,
    metadata_generation, now);
  const std::set<std::string> unique(paths.begin(), paths.end());
  for (const auto& path : unique) {
    if (fresh) {
      const auto found = hint->parents.find(path);
      if (found != hint->parents.end()) {
        resolved.observations.emplace(path, found->second);
        ++resolved.catalog_reuse;
        continue;
      }
    }
    missing.push_back(path);
  }
  if (!missing.empty()) {
    auto loaded = load(missing);
    if (!loaded.ok()) return QueryResult<ResolvedQueryParents>::failure(loaded.status());
    const auto& snapshot = loaded.value();
    // A direct synchronous observation may take more than one second when
    // large ancestor sets require several SQL waves. The surrounding mutation
    // guard validates the whole operation; the catalog-hint TTL does not apply
    // to metadata read specifically for this authorization attempt.
    if (!snapshot || !storage_namespace ||
        snapshot->storage_namespace != storage_namespace ||
        snapshot->metadata_generation != metadata_generation)
      return QueryResult<ResolvedQueryParents>::failure(QueryStatus::failure(
        QueryError::StalePlan, QueryPhase::Catalog,
        "source parent metadata observation is stale"));
    for (const auto& path : missing) {
      const auto found = snapshot->parents.find(path);
      if (found == snapshot->parents.end())
        return QueryResult<ResolvedQueryParents>::failure(QueryStatus::failure(
          QueryError::ReadFailure, QueryPhase::Catalog,
          "source parent metadata observation is incomplete"));
      resolved.observations.emplace(path, found->second);
    }
  }
  return QueryResult<ResolvedQueryParents>::success(std::move(resolved));
}

inline QueryStatus check_query_parent_permission(
    const std::optional<CatalogParentPermission>& parent,
    const QuerySourceReference& source, uid_t uid, gid_t gid,
    const std::vector<gid_t>& groups) {
  if (!parent || parent->version < 0 || !parent->is_dir || parent->mode < 0 ||
      parent->uid < 0 || parent->gid < 0)
    return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog,
      "typed query source parent is no longer available", QueryErrorDetail{source});
  if (check_query_posix_access(parent->mode, parent->uid, parent->gid,
        X_OK, uid, gid, groups) != 0)
    return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
      "source parent search permission denied", QueryErrorDetail{source});
  return QueryStatus::success();
}

// Retry one racing permission update with fresh source reads. This guard is
// independent of the route authorization TTL and never retains observations.
template <class ReadGeneration, class Check>
QueryStatus authorize_source_permissions_with_generation(
    ReadGeneration&& read_generation, Check&& check) {
  for (int attempt = 0; attempt < 2; ++attempt) {
    const auto generation = read_generation();
    auto status = check(attempt == 0, generation);
    if (read_generation() == generation) return status;
  }
  return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog,
    "typed query source permissions changed during authorization");
}

} // namespace iotdbfs
