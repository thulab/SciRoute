#pragma once

#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/DatasetCatalogQuery.h"

#include <algorithm>
#include <map>
#include <limits>
#include <set>

namespace iotdbfs {

inline constexpr std::size_t kMaxInodePermissionPaths = 65536;
inline constexpr std::size_t kMaxInodePermissionPathsPerQuery = 128;
inline constexpr std::size_t kMaxInodePermissionSqlBytes = 64 * 1024;

inline QueryResult<std::vector<std::string>> normalize_inode_permission_paths(
    const std::vector<std::string>& paths) {
  if (paths.size() > kMaxInodePermissionPaths)
    return QueryResult<std::vector<std::string>>::failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Validation,
      "inode permission path count exceeds the limit"));
  std::set<std::string> unique;
  for (const auto& path : paths) {
    if (path.empty() || path.size() > 4096 || path.find('\0') != std::string::npos)
      return QueryResult<std::vector<std::string>>::failure(QueryStatus::failure(
        QueryError::InvalidQuery, QueryPhase::Validation,
        "invalid inode permission path"));
    unique.insert(path_utils::norm(path));
  }
  return QueryResult<std::vector<std::string>>::success(
    std::vector<std::string>(unique.begin(), unique.end()));
}

inline std::string inode_permission_path_list(const std::vector<std::string>& paths) {
  std::string list;
  for (const auto& path : paths) {
    if (!list.empty()) list += ',';
    list += query_utils::sql_literal(path);
  }
  return list;
}

// Rows must arrive in descending version order. Mark the newest row observed
// before validating permissions so an unusable newest inode never falls back
// to an older, more permissive directory. All requested paths start as nullopt.
inline bool observe_latest_parent_permission(SourcePermissionSnapshot& snapshot,
    std::set<std::string>& seen, const std::string& path, std::int64_t version,
    std::optional<int> mode, std::optional<int> uid, std::optional<int> gid,
    std::optional<bool> is_dir) {
  const auto requested = snapshot.parents.find(path);
  if (requested == snapshot.parents.end()) return false;
  if (!seen.insert(path).second) return true;
  if (version < 0) return true;
  if (mode && uid && gid && is_dir && *mode >= 0 && *uid >= 0 && *gid >= 0)
    requested->second = CatalogParentPermission{version, *mode, *uid, *gid, *is_dir};
  return true;
}

// Do not restrict is_dir before taking the newest published row per path:
// replacing a directory by a file must revoke traversal permission.
inline QueryResult<std::vector<std::string>> compile_inode_permission_sql_batches(
    const std::vector<std::string>& paths) {
  auto normalized = normalize_inode_permission_paths(paths);
  if (!normalized.ok())
    return QueryResult<std::vector<std::string>>::failure(normalized.status());
  const std::string prefix =
    "SELECT time,path,mode,uid,gid,is_dir FROM inode WHERE published=true AND path IN (";
  const std::string suffix = ") ORDER BY time DESC,mtime DESC";
  std::vector<std::string> statements;
  std::string list;
  std::size_t count = 0;
  for (const auto& path : normalized.value()) {
    const auto literal = query_utils::sql_literal(path);
    if (count != 0 && (count >= kMaxInodePermissionPathsPerQuery ||
        prefix.size() + list.size() + 1 + literal.size() + suffix.size() >
          kMaxInodePermissionSqlBytes)) {
      statements.push_back(prefix + list + suffix);
      list.clear(); count = 0;
    }
    if (!list.empty()) list += ',';
    list += literal; ++count;
  }
  if (count != 0) statements.push_back(prefix + list + suffix);
  return QueryResult<std::vector<std::string>>::success(std::move(statements));
}

// Manifest identities are frozen before authorization. This query must expose
// finite path tags for pruning while retaining each exact path/version pair.
inline constexpr std::size_t kMaxManifestRevalidationKeys = 65536;
inline constexpr std::size_t kMaxManifestRevalidationKeysPerQuery = 128;
inline constexpr std::size_t kMaxManifestRevalidationSqlBytes = 64 * 1024;

struct ManifestRevalidationSqlBatch {
  std::vector<StructuredBlobVersionKey> keys;
  std::string sql;
};

inline QueryResult<std::set<StructuredBlobVersionKey>>
normalize_manifest_revalidation_keys(const std::vector<StructuredBlobVersionKey>& keys) {
  if (keys.size() > kMaxManifestRevalidationKeys)
    return QueryResult<std::set<StructuredBlobVersionKey>>::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Validation, "manifest revalidation key count exceeds the limit"));
  std::set<StructuredBlobVersionKey> unique;
  for (const auto& key : keys) {
    if (key.path.empty() || key.path.size() > 4096 ||
        key.path.find('\0') != std::string::npos || key.version < 0)
      return QueryResult<std::set<StructuredBlobVersionKey>>::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Validation, "invalid manifest revalidation key"));
    const auto normalized = path_utils::norm(key.path);
    if (normalized.size() > 4096)
      return QueryResult<std::set<StructuredBlobVersionKey>>::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Validation, "invalid normalized manifest revalidation path"));
    unique.insert({normalized, key.version});
  }
  return QueryResult<std::set<StructuredBlobVersionKey>>::success(std::move(unique));
}

inline QueryResult<std::vector<ManifestRevalidationSqlBatch>>
compile_exact_version_sql_batches(
    const std::vector<StructuredBlobVersionKey>& keys, const std::string& prefix) {
  using Result = QueryResult<std::vector<ManifestRevalidationSqlBatch>>;
  auto unique = normalize_manifest_revalidation_keys(keys);
  if (!unique.ok()) return Result::failure(unique.status());
  const auto statement = [&](const std::string& paths, const std::string& pairs,
                             std::size_t count) {
    // One extra row makes any duplicate/unexpected result observable instead
    // of silently truncating it at the number of requested keys.
    return prefix + paths + ") AND (" + pairs + ") LIMIT " +
      std::to_string(count + 1);
  };
  std::vector<ManifestRevalidationSqlBatch> batches;
  ManifestRevalidationSqlBatch batch;
  std::set<std::string> paths_seen;
  std::string paths, pairs;
  for (const auto& key : unique.value()) {
    const auto literal = query_utils::sql_literal(key.path);
    const auto pair = "(path=" + literal + " AND time=" +
      std::to_string(key.version) + ")";
    auto next_paths = paths;
    if (!paths_seen.count(key.path)) {
      if (!next_paths.empty()) next_paths += ',';
      next_paths += literal;
    }
    auto next_pairs = pairs + (pairs.empty() ? "" : " OR ") + pair;
    if (!batch.keys.empty() &&
        (batch.keys.size() >= kMaxManifestRevalidationKeysPerQuery ||
         statement(next_paths, next_pairs, batch.keys.size() + 1).size() >
           kMaxManifestRevalidationSqlBytes)) {
      batch.sql = statement(paths, pairs, batch.keys.size());
      batches.push_back(std::move(batch));
      batch = {}; paths_seen.clear();
      paths.clear(); pairs.clear();
      next_paths = literal; next_pairs = pair;
    }
    if (statement(next_paths, next_pairs, batch.keys.size() + 1).size() >
        kMaxManifestRevalidationSqlBytes)
      return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Validation, "manifest revalidation key exceeds SQL byte limit"));
    paths = std::move(next_paths); pairs = std::move(next_pairs);
    paths_seen.insert(key.path); batch.keys.push_back(key);
  }
  if (!batch.keys.empty()) {
    batch.sql = statement(paths, pairs, batch.keys.size());
    batches.push_back(std::move(batch));
  }
  return Result::success(std::move(batches));
}

inline QueryResult<std::vector<ManifestRevalidationSqlBatch>>
compile_manifest_revalidation_sql_batches(const std::vector<StructuredBlobVersionKey>& keys) {
  return compile_exact_version_sql_batches(keys,
    "SELECT time,path,format_id,size,layout_size,blob_phase,published,"
    "blob_revision FROM inode WHERE path IN (");
}

struct DatasetCatalogMarker {
  StructuredBlobVersionKey key;
  std::int64_t size{0};
  std::string protocol, layout_digest, layout_protocol;
  bool directory_complete{false};
  std::int64_t record_count{0}, attribute_count{0};
};

// One submission, two independently bounded/pruned scans. Tagged UNION rows
// keep source freshness and marker errors separable even when a row is absent.
struct DatasetCatalogVerificationSqlBatch {
  std::vector<StructuredBlobVersionKey> keys, marker_keys;
  std::string sql;
};
inline QueryResult<std::vector<DatasetCatalogVerificationSqlBatch>>
compile_dataset_catalog_verification_sql_batches(
    const std::vector<StructuredBlobVersionKey>& keys,
    const std::vector<StructuredBlobVersionKey>& marker_keys) {
  using Result = QueryResult<std::vector<DatasetCatalogVerificationSqlBatch>>;
  auto sources = normalize_manifest_revalidation_keys(keys);
  if (!sources.ok()) return Result::failure(sources.status());
  auto requested_markers = normalize_manifest_revalidation_keys(marker_keys);
  if (!requested_markers.ok()) return Result::failure(requested_markers.status());
  const auto& source_set = sources.value();
  const auto& marker_set = requested_markers.value();
  for (const auto& key : marker_set)
    if (!source_set.count(key)) return Result::failure(QueryStatus::failure(
      QueryError::InvalidQuery, QueryPhase::Validation,
      "dataset marker key is outside the frozen source set"));
  const std::string source_prefix =
    "SELECT CAST(0 AS INT64) AS kind,time,path,format_id,size,layout_size,blob_phase,published,"
    "blob_revision,'' AS protocol,'' AS layout_digest,'' AS layout_protocol,"
    "false AS directory_complete,CAST(0 AS INT64) AS record_count,"
    "CAST(0 AS INT64) AS attribute_count FROM inode WHERE path IN (";
  const std::string marker_prefix =
    "SELECT CAST(1 AS INT64) AS kind,time,path,'' AS format_id,size,"
    "CAST(0 AS INT64) AS layout_size,0 AS blob_phase,false AS published,"
    "CAST(0 AS INT64) AS blob_revision,protocol,layout_digest,layout_protocol,"
    "directory_complete,record_count,attribute_count FROM blob_dataset_catalog WHERE path IN (";
  struct PredicateText {
    std::set<std::string> paths_seen;
    std::string paths, pairs;
    std::size_t size_after(const StructuredBlobVersionKey& key,
        const std::string& literal, const std::string& pair) const {
      return paths.size() + (paths_seen.count(key.path) ? 0 : literal.size() + !paths.empty()) +
        pairs.size() + pair.size() + (pairs.empty() ? 0 : 4);
    }
    void append(const StructuredBlobVersionKey& key, const std::string& literal,
        const std::string& pair) {
      if (paths_seen.insert(key.path).second) {
        if (!paths.empty()) paths += ',';
        paths += literal;
      }
      if (!pairs.empty()) pairs += " OR ";
      pairs += pair;
    }
    std::string finish(const std::string& prefix, std::size_t count) const {
      return prefix + paths + ") AND (" + pairs + ") LIMIT " + std::to_string(count + 1);
    }
  } source_text, marker_text;
  std::vector<DatasetCatalogVerificationSqlBatch> batches;
  DatasetCatalogVerificationSqlBatch batch;
  const auto size = [&](std::size_t source_chars, std::size_t source_count,
                        std::size_t marker_chars, std::size_t marker_count) {
    auto bytes = source_prefix.size() + source_chars + 15 + std::to_string(source_count + 1).size();
    if (marker_count) bytes += marker_prefix.size() + marker_chars + 15 +
      std::to_string(marker_count + 1).size() + sizeof("() UNION ALL ()") - 1;
    return bytes;
  };
  const auto flush = [&] {
    if (batch.keys.empty()) return;
    batch.sql = source_text.finish(source_prefix, batch.keys.size());
    if (!batch.marker_keys.empty()) batch.sql = "(" + batch.sql + ") UNION ALL (" +
      marker_text.finish(marker_prefix, batch.marker_keys.size()) + ")";
    batches.push_back(std::move(batch));
    batch = {}; source_text = {}; marker_text = {};
  };
  for (const auto& key : source_set) {
    const auto literal = query_utils::sql_literal(key.path);
    const auto pair = "(path=" + literal + " AND time=" + std::to_string(key.version) + ")";
    const bool marker = marker_set.count(key) != 0;
    const auto next_size = [&] {
      return size(source_text.size_after(key, literal, pair), batch.keys.size() + 1,
        marker ? marker_text.size_after(key, literal, pair) : marker_text.paths.size() + marker_text.pairs.size(),
        batch.marker_keys.size() + marker);
    };
    if (!batch.keys.empty() && (batch.keys.size() == kMaxManifestRevalidationKeysPerQuery ||
        next_size() > kMaxManifestRevalidationSqlBytes)) flush();
    if (next_size() > kMaxManifestRevalidationSqlBytes)
      return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Validation, "dataset verification key exceeds SQL byte limit"));
    source_text.append(key, literal, pair); batch.keys.push_back(key);
    if (marker) { marker_text.append(key, literal, pair); batch.marker_keys.push_back(key); }
  }
  flush();
  return Result::success(std::move(batches));
}

inline QueryResult<std::vector<ManifestRevalidationSqlBatch>>
compile_dataset_catalog_marker_sql_batches(const std::vector<StructuredBlobVersionKey>& keys) {
  return compile_exact_version_sql_batches(keys,
    "SELECT time,path,size,protocol,layout_digest,layout_protocol,directory_complete,"
    "record_count,attribute_count FROM blob_dataset_catalog WHERE path IN (");
}

inline QueryStatus validate_dataset_catalog_marker_rows(
    const std::vector<StructuredBlobManifest>& manifests,
    const std::vector<DatasetCatalogMarker>& rows) {
  const auto corrupt = [] {
    return QueryStatus::failure(QueryError::CorruptLayout, QueryPhase::Catalog,
      "dataset catalog marker is malformed or inconsistent");
  };
  const auto missing = [] {
    return QueryStatus::failure(QueryError::LayoutUnavailable, QueryPhase::Catalog,
      "dataset catalog missing; reingestion required");
  };
  std::map<StructuredBlobVersionKey, const StructuredBlobManifest*> expected;
  for (const auto& m : manifests)
    if (!expected.emplace(m.key, &m).second) return corrupt();
  std::set<StructuredBlobVersionKey> seen;
  for (const auto& row : rows) {
    const auto found = expected.find(row.key);
    if (found == expected.end() || !seen.insert(row.key).second ||
        row.size <= 0 || static_cast<std::uint64_t>(row.size) > kMaxDatasetCatalogBytes ||
        row.protocol != "iotdbfs-dataset-catalog/1" || row.layout_digest.size() != 64 ||
        row.layout_digest.find_first_not_of("0123456789abcdef") != std::string::npos ||
        row.layout_protocol.empty() || row.layout_protocol.size() > 128 ||
        row.layout_protocol.find('\0') != std::string::npos || !row.directory_complete ||
        row.record_count < 0 || row.record_count > 65536 ||
        row.attribute_count < 0 || row.attribute_count > 16384)
      return corrupt();
    const auto& metadata = found->second->metadata;
    const auto protocol = metadata.find("__iotdbfs.dataset_catalog.protocol");
    const auto digest = metadata.find("__iotdbfs.dataset_catalog.layout_digest");
    if (protocol == metadata.end() || digest == metadata.end() ||
        protocol->second.type() != MetadataValueType::String ||
        digest->second.type() != MetadataValueType::String) return missing();
    if (std::get<std::string>(protocol->second.value()) != row.protocol ||
        std::get<std::string>(digest->second.value()) != row.layout_digest) return corrupt();
  }
  return seen.size() == expected.size() ? QueryStatus::success() : missing();
}

inline QueryStatus validate_dataset_catalog_marker_artifact(
    const DatasetCatalogMarker& marker, const StructuredBlobManifest& manifest,
    const DatasetCatalogArtifact& artifact, std::uint64_t read_bytes) {
  std::uint64_t attribute_count = 0;
  for (const auto& record : artifact.records) attribute_count += record.attributes.size();
  if (!(marker.key == manifest.key) || marker.size <= 0 ||
      static_cast<std::uint64_t>(marker.size) != read_bytes ||
      artifact.format_id != manifest.format_id || artifact.protocol != marker.protocol ||
      artifact.layout_digest != marker.layout_digest || artifact.layout_protocol != marker.layout_protocol ||
      artifact.directory_complete != marker.directory_complete ||
      marker.record_count < 0 || marker.attribute_count < 0 ||
      artifact.records.size() != static_cast<std::uint64_t>(marker.record_count) ||
      attribute_count != static_cast<std::uint64_t>(marker.attribute_count))
    return QueryStatus::failure(QueryError::CorruptLayout, QueryPhase::Catalog,
      "dataset catalog artifact does not match its marker");
  return QueryStatus::success();
}

inline constexpr std::uint64_t kMaxDatasetCatalogObjectBatchBytes = 16ULL * 1024 * 1024;
struct DatasetCatalogObjectSqlBatch {
  std::vector<DatasetCatalogMarker> markers;
  std::string sql;
  std::uint64_t expected_bytes{0};
};
struct DatasetCatalogObjectRow {
  StructuredBlobVersionKey key;
  std::int64_t size{0};
  std::string bytes;
};

inline QueryResult<std::vector<DatasetCatalogObjectSqlBatch>>
compile_dataset_catalog_object_sql_batches(const std::vector<DatasetCatalogMarker>& markers) {
  using Result = QueryResult<std::vector<DatasetCatalogObjectSqlBatch>>;
  if (markers.size() > kMaxManifestRevalidationKeys)
    return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Validation, "dataset catalog object key count exceeds the limit"));
  std::vector<StructuredBlobVersionKey> keys;
  for (const auto& marker : markers) keys.push_back(marker.key);
  auto normalized = compile_manifest_revalidation_sql_batches(keys);
  if (!normalized.ok()) return Result::failure(normalized.status());
  std::map<StructuredBlobVersionKey, DatasetCatalogMarker> unique;
  for (auto marker : markers) {
    marker.key.path = path_utils::norm(marker.key.path);
    if (marker.size <= 0 || static_cast<std::uint64_t>(marker.size) > kMaxDatasetCatalogObjectBatchBytes ||
        !unique.emplace(marker.key, std::move(marker)).second)
      return Result::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Validation, "invalid or duplicate dataset catalog object marker"));
  }
  const auto statement = [](const std::vector<DatasetCatalogMarker>& batch) {
    std::set<std::string> paths;
    std::string pairs;
    for (const auto& marker : batch) {
      paths.insert(marker.key.path);
      if (!pairs.empty()) pairs += " OR ";
      // Freeze each validated size in the predicate before passing its column
      // to READ_OBJECT. A changed marker cannot enlarge this request's read.
      pairs += "(path=" + query_utils::sql_literal(marker.key.path) + " AND time=" +
        std::to_string(marker.key.version) + " AND size=" + std::to_string(marker.size) + ")";
    }
    return "SELECT time,path,size,READ_OBJECT(data,0,size) FROM blob_dataset_catalog WHERE path IN (" +
      inode_permission_path_list(std::vector<std::string>(paths.begin(), paths.end())) +
      ") AND (" + pairs + ") LIMIT " + std::to_string(batch.size() + 1);
  };
  std::vector<DatasetCatalogObjectSqlBatch> batches;
  DatasetCatalogObjectSqlBatch batch;
  for (const auto& item : unique) {
    auto candidate = batch;
    candidate.markers.push_back(item.second);
    candidate.expected_bytes += static_cast<std::uint64_t>(item.second.size);
    candidate.sql = statement(candidate.markers);
    if (!batch.markers.empty() && (candidate.markers.size() > kMaxManifestRevalidationKeysPerQuery ||
        candidate.expected_bytes > kMaxDatasetCatalogObjectBatchBytes ||
        candidate.sql.size() > kMaxManifestRevalidationSqlBytes)) {
      batches.push_back(std::move(batch)); candidate = {};
      candidate.markers.push_back(item.second);
      candidate.expected_bytes = static_cast<std::uint64_t>(item.second.size);
      candidate.sql = statement(candidate.markers);
    }
    if (candidate.sql.size() > kMaxManifestRevalidationSqlBytes)
      return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Validation, "dataset catalog object SQL exceeds the byte limit"));
    batch = std::move(candidate);
  }
  if (!batch.markers.empty()) batches.push_back(std::move(batch));
  return Result::success(std::move(batches));
}

inline QueryStatus validate_dataset_catalog_object_rows(
    const std::vector<DatasetCatalogMarker>& markers,
    const std::vector<DatasetCatalogObjectRow>& rows) {
  const auto failure = [](QueryError code) {
    return QueryStatus::failure(code, QueryPhase::Catalog,
      "dataset catalog object rows are missing, malformed or inconsistent");
  };
  std::map<StructuredBlobVersionKey, std::int64_t> expected;
  for (const auto& marker : markers)
    if (!expected.emplace(marker.key, marker.size).second) return failure(QueryError::CorruptLayout);
  std::set<StructuredBlobVersionKey> seen;
  for (const auto& row : rows) {
    const auto found = expected.find(row.key);
    if (found == expected.end() || !seen.insert(row.key).second) return failure(QueryError::CorruptLayout);
    if (row.size <= 0 || row.size != found->second ||
        row.bytes.size() != static_cast<std::uint64_t>(row.size)) return failure(QueryError::ReadFailure);
  }
  return seen.size() == expected.size() ? QueryStatus::success() : failure(QueryError::ReadFailure);
}

// Metadata was already read in the immutable selected snapshot. Revalidation
// compares the publication frontier and identity fields without rereading it.
inline QueryStatus validate_manifest_revalidation_rows(
    const std::vector<StructuredBlobManifest>& expected,
    const std::vector<StructuredBlobManifest>& observed) {
  const auto stale = [] {
    return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog,
      "dataset discovery source changed after authorization");
  };
  std::map<StructuredBlobVersionKey, const StructuredBlobManifest*> identities;
  for (const auto& m : expected) {
    const StructuredBlobVersionKey key{path_utils::norm(m.key.path), m.key.version};
    if (!identities.emplace(key, &m).second) return stale();
  }
  std::set<StructuredBlobVersionKey> seen;
  for (const auto& m : observed) {
    const auto match = identities.find(m.key);
    if (match == identities.end() || !seen.insert(m.key).second) return stale();
    const auto& selected = *match->second;
    if (!m.published || m.phase != StructuredBlobPhase::Published ||
        m.revision != selected.revision || m.format_id != selected.format_id ||
        m.payload_size != selected.payload_size || m.layout_size != selected.layout_size)
      return stale();
  }
  return seen.size() == identities.size() ? QueryStatus::success() : stale();
}

struct DatasetCatalogSqlSource {
  StructuredBlobVersionKey key;
  std::uint64_t expected_records{0}, expected_attributes{0};
};
enum class DatasetCatalogVisibility {
  PublishedInode,
  // Caller has frozen, authorized and revalidated these exact keys against
  // the committed owner root while holding its mutation boundary. The SQL
  // inode is only staging in this mode and must not decide visibility.
  OwnerSelectedExactKeys
};
struct DatasetCatalogIndexSqlBatch {
  std::vector<DatasetCatalogSqlSource> sources;
  std::string entry_count_sql, attribute_count_sql, filter_sql;
  std::uint64_t expected_records{0};
};
struct DatasetCatalogIndexCountRow {
  StructuredBlobVersionKey key;
  std::int64_t count{0};
};
struct DatasetCatalogSqlObjectRow {
  StructuredBlobVersionKey key;
  std::string object_id;
};
using DatasetCatalogSqlObjects = std::map<StructuredBlobVersionKey, std::set<std::string>>;

// Shared finite tag pruning plus exact identity pairs; independent path/time
// IN lists would incorrectly admit cross-paired versions.
inline std::string dataset_catalog_exact_key_predicate(
    const std::vector<DatasetCatalogSqlSource>& sources, const std::string& alias = "") {
  std::set<std::string> unique_paths;
  std::string pairs;
  for (const auto& source : sources) {
    unique_paths.insert(source.key.path);
    if (!pairs.empty()) pairs += " OR ";
    pairs += "(" + alias + "path=" + query_utils::sql_literal(source.key.path) +
      " AND " + alias + "time=" + std::to_string(source.key.version) + ")";
  }
  return alias + "path IN (" + inode_permission_path_list(
    std::vector<std::string>(unique_paths.begin(), unique_paths.end())) + ") AND (" + pairs + ")";
}

inline QueryResult<std::vector<DatasetCatalogIndexSqlBatch>>
compile_dataset_catalog_index_sql_batches(
    const std::vector<DatasetCatalogSqlSource>& sources,
    const DatasetPredicateSql& predicate,
    const std::optional<std::string>& dataset_path = std::nullopt,
    DatasetCatalogVisibility visibility = DatasetCatalogVisibility::PublishedInode) {
  using Result = QueryResult<std::vector<DatasetCatalogIndexSqlBatch>>;
  if (sources.size() > kMaxManifestRevalidationKeys)
    return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Validation, "dataset catalog source count exceeds the limit"));
  std::vector<StructuredBlobVersionKey> keys;
  keys.reserve(sources.size());
  for (const auto& source : sources) keys.push_back(source.key);
  auto normalized = compile_manifest_revalidation_sql_batches(keys);
  if (!normalized.ok()) return Result::failure(normalized.status());
  std::map<StructuredBlobVersionKey, DatasetCatalogSqlSource> unique;
  for (auto source : sources) {
    source.key.path = path_utils::norm(source.key.path);
    if (!unique.emplace(source.key, std::move(source)).second)
      return Result::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Validation, "duplicate dataset catalog source key"));
  }
  const auto exhausted = [] {
    return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Validation, "dataset catalog batch exceeds SQL or row limits"));
  };
  const auto finish = [&](DatasetCatalogIndexSqlBatch& batch) {
    const auto exact = dataset_catalog_exact_key_predicate(batch.sources);
    const auto suffix = " WHERE " + exact + " GROUP BY path,time LIMIT " +
      std::to_string(batch.sources.size() + 1);
    batch.entry_count_sql = "SELECT path,time,count(*) FROM blob_dataset_entry" + suffix;
    batch.attribute_count_sql = "SELECT path,time,count(*) FROM blob_dataset_attribute" + suffix;
    batch.filter_sql = "SELECT d.path,d.time,d.object_id FROM blob_dataset_entry d";
    if (visibility == DatasetCatalogVisibility::PublishedInode)
      batch.filter_sql += " JOIN inode i ON d.path=i.path AND d.time=i.time";
    for (std::size_t j = 0; j < predicate.attributes.size(); ++j) {
      const auto alias = "a" + std::to_string(j);
      batch.filter_sql += " LEFT JOIN blob_dataset_attribute " + alias +
        " ON d.path=" + alias + ".path AND d.time=" + alias +
        ".time AND d.object_id=" + alias + ".object_id AND " + alias +
        ".attribute_name=" + query_utils::sql_literal(predicate.attributes[j]);
    }
    batch.filter_sql += " WHERE ";
    if (visibility == DatasetCatalogVisibility::PublishedInode)
      batch.filter_sql += "i.published=true AND ";
    batch.filter_sql += dataset_catalog_exact_key_predicate(batch.sources, "d.");
    if (dataset_path) batch.filter_sql += " AND d.dataset_path=" + query_utils::sql_literal(*dataset_path);
    batch.filter_sql += " AND (" + predicate.predicate +
      ") ORDER BY d.path,d.time,d.object_id LIMIT " + std::to_string(batch.expected_records + 1);
    return batch.entry_count_sql.size() <= kMaxManifestRevalidationSqlBytes &&
      batch.attribute_count_sql.size() <= kMaxManifestRevalidationSqlBytes &&
      batch.filter_sql.size() <= kMaxManifestRevalidationSqlBytes;
  };
  std::vector<DatasetCatalogIndexSqlBatch> batches;
  DatasetCatalogIndexSqlBatch batch;
  for (const auto& item : unique) {
    const auto& source = item.second;
    // IoTDB LIMIT and count results are signed INT64. Keep one sentinel row.
    const auto max_records = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) - 1;
    if (source.expected_records > max_records ||
        source.expected_attributes > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
      return exhausted();
    auto candidate = batch;
    candidate.sources.push_back(source);
    const bool row_limit = source.expected_records > max_records - candidate.expected_records;
    if (!row_limit) candidate.expected_records += source.expected_records;
    if (!batch.sources.empty() && (candidate.sources.size() > kMaxManifestRevalidationKeysPerQuery ||
        row_limit || !finish(candidate))) {
      batches.push_back(std::move(batch));
      batch = {}; candidate = {};
      candidate.sources.push_back(source); candidate.expected_records = source.expected_records;
    }
    if (!finish(candidate)) return exhausted();
    batch = std::move(candidate);
  }
  if (!batch.sources.empty()) batches.push_back(std::move(batch));
  return Result::success(std::move(batches));
}

inline QueryStatus validate_dataset_catalog_index_counts(
    const std::vector<DatasetCatalogSqlSource>& sources,
    const std::vector<DatasetCatalogIndexCountRow>& rows, bool attributes) {
  const auto corrupt = [] {
    return QueryStatus::failure(QueryError::CorruptLayout, QueryPhase::Catalog,
      "dataset attribute secondary index is incomplete");
  };
  std::map<StructuredBlobVersionKey, std::uint64_t> expected;
  for (const auto& source : sources)
    if (!expected.emplace(source.key, attributes ? source.expected_attributes :
        source.expected_records).second) return corrupt();
  std::set<StructuredBlobVersionKey> seen;
  for (const auto& row : rows) {
    const auto found = expected.find(row.key);
    if (found == expected.end() || !seen.insert(row.key).second || row.count < 0 ||
        static_cast<std::uint64_t>(row.count) != found->second) return corrupt();
  }
  for (const auto& source : expected)
    if (!seen.count(source.first) && source.second != 0) return corrupt();
  return QueryStatus::success();
}

inline QueryStatus validate_dataset_catalog_filter_rows(
    const DatasetCatalogSqlObjects& expected,
    const std::vector<DatasetCatalogSqlObjectRow>& rows) {
  std::set<std::pair<StructuredBlobVersionKey, std::string>> seen;
  for (const auto& row : rows) {
    const auto source = expected.find(row.key);
    if (source == expected.end() || !source->second.count(row.object_id) ||
        !seen.emplace(row.key, row.object_id).second)
      return QueryStatus::failure(QueryError::CorruptLayout, QueryPhase::Catalog,
        "dataset SQL returned unknown or duplicate object");
  }
  return QueryStatus::success();
}

// Storage-side selector compilation, kept outside query core. Latest and
// residual metadata predicates are deliberately finalized by the catalog:
// format selection precedes latest, while file_predicate follows it.
inline QueryResult<std::string> compile_published_catalog_sql(
    const SourceSelector& selector, const std::vector<std::string>& ancestors = {}) {
  auto valid = validate_source_selector(selector);
  if (!valid.ok()) return QueryResult<std::string>::failure(std::move(valid));

  std::string predicate = "is_dir=false";
  if (!selector.paths.empty()) {
    std::string paths;
    bool all_paths = false;
    for (const auto& scope : selector.paths.normalized_paths()) {
      const auto& path = scope.path;
      if (scope.kind == PathScopeKind::Subtree && path == "/") {
        all_paths = true;
        break;
      }
      if (!paths.empty()) paths += " OR ";
      const auto exact = "path=" + query_utils::sql_literal(path);
      if (scope.kind == PathScopeKind::Exact) paths += exact;
      else paths += "(" + exact + " OR " +
        query_utils::like_subtree_predicate(path) + ")";
    }
    if (!all_paths) predicate += " AND (" + paths + ")";
  }
  const auto& versions = selector.versions;
  if (versions.kind == VersionSelectionKind::Exact)
    predicate += " AND time=" + std::to_string(*versions.exact);
  else if (versions.kind == VersionSelectionKind::Between)
    predicate += " AND time>=" + std::to_string(*versions.lower_inclusive) +
      " AND time<" + std::to_string(*versions.upper_exclusive);

  const auto& formats = selector.formats;
  if (formats.adapters == AdapterSelectionKind::Listed) {
    std::string format_predicate;
    for (const auto& id : formats.adapter_ids) {
      if (!format_predicate.empty()) format_predicate += " OR ";
      format_predicate += "format_id=" + query_utils::sql_literal(id);
    }
    if (formats.include_byte_passthrough) {
      if (!format_predicate.empty()) format_predicate += " OR ";
      format_predicate += "format_id IS NULL OR format_id=''";
    }
    predicate += " AND (" + (format_predicate.empty() ? "false" : format_predicate) + ")";
  } else if (!formats.include_byte_passthrough) {
    predicate += " AND (format_id IS NOT NULL AND format_id<>'')";
  }
  const std::string prefix =
    "SELECT time,path,format_id,metadata,size,layout_size,blob_phase,"
    "published,blob_revision,mode,uid,gid,is_dir FROM inode WHERE published=true AND ";
  std::string sql = prefix + predicate;
  if (!ancestors.empty()) {
    auto normalized = normalize_inode_permission_paths(ancestors);
    if (!normalized.ok()) return QueryResult<std::string>::failure(normalized.status());
    if (normalized.value().size() > kMaxInodePermissionPathsPerQuery)
      return QueryResult<std::string>::failure(QueryStatus::failure(
        QueryError::ResourceExhausted, QueryPhase::Validation,
        "combined catalog ancestor count exceeds one batch"));
    sql = prefix + "((" + predicate + ") OR path IN (" +
      inode_permission_path_list(normalized.value()) + "))";
    if (!selector.paths.empty() && std::all_of(selector.paths.begin(),
        selector.paths.end(), [](const auto& scope) {
          return scope.kind == PathScopeKind::Exact;
        })) {
      // The OR separates source version/format from ancestor latest lookup,
      // but can obscure IoTDB's tag pruning. This redundant outer constraint
      // exposes the finite candidate paths without narrowing either branch.
      std::set<std::string> candidates(normalized.value().begin(), normalized.value().end());
      for (const auto& scope : selector.paths.normalized_paths())
        candidates.insert(scope.path);
      if (candidates.size() > kMaxInodePermissionPathsPerQuery)
        return QueryResult<std::string>::failure(QueryStatus::failure(
          QueryError::ResourceExhausted, QueryPhase::Validation,
          "combined catalog candidate count exceeds one batch"));
      sql = prefix + "path IN (" + inode_permission_path_list(
        std::vector<std::string>(candidates.begin(), candidates.end())) +
        ") AND ((" + predicate + ") OR path IN (" +
        inode_permission_path_list(normalized.value()) + "))";
    }
  }
  sql += " ORDER BY time DESC";
  if (!ancestors.empty() && sql.size() > kMaxInodePermissionSqlBytes)
    return QueryResult<std::string>::failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Validation,
      "combined catalog SQL exceeds one batch"));
  return QueryResult<std::string>::success(std::move(sql));
}

} // namespace iotdbfs
