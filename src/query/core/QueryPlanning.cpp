#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/formats/arrow/ArrowQueryModel.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"

#include "iotdbfs/PathUtils.h"

#include <algorithm>
#include <cmath>
#include <exception>
#include <map>
#include <set>
#include <tuple>

namespace iotdbfs {
namespace {

QueryStatus query_failure(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail = {}) {
  return QueryStatus::failure(code, phase, std::move(message),
    std::move(detail));
}

QueryStatus invalid_request(std::string message,
    std::optional<std::string> field_id = std::nullopt) {
  QueryErrorDetail detail;
  detail.field_id = std::move(field_id);
  return query_failure(QueryError::InvalidQuery, QueryPhase::Validation,
    std::move(message), std::move(detail));
}

QueryStatus validate_version_selection(const VersionSelection& selection) {
  switch (selection.kind) {
  case VersionSelectionKind::Latest:
  case VersionSelectionKind::All:
    if (selection.exact || selection.lower_inclusive ||
        selection.upper_exclusive)
      return invalid_request(
        "latest/all version selection cannot contain version bounds");
    return QueryStatus::success();
  case VersionSelectionKind::Exact:
    if (!selection.exact || *selection.exact < 0 ||
        selection.lower_inclusive || selection.upper_exclusive)
      return invalid_request(
        "exact version selection requires one non-negative version");
    return QueryStatus::success();
  case VersionSelectionKind::Between:
    if (selection.exact || !selection.lower_inclusive ||
        !selection.upper_exclusive || *selection.lower_inclusive < 0 ||
        *selection.upper_exclusive <= *selection.lower_inclusive)
      return invalid_request(
        "between version selection requires a non-empty [lower, upper) range");
    return QueryStatus::success();
  }
  return invalid_request("unknown version selection kind");
}

QueryStatus validate_source_selector_impl(const SourceSelector& selector) {
  auto version = validate_version_selection(selector.versions);
  if (!version.ok()) return version;

  const auto& paths = selector.paths.validation_status();
  if (!paths.ok()) return paths;
  std::set<std::string> formats;
  if (selector.formats.adapters != AdapterSelectionKind::All &&
      selector.formats.adapters != AdapterSelectionKind::Listed)
    return invalid_request("unknown format selection kind");
  if (selector.formats.adapters == AdapterSelectionKind::All &&
      !selector.formats.adapter_ids.empty())
    return invalid_request(
      "all-format selection cannot contain listed adapter ids");
  for (const auto& id : selector.formats.adapter_ids) {
    if (id.empty())
      return invalid_request("format adapter id cannot be empty");
    if (id.find('\0') != std::string::npos)
      return invalid_request("format adapter id cannot contain a NUL byte");
    if (!formats.insert(id).second)
      return invalid_request("format adapter ids must be unique");
  }
  return QueryStatus::success();
}

bool matches_path(const std::string& path, const PathScopeSet& scopes) {
  return scopes.matches_normalized_path(path_utils::norm(path));
}

bool matches_version(std::int64_t version,
    const VersionSelection& selection) {
  switch (selection.kind) {
  case VersionSelectionKind::Latest:
  case VersionSelectionKind::All:
    return true;
  case VersionSelectionKind::Exact:
    return version == *selection.exact;
  case VersionSelectionKind::Between:
    return version >= *selection.lower_inclusive &&
      version < *selection.upper_exclusive;
  }
  return false;
}

bool matches_format(const StructuredBlobManifest& manifest,
    const FormatSelector& selector) {
  if (!manifest.format_id) return selector.include_byte_passthrough;
  if (selector.adapters == AdapterSelectionKind::All) return true;
  return std::find(selector.adapter_ids.begin(), selector.adapter_ids.end(),
    *manifest.format_id) != selector.adapter_ids.end();
}

bool positive_finite_spacing(const LogicalValue& value) {
  if (value.is_null()) return false;
  if (value.type() == LogicalTypeId::Float32) {
    const auto spacing = std::get<float>(value.value());
    return std::isfinite(spacing) && spacing > 0.0F;
  }
  if (value.type() == LogicalTypeId::Float64) {
    const auto spacing = std::get<double>(value.value());
    return std::isfinite(spacing) && spacing > 0.0;
  }
  return false;
}

QueryResult<CatalogSelection> select_published_snapshot(
    const std::vector<StructuredBlobManifest>& manifests,
    const SourceSelector& selector, std::size_t max_sources) {
  auto valid = validate_source_selector(selector);
  if (!valid.ok())
    return QueryResult<CatalogSelection>::failure(std::move(valid));
  if (max_sources == 0)
    return QueryResult<CatalogSelection>::failure(
      invalid_request("catalog source limit must be positive"));

  std::map<StructuredBlobVersionKey, StructuredBlobManifest> unique;
  for (const auto& manifest : manifests) {
    if (!manifest.published ||
        manifest.phase != StructuredBlobPhase::Published)
      continue;
    if (!matches_path(manifest.key.path, selector.paths) ||
        !matches_version(manifest.key.version, selector.versions) ||
        !matches_format(manifest, selector.formats))
      continue;
    if (!unique.emplace(manifest.key, manifest).second) {
      return QueryResult<CatalogSelection>::failure(query_failure(
        QueryError::Internal, QueryPhase::Catalog,
        "catalog returned a duplicate File Version"));
    }
  }

  std::vector<StructuredBlobManifest> selected;
  if (selector.versions.kind == VersionSelectionKind::Latest) {
    std::map<std::string, StructuredBlobManifest> latest;
    for (const auto& entry : unique) {
      const auto& manifest = entry.second;
      auto found = latest.find(manifest.key.path);
      if (found == latest.end() ||
          found->second.key.version < manifest.key.version)
        latest[manifest.key.path] = manifest;
    }
    for (auto& entry : latest) selected.push_back(std::move(entry.second));
  } else {
    for (const auto& entry : unique) selected.push_back(entry.second);
  }

  std::sort(selected.begin(), selected.end(),
    [](const auto& left, const auto& right) {
      if (left.key.path != right.key.path)
        return left.key.path < right.key.path;
      return left.key.version > right.key.version;
    });
  if (selected.size() > max_sources)
    return QueryResult<CatalogSelection>::failure(query_failure(
      QueryError::ResourceExhausted, QueryPhase::Catalog,
      "catalog selection exceeds the source limit"));

  return QueryResult<CatalogSelection>::success(CatalogSelection{
    std::move(selected), selector.file_predicate });
}

class InMemoryPublishedVersionCatalog final
  : public PublishedVersionCatalog {
public:
  explicit InMemoryPublishedVersionCatalog(
      std::vector<StructuredBlobManifest> manifests)
    : manifests_(std::move(manifests)) {}

  QueryResult<CatalogSelection> scan(const SourceSelector& selector,
      std::size_t max_sources) const override {
    return select_published_snapshot(manifests_, selector, max_sources);
  }

private:
  std::vector<StructuredBlobManifest> manifests_;
};

class RefreshingPublishedVersionCatalog final
  : public PublishedVersionCatalog {
public:
  explicit RefreshingPublishedVersionCatalog(
      PublishedVersionSnapshotLoader loader, std::uint64_t storage_namespace)
    : loader_(std::move(loader)), storage_namespace_(storage_namespace) {}

  QueryResult<CatalogSelection> scan(const SourceSelector& selector,
      std::size_t max_sources) const override {
    auto valid = validate_source_selector(selector);
    if (!valid.ok())
      return QueryResult<CatalogSelection>::failure(std::move(valid));
    if (max_sources == 0)
      return QueryResult<CatalogSelection>::failure(
        invalid_request("catalog source limit must be positive"));
    if (!loader_)
      return QueryResult<CatalogSelection>::failure(query_failure(
        QueryError::Internal, QueryPhase::Catalog,
        "refreshing catalog has no snapshot loader"));
    try {
      auto snapshot = loader_(selector);
      if (!snapshot.ok())
        return QueryResult<CatalogSelection>::failure(snapshot.status());
      auto selected = select_published_snapshot(snapshot.value().manifests, selector, max_sources);
      if (selected.ok()) {
        selected.value().storage_namespace = storage_namespace_;
        if (snapshot.value().read_context) {
          auto context = snapshot.value().read_context->select(selected.value().versions());
          if (!context.ok()) return QueryResult<CatalogSelection>::failure(context.status());
          selected.value().read_context = std::move(context.value());
        }
        const auto& evidence = snapshot.value().source_permissions;
        if (evidence && storage_namespace_ != 0 &&
            evidence->storage_namespace == storage_namespace_) {
          auto filtered = std::make_shared<SourcePermissionSnapshot>();
          filtered->storage_namespace = evidence->storage_namespace;
          filtered->metadata_generation = evidence->metadata_generation;
          filtered->observed_at = evidence->observed_at;
          for (const auto& manifest : selected.value().versions()) {
            const auto found = evidence->sources.find(manifest.key);
            if (found != evidence->sources.end()) {
              const auto& fact = found->second;
              if (fact.format_id == manifest.format_id.value_or("") &&
                  fact.manifest_revision == manifest.revision &&
                  fact.payload_size == manifest.payload_size &&
                  fact.layout_size == manifest.layout_size &&
                  !fact.is_dir && fact.mode >= 0 && fact.uid >= 0 && fact.gid >= 0)
                filtered->sources.emplace(manifest.key, fact);
            }
            for (auto parent = path_utils::parent_of(manifest.key.path);;
                 parent = path_utils::parent_of(parent)) {
              const auto observed = evidence->parents.find(parent);
              if (observed != evidence->parents.end()) {
                const auto& permission = observed->second;
                if (!permission || permission->version < 0 ||
                    permission->mode < 0 || permission->uid < 0 || permission->gid < 0)
                  filtered->parents.emplace(parent, std::nullopt);
                else
                  filtered->parents.emplace(parent, permission);
              }
              if (parent == "/") break;
            }
          }
          selected.value().source_permissions = std::move(filtered);
        }
      }
      return selected;
    } catch (const std::exception& error) {
      QueryErrorDetail detail;
      detail.operation = "load-catalog-snapshot";
      detail.retriable = true;
      return QueryResult<CatalogSelection>::failure(query_failure(
        QueryError::ReadFailure, QueryPhase::Catalog,
        std::string("catalog snapshot loader failed: ") + error.what(),
        std::move(detail)));
    } catch (...) {
      QueryErrorDetail detail;
      detail.operation = "load-catalog-snapshot";
      detail.retriable = true;
      return QueryResult<CatalogSelection>::failure(query_failure(
        QueryError::ReadFailure, QueryPhase::Catalog,
        "catalog snapshot loader failed with an unknown exception",
        std::move(detail)));
    }
  }

private:
  PublishedVersionSnapshotLoader loader_;
  const std::uint64_t storage_namespace_;
};

QueryStatus storage_failure(const StructuredBlobStatus& status,
    const StructuredBlobManifest* manifest, const char* operation,
    bool during_open) {
  QueryErrorDetail detail;
  detail.operation = operation;
  if (manifest) {
    detail.source = QuerySourceReference{manifest->key.path,
      manifest->key.version, manifest->format_id.value_or("")};
  }
  switch (status.code()) {
  case StructuredBlobError::InvalidArgument:
  case StructuredBlobError::RangeError:
    return query_failure(QueryError::InvalidQuery, QueryPhase::Storage,
      status.message(), std::move(detail));
  case StructuredBlobError::NotFound:
    return query_failure(during_open ? QueryError::SourceNotFound
                                    : QueryError::StalePlan,
      during_open ? QueryPhase::Catalog : QueryPhase::Storage,
      status.message(), std::move(detail));
  case StructuredBlobError::BackendFailure:
    detail.retriable = true;
    return query_failure(QueryError::ReadFailure, QueryPhase::Storage,
      status.message(), std::move(detail));
  case StructuredBlobError::AlreadyExists:
  case StructuredBlobError::InvalidState:
    return query_failure(QueryError::Internal, QueryPhase::Storage,
      status.message(), std::move(detail));
  case StructuredBlobError::None:
    break;
  }
  return query_failure(QueryError::Internal, QueryPhase::Storage,
    "storage operation failed without an error", std::move(detail));
}

class StructuredBlobQuerySource final : public QuerySource {
public:
  explicit StructuredBlobQuerySource(StructuredBlobReadHandle reader)
    : reader_(std::move(reader)) {}

  std::uint64_t storage_namespace() const override {
    return reader_.storage_namespace();
  }

  const StructuredBlobManifest& manifest() const override {
    return reader_.manifest();
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    auto result = reader_.open_native_file();
    if (!result.ok()) return QueryResult<std::shared_ptr<const NativeFileHandle>>::failure(
      storage_failure(result.status(), &reader_.manifest(), "open_native_file", false));
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::success(std::move(result.value()));
  }
  QueryResult<std::optional<std::string>> verified_layout_identity() const override {
    auto result=reader_.verified_layout_identity();
    if (!result.ok()) return QueryResult<std::optional<std::string>>::failure(
      storage_failure(result.status(), &reader_.manifest(), "verified_layout_identity", false));
    return QueryResult<std::optional<std::string>>::success(result.value());
  }

  QueryResult<std::optional<std::string>> native_file_cache_identity() const override {
    auto result = reader_.native_file_cache_identity();
    if (!result.ok()) return QueryResult<std::optional<std::string>>::failure(
      storage_failure(result.status(), &reader_.manifest(), "native_file_cache_identity", false));
    return QueryResult<std::optional<std::string>>::success(std::move(result.value()));
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>& handle) const override {
    auto result = reader_.reuse_native_file(handle);
    if (!result.ok()) return QueryResult<std::shared_ptr<const NativeFileHandle>>::failure(
      storage_failure(result.status(), &reader_.manifest(), "reuse_native_file", false));
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::success(std::move(result.value()));
  }

  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    const auto& manifest = reader_.manifest();
    if (offset > manifest.payload_size ||
        length > manifest.payload_size - offset)
      return QueryResult<StructuredBlobBytes>::failure(query_failure(
        QueryError::InvalidQuery, QueryPhase::Storage,
        "payload read is outside the planned File Version",
        source_detail("read_payload")));
    auto result = reader_.read_payload_range(offset, length);
    if (!result.ok())
      return QueryResult<StructuredBlobBytes>::failure(storage_failure(
        result.status(), &manifest, "read_payload", false));
    return QueryResult<StructuredBlobBytes>::success(std::move(result.value()));
  }

  PayloadReadCapabilities payload_read_capabilities() const override {
    return {reader_.supports_payload_ranges(),
      reader_.supports_payload_ranges() ? kMaxPayloadRangesPerBatch : 1,
      kMaxPayloadBytesPerBatch};
  }

  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    const auto& manifest = reader_.manifest();
    if (ranges.empty() || ranges.size() > kMaxPayloadRangesPerBatch)
      return QueryResult<StructuredBlobPayloadBatch>::failure(query_failure(
        QueryError::InvalidQuery, QueryPhase::Storage,
        "payload batch range count is outside the supported bounds",
        source_detail("read_payload_ranges")));
    std::uint64_t total = 0;
    for (const auto& range : ranges) {
      if (range.length == 0 || range.offset > manifest.payload_size ||
          range.length > manifest.payload_size - range.offset ||
          range.length > kMaxPayloadBytesPerBatch - total) {
        return QueryResult<StructuredBlobPayloadBatch>::failure(query_failure(
          QueryError::InvalidQuery, QueryPhase::Storage,
          "payload batch range is outside the planned File Version or batch budget",
          source_detail("read_payload_ranges")));
      }
      total += range.length;
    }
    auto result = reader_.read_payload_ranges(ranges);
    if (!result.ok())
      return QueryResult<StructuredBlobPayloadBatch>::failure(storage_failure(
        result.status(), &manifest, "read_payload_ranges", false));
    return QueryResult<StructuredBlobPayloadBatch>::success(
      std::move(result.value()));
  }

  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset,
      std::uint64_t length) const override {
    const auto& manifest = reader_.manifest();
    if (offset > manifest.layout_size ||
        length > manifest.layout_size - offset)
      return QueryResult<StructuredBlobBytes>::failure(query_failure(
        QueryError::InvalidQuery, QueryPhase::Storage,
        "layout read is outside the planned File Version",
        source_detail("read_layout")));
    auto result = reader_.read_layout_range(offset, length);
    if (!result.ok())
      return QueryResult<StructuredBlobBytes>::failure(storage_failure(
        result.status(), &manifest, "read_layout", false));
    return QueryResult<StructuredBlobBytes>::success(std::move(result.value()));
  }

private:
  QueryErrorDetail source_detail(const char* operation) const {
    const auto& manifest = reader_.manifest();
    QueryErrorDetail detail;
    detail.source = QuerySourceReference{manifest.key.path,
      manifest.key.version, manifest.format_id.value_or("")};
    detail.operation = operation;
    return detail;
  }

  StructuredBlobReadHandle reader_;
};

} // namespace

struct PathScopeSet::Compiled {
  std::vector<PathScope> scopes;
  std::vector<PathScope> normalized;
  // Sorted indices avoid copying path strings for exact membership and keys.
  std::vector<std::size_t> order;
  std::size_t exact_count{0};
  QueryStatus status{QueryStatus::success()};
  std::string key;

  explicit Compiled(std::vector<PathScope> input) : scopes(std::move(input)) {
    normalized.reserve(scopes.size());
    order.reserve(scopes.size());
    std::set<std::pair<PathScopeKind, std::string>> unique;
    for (const auto& scope : scopes) {
      if (scope.kind != PathScopeKind::Exact && scope.kind != PathScopeKind::Subtree) {
        status = invalid_request("unknown path scope kind"); return;
      }
      if (scope.path.empty()) {
        status = invalid_request("path scope cannot be empty"); return;
      }
      if (scope.path.find('\0') != std::string::npos) {
        status = invalid_request("path scope cannot contain a NUL byte"); return;
      }
      auto path = path_utils::norm(scope.path);
      if (!unique.emplace(scope.kind, path).second) {
        status = invalid_request("path scopes must be unique"); return;
      }
      order.push_back(normalized.size());
      normalized.push_back({scope.kind, std::move(path)});
      if (scope.kind == PathScopeKind::Exact) ++exact_count;
    }
    std::sort(order.begin(), order.end(), [this](auto a, auto b) {
      return std::tie(normalized[a].kind, normalized[a].path) <
        std::tie(normalized[b].kind, normalized[b].path);
    });
    key = "/paths:" + std::to_string(order.size());
    for (const auto index : order) {
      const auto& scope = normalized[index];
      key += ":" + std::to_string(static_cast<int>(scope.kind)) +
        std::to_string(scope.path.size()) + ":" + scope.path;
    }
  }
};

PathScopeSet::PathScopeSet() {
  static const auto empty = std::make_shared<const Compiled>(std::vector<PathScope>{});
  compiled_ = empty;
}

PathScopeSet::PathScopeSet(std::vector<PathScope> scopes)
  : compiled_(std::make_shared<const Compiled>(std::move(scopes))) {}

PathScopeSet::PathScopeSet(std::initializer_list<PathScope> scopes)
  : PathScopeSet(std::vector<PathScope>(scopes)) {}

const std::vector<PathScope>& PathScopeSet::scopes() const { return compiled_->scopes; }
const std::vector<PathScope>& PathScopeSet::normalized_paths() const { return compiled_->normalized; }
const std::string& PathScopeSet::identity_key() const { return compiled_->key; }
const QueryStatus& PathScopeSet::validation_status() const { return compiled_->status; }

bool PathScopeSet::matches_normalized_path(const std::string& path) const {
  if (empty()) return true;
  const auto& data = *compiled_;
  if (!data.status.ok()) return false;
  const auto exact_end = data.order.begin() + data.exact_count;
  const auto exact = std::lower_bound(data.order.begin(), exact_end, path,
    [&data](auto index, const std::string& wanted) {
      return data.normalized[index].path < wanted;
    });
  if (exact != exact_end && data.normalized[*exact].path == path) return true;
  for (auto it = exact_end; it != data.order.end(); ++it) {
    const auto& root = data.normalized[*it].path;
    if (root == "/" || path == root ||
        (path.size() > root.size() && path.compare(0, root.size(), root) == 0 &&
         path[root.size()] == '/')) return true;
  }
  return false;
}

QueryStatus validate_source_selector(const SourceSelector& selector) {
  return validate_source_selector_impl(selector);
}

QueryResult<StructuredBlobPayloadBatch> QuerySource::read_payload_ranges(
    const StructuredBlobPayloadRanges&) const {
  return QueryResult<StructuredBlobPayloadBatch>::failure(
    QueryStatus::failure(QueryError::UnsupportedCapability,
      QueryPhase::Storage,
      "query source does not support batched payload reads"));
}

VersionSelection VersionSelection::latest() {
  return { VersionSelectionKind::Latest, std::nullopt, std::nullopt,
    std::nullopt };
}

VersionSelection VersionSelection::exact_version(std::int64_t version) {
  return { VersionSelectionKind::Exact, version, std::nullopt, std::nullopt };
}

VersionSelection VersionSelection::all() {
  return { VersionSelectionKind::All, std::nullopt, std::nullopt,
    std::nullopt };
}

VersionSelection VersionSelection::between(std::int64_t lower_inclusive,
    std::int64_t upper_exclusive) {
  return { VersionSelectionKind::Between, std::nullopt, lower_inclusive,
    upper_exclusive };
}

std::shared_ptr<const PublishedVersionCatalog>
make_in_memory_published_version_catalog(
    std::vector<StructuredBlobManifest> manifests) {
  return std::make_shared<const InMemoryPublishedVersionCatalog>(
    std::move(manifests));
}

std::shared_ptr<const PublishedVersionCatalog>
make_refreshing_published_version_catalog(
    PublishedVersionSnapshotLoader loader, std::uint64_t storage_namespace) {
  return std::make_shared<const RefreshingPublishedVersionCatalog>(
    std::move(loader), storage_namespace);
}

QueryResult<std::shared_ptr<const QuerySource>>
open_structured_blob_query_source(
    std::shared_ptr<const StructuredBlobStorage> storage,
    const StructuredBlobVersionKey& key) {
  if (!storage)
    return QueryResult<std::shared_ptr<const QuerySource>>::failure(
      invalid_request("query source storage is required"));
  auto reader = storage->open_published_version(key);
  if (!reader.ok())
    return QueryResult<std::shared_ptr<const QuerySource>>::failure(
      storage_failure(reader.status(), nullptr, "open_source", true));
  std::shared_ptr<const QuerySource> source =
    std::make_shared<const StructuredBlobQuerySource>(
      std::move(reader.value()));
  return QueryResult<std::shared_ptr<const QuerySource>>::success(
    std::move(source));
}

QueryResult<std::shared_ptr<const QuerySource>>
open_catalog_query_source(std::shared_ptr<const StructuredBlobStorage> storage,
    const StructuredBlobManifest& manifest, std::uint64_t catalog_namespace) {
  using Result = QueryResult<std::shared_ptr<const QuerySource>>;
  if (!storage) return Result::failure(invalid_request("query source storage is required"));
  auto reader = storage->open_catalog_version(manifest.key, manifest, catalog_namespace);
  if (!reader.ok()) return Result::failure(
    storage_failure(reader.status(), &manifest, "open_catalog_source", true));
  return Result::success(std::make_shared<const StructuredBlobQuerySource>(
    std::move(reader.value())));
}

bool query_fragment_requires_exact_source(const QueryFragment& fragment) {
  return fragment.physical_array.has_value() || has_hdf5_query_fields(fragment);
}

QueryStatus validate_query_request(const QueryRequest& request) {
  auto sources = validate_source_selector(request.sources);
  if (!sources.ok()) return sources;

  const auto& limits = request.limits;
  if (limits.max_sources == 0 || limits.max_ranges == 0 ||
      limits.max_layout_bytes == 0 || limits.max_payload_bytes == 0 ||
      limits.max_result_rows == 0 || limits.target_batch_rows == 0 ||
      limits.target_batch_bytes == 0)
    return invalid_request("query resource limits must be positive");

  if (!request.subset) return QueryStatus::success();
  // This is a request-model seam only. Dataset binding, layout and execution
  // stay with the format adapter; malformed format fields cannot bypass the
  // exact-source contract by reaching a catalog with no matching sources.
  auto hdf5 = validate_hdf5_query_request(request);
  if (!hdf5.ok()) return hdf5;
  if (request.subset->physical_array) {
    const auto& physical = *request.subset->physical_array;
    if (physical.values.size() > limits.max_ranges) {
      QueryErrorDetail detail;
      detail.field_id = physical.field_id;
      return query_failure(QueryError::ResourceExhausted,
        QueryPhase::Validation,
        "physical array paths exceed the query range limit",
        std::move(detail));
    }
    auto subject = validate_physical_array_subject(
      physical);
    if (!subject.ok()) return subject;
    if (request.sources.paths.size() != 1 ||
        request.sources.paths.front().kind != PathScopeKind::Exact ||
        request.sources.versions.kind != VersionSelectionKind::Exact)
      return invalid_request(
        "physical array subject requires one exact path and version");
    if (request.subset->output.mode !=
          QueryFragment::ResultMode::LogicalValues ||
        request.subset->output.interpolation)
      return invalid_request(
        "physical array subject requires logical values without interpolation");
  }
  if (request.subset->projection.empty())
    return invalid_request(
      "semantic subset query requires an explicit projection");

  std::set<std::string> projected;
  for (const auto& field : request.subset->projection) {
    if (field.field_id.empty())
      return invalid_request("projection field cannot be empty");
    if (!projected.insert(field.field_id).second)
      return invalid_request("projection fields must be unique",
        field.field_id);
  }
  std::set<std::string> sliced;
  for (const auto& slice : request.subset->slices) {
    if (slice.dimension.field_id.empty())
      return invalid_request("domain slice dimension cannot be empty");
    if (!sliced.insert(slice.dimension.field_id).second)
      return invalid_request("domain slice dimensions must be unique",
        slice.dimension.field_id);
    if (!slice.lower && !slice.upper)
      return invalid_request("domain slice requires at least one bound",
        slice.dimension.field_id);
    if ((slice.lower && slice.lower->value.is_null()) ||
        (slice.upper && slice.upper->value.is_null()))
      return invalid_request("domain slice bounds cannot be null",
        slice.dimension.field_id);
    if (slice.lower && slice.upper &&
        slice.lower->value.type() != slice.upper->value.type())
      return invalid_request("domain slice bounds must have the same type",
        slice.dimension.field_id);
  }

  const auto& output = request.subset->output;
  std::set<std::string> overridden;
  for (const auto& type_override : output.type_overrides) {
    if (type_override.field.field_id.empty())
      return invalid_request("output type override field cannot be empty");
    if (!overridden.insert(type_override.field.field_id).second)
      return invalid_request("output type override fields must be unique",
        type_override.field.field_id);
    if (projected.count(type_override.field.field_id) == 0)
      return invalid_request(
        "output type override field must be projected",
        type_override.field.field_id);
  }
  if (output.mode == QueryFragment::ResultMode::BinaryView &&
      (request.subset->projection.size() != 1 ||
       !output.type_overrides.empty() || output.interpolation ||
       !request.subset->slices.empty()))
    return invalid_request(
      "binary-view results require one projection and cannot use type "
      "overrides, interpolation, or domain slices");
  if (!output.interpolation) return QueryStatus::success();

  const auto& interpolation = *output.interpolation;
  if (interpolation.axes.size() != 2)
    return invalid_request(
      "bilinear interpolation requires exactly two named dimensions");
  std::set<std::string> axes;
  bool uses_counts = false;
  bool uses_spacing = false;
  for (const auto& axis : interpolation.axes) {
    if (axis.dimension.field_id.empty() ||
        !axes.insert(axis.dimension.field_id).second)
      return invalid_request(
        "interpolation dimensions must be non-empty and unique",
        axis.dimension.field_id);
    if (axis.count && *axis.count == 0)
      return invalid_request("interpolation axis count must be positive",
        axis.dimension.field_id);
    if (axis.spacing && !positive_finite_spacing(*axis.spacing))
      return invalid_request(
        "interpolation spacing must be a positive finite floating value",
        axis.dimension.field_id);
    if (axis.count && axis.spacing)
      return invalid_request(
        "interpolation axis cannot specify both count and spacing",
        axis.dimension.field_id);
    uses_counts = uses_counts || axis.count.has_value();
    uses_spacing = uses_spacing || axis.spacing.has_value();
  }
  if (uses_counts && uses_spacing)
    return invalid_request(
      "interpolation axes must use one sizing mode");
  if (uses_counts && std::any_of(interpolation.axes.begin(),
        interpolation.axes.end(), [](const auto& axis) {
          return !axis.count.has_value();
        }))
    return invalid_request(
      "count-based interpolation requires a count for every axis");
  if (uses_spacing && std::any_of(interpolation.axes.begin(),
        interpolation.axes.end(), [](const auto& axis) {
          return !axis.spacing.has_value();
        }))
    return invalid_request(
      "spacing-based interpolation requires spacing for every axis");
  return QueryStatus::success();
}

QueryStatus validate_query_capabilities(
    const QueryCapabilities& capabilities) {
  if (capabilities.interface_version() == 0 ||
      capabilities.adapter_id().empty() ||
      capabilities.adapter_version().empty())
    return query_failure(QueryError::Internal, QueryPhase::Planning,
      "query adapter capability identity is incomplete");

  std::set<std::string> fields;
  for (const auto& capability : capabilities.fields()) {
    const auto* schema_field = capabilities.schema().find(
      capability.field.field_id);
    if (capability.field.field_id.empty() ||
        !fields.insert(capability.field.field_id).second ||
        !schema_field)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter capability references an invalid field");
    std::set<ComparisonOperator> operators;
    for (const auto operation : capability.predicate_operators) {
      if (!operators.insert(operation).second)
        return query_failure(QueryError::Internal, QueryPhase::Planning,
          "query adapter capability repeats a predicate operator");
    }
    // Operators describe supported semantics; None means core residual
    // evaluation, not that the operator is unsupported.
    if (capability.predicate_operators.empty() &&
        capability.predicate_pushdown != PushdownLevel::None)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter predicate capability is inconsistent");
    if (capability.sliceable !=
        (capability.slice_pushdown != PushdownLevel::None))
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter slice capability is inconsistent");
    if (capability.sliceable && schema_field->role != FieldRole::Dimension)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter slice capability requires a dimension");
    std::set<LogicalTypeId> output_types;
    for (const auto type : capability.output_types) {
      if (!output_types.insert(type).second)
        return query_failure(QueryError::Internal, QueryPhase::Planning,
          "query adapter repeats an output type capability");
    }
    if (!capability.output_types.empty() && !capability.projectable)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "output type capability requires a projectable field");
    if (capability.null_predicate && !schema_field->type.nullable)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "null predicate capability requires a nullable field");
    std::set<QueryFragment::ResultMode> field_modes;
    for (const auto mode : capability.result_modes) {
      if (!field_modes.insert(mode).second ||
          std::find(capabilities.result_modes().begin(),
            capabilities.result_modes().end(), mode) ==
            capabilities.result_modes().end())
        return query_failure(QueryError::Internal, QueryPhase::Planning,
          "field result modes must be unique adapter result modes");
    }
    if (capability.interpolatable &&
        (!capability.sliceable ||
         schema_field->role != FieldRole::Dimension))
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "interpolation capability requires a sliceable dimension");
  }

  std::set<DomainKind> domains;
  for (const auto domain : capabilities.domains()) {
    if (!domains.insert(domain).second)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter capability repeats a domain kind");
  }
  if (capabilities.ordering() == OrderingGuarantee::Domain &&
      capabilities.domains().empty())
    return query_failure(QueryError::Internal, QueryPhase::Planning,
      "domain ordering requires a declared domain");
  std::set<QueryFragment::ResultMode> result_modes;
  for (const auto mode : capabilities.result_modes()) {
    if (!result_modes.insert(mode).second)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter repeats a result mode");
  }
  if (result_modes.empty())
    return query_failure(QueryError::Internal, QueryPhase::Planning,
      "query adapter must declare at least one result mode");
  if (result_modes.count(QueryFragment::ResultMode::BinaryView) != 0 &&
      std::none_of(capabilities.fields().begin(),
        capabilities.fields().end(), [&](const auto& capability) {
          const auto* field = capabilities.schema().find(
            capability.field.field_id);
          return capability.projectable && field &&
            field->type.id == LogicalTypeId::Binary;
        }))
    return query_failure(QueryError::Internal, QueryPhase::Planning,
      "binary-view result mode requires a projectable Binary field");
  std::set<QueryFragment::InterpolationMethod> interpolation_methods;
  for (const auto method : capabilities.interpolation_methods()) {
    if (!interpolation_methods.insert(method).second)
      return query_failure(QueryError::Internal, QueryPhase::Planning,
        "query adapter repeats an interpolation method");
  }
  const auto interpolatable_dimensions = static_cast<std::size_t>(
    std::count_if(capabilities.fields().begin(),
      capabilities.fields().end(), [](const auto& capability) {
        return capability.interpolatable;
      }));
  if ((!interpolation_methods.empty() && interpolatable_dimensions < 2) ||
      (interpolation_methods.empty() && interpolatable_dimensions != 0))
    return query_failure(QueryError::Internal, QueryPhase::Planning,
      "interpolation methods and dimensions are inconsistent");
  return QueryStatus::success();
}

QueryResult<PreparedQuery> PreparedQuery::create(QueryRequest request,
    std::optional<LogicalSchema> result_schema,
    std::vector<PreparedSourceQuery> sources) {
  auto valid = validate_query_request(request);
  if (!valid.ok())
    return QueryResult<PreparedQuery>::failure(std::move(valid));
  if (sources.size() > request.limits.max_sources)
    return QueryResult<PreparedQuery>::failure(query_failure(
      QueryError::ResourceExhausted, QueryPhase::Planning,
      "prepared query exceeds the source limit"));
  if (request.subset && !sources.empty() && !result_schema)
    return QueryResult<PreparedQuery>::failure(query_failure(
      QueryError::SchemaMismatch, QueryPhase::Planning,
      "semantic prepared query requires a result schema"));
  std::set<std::pair<std::string, std::int64_t>> keys;
  for (const auto& prepared : sources) {
    if (!prepared.source)
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::Internal, QueryPhase::Planning,
        "prepared query contains a null QuerySource"));
    const auto& manifest = prepared.source->manifest();
    if (!manifest.published ||
        manifest.phase != StructuredBlobPhase::Published)
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::SourceNotPublished, QueryPhase::Planning,
        "prepared query source is not published"));
    if (!keys.emplace(manifest.key.path, manifest.key.version).second)
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::Internal, QueryPhase::Planning,
        "prepared query contains a duplicate File Version"));

    if (!request.subset) {
      if (prepared.adapter || prepared.plan)
        return QueryResult<PreparedQuery>::failure(query_failure(
          QueryError::Internal, QueryPhase::Planning,
          "file selection must not contain an adapter plan"));
      continue;
    }
    if (!prepared.adapter || !prepared.plan)
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::AdapterUnavailable, QueryPhase::Planning,
        "semantic query source requires an adapter plan"));
    if (!manifest.format_id || *manifest.format_id != prepared.adapter->id())
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::AdapterUnavailable, QueryPhase::Planning,
        "prepared adapter does not match persisted Format Identity"));
    if (!prepared.plan->implementation())
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::Internal, QueryPhase::Planning,
        "prepared adapter plan has no implementation"));
    if (prepared.plan->final_projection().empty())
      return QueryResult<PreparedQuery>::failure(query_failure(
        QueryError::SchemaMismatch, QueryPhase::Planning,
        "prepared adapter plan has no final projection"));
    const auto& adapter_schema = prepared.plan->adapter_output_schema();
    if (prepared.plan->residual_predicate()) {
      auto status = validate_expression(
        prepared.plan->residual_predicate(), adapter_schema);
      if (!status.ok())
        return QueryResult<PreparedQuery>::failure(std::move(status));
    }
    for (const auto& slice : prepared.plan->residual_slices()) {
      auto status = validate_domain_slice(slice, adapter_schema);
      if (!status.ok())
        return QueryResult<PreparedQuery>::failure(std::move(status));
    }
    std::set<std::string> projection;
    for (const auto& field : prepared.plan->final_projection()) {
      if (!adapter_schema.find(field.field_id) ||
          !projection.insert(field.field_id).second)
        return QueryResult<PreparedQuery>::failure(query_failure(
          QueryError::SchemaMismatch, QueryPhase::Planning,
          "prepared final projection is not present exactly once in the "
          "adapter output schema"));
    }
  }

  return QueryResult<PreparedQuery>::success(PreparedQuery(
    std::move(request), std::move(result_schema), std::move(sources)));
}

} // namespace iotdbfs
