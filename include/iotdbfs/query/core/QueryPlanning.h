#pragma once
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/core/QueryDomainCache.h"

#include "iotdbfs/formats/arrow/ArrowQuerySubject.h"
#include "iotdbfs/query/core/QueryModel.h"
#include "iotdbfs/StructuredBlobStorage.h"

#include <cstddef>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {

enum class PathScopeKind { Exact, Subtree };

struct PathScope {
  PathScopeKind kind{ PathScopeKind::Exact };
  std::string path;
};

// Owns the request's path scopes and their compiled, immutable form. Copies
// share storage; replacing the entire set compiles one new identity. Exposing
// only const elements prevents a stale validation result or cache identity.
class PathScopeSet {
public:
  using const_iterator = std::vector<PathScope>::const_iterator;

  PathScopeSet();
  PathScopeSet(std::vector<PathScope> scopes);
  PathScopeSet(std::initializer_list<PathScope> scopes);

  const std::vector<PathScope>& scopes() const;
  const std::vector<PathScope>& normalized_paths() const;
  const std::string& identity_key() const;
  const QueryStatus& validation_status() const;
  bool matches_normalized_path(const std::string& path) const;

  bool empty() const { return scopes().empty(); }
  std::size_t size() const { return scopes().size(); }
  const PathScope& front() const { return scopes().front(); }
  const PathScope& operator[](std::size_t index) const { return scopes()[index]; }
  const_iterator begin() const { return scopes().begin(); }
  const_iterator end() const { return scopes().end(); }

private:
  struct Compiled;
  std::shared_ptr<const Compiled> compiled_;
};

enum class VersionSelectionKind { Latest, Exact, All, Between };

struct VersionSelection {
  VersionSelectionKind kind{ VersionSelectionKind::Latest };
  std::optional<std::int64_t> exact;
  std::optional<std::int64_t> lower_inclusive;
  std::optional<std::int64_t> upper_exclusive;

  static VersionSelection latest();
  static VersionSelection exact_version(std::int64_t version);
  static VersionSelection all();
  static VersionSelection between(std::int64_t lower_inclusive,
    std::int64_t upper_exclusive);
};

enum class AdapterSelectionKind { All, Listed };

struct FormatSelector {
  // Listed with an empty adapter_ids set selects no recognized formats and can
  // therefore express byte-pass-through-only selection without a fake id.
  AdapterSelectionKind adapters{ AdapterSelectionKind::All };
  std::vector<std::string> adapter_ids;
  bool include_byte_passthrough{ true };
};

struct SourceSelector {
  // Empty path scopes select every visible Logical File available to the
  // catalog. Non-empty scopes are ORed.
  PathScopeSet paths;
  VersionSelection versions;
  FormatSelector formats;

  // This expression uses the catalog's manifest/Sigma Logical Schema. A
  // catalog may push down all or part of it and must return any residual.
  QueryExpressionPtr file_predicate;
};

// Shared by catalogs and storage-side conservative selector pushdown. Validate
// before issuing SQL or invoking a snapshot loader.
QueryStatus validate_source_selector(const SourceSelector& selector);

// Request-local inode evidence, independent of immutable content manifests.
// Permissions can change without changing a File Version's blob revision.
struct CatalogSourcePermission {
  std::string format_id;
  std::uint64_t manifest_revision{0}, payload_size{0}, layout_size{0};
  int mode{0}, uid{0}, gid{0};
  bool is_dir{false};
};

struct CatalogParentPermission {
  std::int64_t version{-1};
  int mode{0}, uid{0}, gid{0};
  bool is_dir{false};
};

struct SourcePermissionSnapshot {
  std::uint64_t storage_namespace{0}, metadata_generation{0};
  std::chrono::steady_clock::time_point observed_at{};
  std::map<StructuredBlobVersionKey, CatalogSourcePermission> sources;
  // A present nullopt records a missing or unusable newest published inode.
  // Absence means unobserved and allows a fresh bulk observation instead.
  std::map<std::string, std::optional<CatalogParentPermission>> parents;
};

struct PublishedVersionSnapshot {
  std::vector<StructuredBlobManifest> manifests;
  std::shared_ptr<const SourcePermissionSnapshot> source_permissions;
  std::shared_ptr<const QueryReadContext> read_context;
};

struct CatalogSelection {
  // Every copy pins the same frozen candidate array. Replacing a selection's
  // array never changes another selection or an in-flight query's candidates.
  explicit CatalogSelection(std::vector<StructuredBlobManifest> versions = {},
      QueryExpressionPtr residual = nullptr, std::uint64_t backend_namespace = 0,
      std::shared_ptr<const SourcePermissionSnapshot> permissions = {},
      std::shared_ptr<const QueryReadContext> context = {},
      std::shared_ptr<QueryDomainCache> state = {})
    : residual_file_predicate(std::move(residual)), storage_namespace(backend_namespace),
      source_permissions(std::move(permissions)), read_context(std::move(context)),
      hot_state(std::move(state)) { set_versions(std::move(versions)); }

  const std::vector<StructuredBlobManifest>& versions() const {
    static const std::vector<StructuredBlobManifest> empty;
    return versions_ ? *versions_ : empty;
  }
  void set_versions(std::vector<StructuredBlobManifest> versions) {
    versions_ = versions.empty() ? nullptr :
      std::make_shared<const std::vector<StructuredBlobManifest>>(std::move(versions));
  }
  QueryExpressionPtr residual_file_predicate;
  // Nonzero only when the catalog read manifests from this backend instance.
  std::uint64_t storage_namespace{0};
  // Trusted in-process evidence only: never a persisted authorization result.
  std::shared_ptr<const SourcePermissionSnapshot> source_permissions;
  std::shared_ptr<const QueryReadContext> read_context;
  // Optional query-independent artifacts in this full frozen candidate domain.
  // This is not an authorization grant; frontends still authorize all sources.
  std::shared_ptr<QueryDomainCache> hot_state;
private:
  std::shared_ptr<const std::vector<StructuredBlobManifest>> versions_;
};

// Enumerates immutable, published File Versions. Implementations may use
// physical indexes, but callers never construct backend SQL or observe
// unpublished lifecycle state.
class PublishedVersionCatalog {
public:
  virtual ~PublishedVersionCatalog() = default;

  virtual QueryResult<CatalogSelection> scan(
    const SourceSelector& selector, std::size_t max_sources) const = 0;
};

// Deterministic Adapter useful for tests and embedded callers. File predicates
// are deliberately returned as residual expressions; path, version, format,
// publication, and source limits are enforced exactly.
std::shared_ptr<const PublishedVersionCatalog>
make_in_memory_published_version_catalog(
  std::vector<StructuredBlobManifest> manifests);

// A refreshing Catalog obtains a new immutable snapshot for every scan. This
// keeps prepare(latest) current without weakening Prepared Query snapshot
// semantics or flushing version-keyed format caches. The loader may push down
// selector filters conservatively; final selection and source limits always
// remain enforced by the catalog.
using PublishedVersionSnapshotLoader =
  std::function<QueryResult<PublishedVersionSnapshot>(
    const SourceSelector&)>;

std::shared_ptr<const PublishedVersionCatalog>
make_refreshing_published_version_catalog(
  PublishedVersionSnapshotLoader loader, std::uint64_t storage_namespace = 0);

// Physical transport bounds, independent of any query/request budget. Const
// source reads are thread-safe; callers choose their own bounded concurrency.
struct PayloadReadCapabilities {
  bool multi_range{false};
  std::size_t max_ranges{1};
  std::uint64_t max_bytes{kMaxPayloadBytesPerBatch};
};

// Read-only view of one fixed, published File Version. FormatQueryAdapter
// implementations receive this Interface instead of StructuredBlobStorage or
// an IoTDB connector. Range reads may be issued concurrently during execute;
// implementations must therefore make const reads thread-safe.
class QuerySource {
public:
  QuerySource() : storage_namespace_(detail::allocate_storage_namespace()) {}
  // A copied standalone source is a distinct storage namespace. Backend-backed
  // implementations override this method with the retained backend identity.
  QuerySource(const QuerySource&) : QuerySource() {}
  QuerySource& operator=(const QuerySource&) = delete;
  virtual ~QuerySource() = default;

  virtual std::uint64_t storage_namespace() const { return storage_namespace_; }

  virtual const StructuredBlobManifest& manifest() const = 0;
  // Format-neutral optional capability. A native file is an explicitly bound
  // immutable version, not a path supplied by a query or a downloaded payload.
  virtual QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const {
    return QueryResult<std::shared_ptr<const NativeFileHandle>>::failure(QueryStatus::failure(
      QueryError::UnsupportedCapability, QueryPhase::Storage,
      "this source has no exact-version filesystem binding"));
  }
  // Full source/head/location identity, not permission evidence. A new request
  // must rebind a cached descriptor through its own source before using it.
  virtual QueryResult<std::optional<std::string>> native_file_cache_identity() const {
    return QueryResult<std::optional<std::string>>::success(std::nullopt);
  }
  virtual QueryResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>&) const {
    return open_native_file();
  }
  virtual QueryResult<std::optional<std::string>> verified_layout_identity() const {
    return QueryResult<std::optional<std::string>>::success(std::nullopt);
  }
  // Shared fragment producers read through an unbudgeted source. Before
  // acquiring a fragment (including hits), reserve its logical demand through
  // the request source instead. Do not also read through this request source:
  // read_payload already charges that transport demand. Standalone sources
  // have no aggregate request budget; adapters still enforce execution limits.
  virtual QueryStatus reserve_logical_payload(std::uint64_t length) const {
    return QueryStatus::success();
  }
  virtual QueryResult<StructuredBlobBytes> read_payload(
    std::uint64_t offset, std::uint64_t length) const = 0;
  virtual PayloadReadCapabilities payload_read_capabilities() const { return {}; }
  // An optional true multi-range transport for bounded reads.
  // Sources without this capability fail explicitly; a caller may instead
  // plan bounded scalar reads based on payload_read_capabilities().
  virtual QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
    const StructuredBlobPayloadRanges& ranges) const;
  virtual QueryResult<StructuredBlobBytes> read_layout(
    std::uint64_t offset, std::uint64_t length) const = 0;

private:
  const std::uint64_t storage_namespace_;
};

// Execution owns both capabilities. Request-local reads use source; shared
// producers retain shared_source and apply QueryRuntime service budgets without
// depending on any waiter's budget or lifetime. Both name the same version.
struct QueryExecutionContext {
  explicit QueryExecutionContext(std::shared_ptr<const QuerySource> owned_source)
    : source(owned_source), shared_source(std::move(owned_source)) {}
  QueryExecutionContext(std::shared_ptr<const QuerySource> request_source,
      std::shared_ptr<const QuerySource> producer_source)
    : source(std::move(request_source)),
      shared_source(std::move(producer_source)) {}

  const std::shared_ptr<const QuerySource> source;
  const std::shared_ptr<const QuerySource> shared_source;
};

// Opens a QuerySource only when the key currently names a published version.
// The returned source retains that exact immutable Manifest as its read
// capability; bounded payload/layout reads do not re-resolve publication.
QueryResult<std::shared_ptr<const QuerySource>>
open_structured_blob_query_source(
  std::shared_ptr<const StructuredBlobStorage> storage,
  const StructuredBlobVersionKey& key);
QueryResult<std::shared_ptr<const QuerySource>>
open_catalog_query_source(std::shared_ptr<const StructuredBlobStorage> storage,
  const StructuredBlobManifest& manifest, std::uint64_t catalog_namespace);

struct QueryFragment {
  // Projection is mandatory for semantic subset queries. The adapter may add
  // hidden fields needed by residual operators, but the final result uses this
  // projection.
  std::vector<FieldRef> projection;
  QueryExpressionPtr predicate;
  std::vector<DomainSlice> slices;

  enum class ResultMode { LogicalValues, BinaryView };
  enum class InterpolationMethod { Bilinear };
  enum class BorderMode { Clamp, NaN };

  struct FieldTypeOverride {
    FieldRef field;
    LogicalTypeId type{ LogicalTypeId::Float64 };
  };

  struct InterpolationAxis {
    FieldRef dimension;
    std::optional<std::uint64_t> count;
    std::optional<LogicalValue> spacing;
  };

  struct GridInterpolation {
    InterpolationMethod method{ InterpolationMethod::Bilinear };
    std::vector<InterpolationAxis> axes;
    BorderMode border{ BorderMode::Clamp };
  };

  // BinaryView is a typed Binary result selected by an adapter, not an escape
  // hatch around QueryBatch. Type overrides name projected fields explicitly.
  struct Output {
    ResultMode mode{ ResultMode::LogicalValues };
    std::vector<FieldTypeOverride> type_overrides;
    std::optional<GridInterpolation> interpolation;
  } output;

  // An Arrow physical nested-array address, resolved before capabilities. It has no
  // variable, time, or dataset semantics; adapters may expose positional
  // scalar fields after validating the native shape.
  std::optional<PhysicalArraySubject> physical_array;
};

struct QueryLimits {
  std::size_t max_sources{ 1024 };
  std::size_t max_ranges{ 65536 };
  std::uint64_t max_layout_bytes{ 64ULL * 1024ULL * 1024ULL };
  std::uint64_t max_payload_bytes{ 1024ULL * 1024ULL * 1024ULL };
  std::uint64_t max_result_rows{ 100000000ULL };
  std::size_t target_batch_rows{ 65536 };
  std::uint64_t target_batch_bytes{ 8ULL * 1024ULL * 1024ULL };
};

// Immutable adapter-owned data retained only for one exact-source planning
// operation. State and resulting plans must not retain the context/source.
class AdapterPlanningState {
public:
  virtual ~AdapterPlanningState() = default;
};

// A sequential capabilities/prepare session for one exact source. The caller
// keeps source alive until both calls finish. Contexts are never shared across
// requests or sources; there is no path/version-only global cache identity.
class QueryPlanningContext {
public:
  explicit QueryPlanningContext(const QuerySource& source,
      QueryLimits limits = {}) : source_(source), limits_(std::move(limits)) {}

  QueryPlanningContext(const QueryPlanningContext&) = delete;
  QueryPlanningContext& operator=(const QueryPlanningContext&) = delete;
  QueryPlanningContext(QueryPlanningContext&&) = delete;
  QueryPlanningContext& operator=(QueryPlanningContext&&) = delete;

  const QuerySource& source() const { return source_; }
  const QueryLimits& limits() const { return limits_; }
  const std::shared_ptr<const AdapterPlanningState>& state() const {
    return state_;
  }
  void set_state(std::shared_ptr<const AdapterPlanningState> state) const {
    state_ = std::move(state);
  }

private:
  const QuerySource& source_;
  const QueryLimits limits_;
  mutable std::shared_ptr<const AdapterPlanningState> state_;
};

struct QueryRequest {
  SourceSelector sources;

  // Absent means a file/version selection request. Present means a semantic
  // in-file query and therefore requires a FormatQueryAdapter.
  std::optional<QueryFragment> subset;
  QueryLimits limits;
};

// Validates structure and resource limits only. Field/type validation occurs
// later against catalog and format-specific Logical Schemas.
QueryStatus validate_query_request(const QueryRequest& request);

// Physical format request contracts that bind one exact source need an
// existence probe when catalog filtering produces no candidates. Field
// syntax stays in each format's model; callers must validate the request first.
bool query_fragment_requires_exact_source(const QueryFragment& fragment);

enum class PushdownLevel { None, Candidate, Exact };
enum class OrderingGuarantee { None, Source, Domain };

struct FieldQueryCapability {
  FieldRef field;
  bool projectable{ false };
  std::vector<ComparisonOperator> predicate_operators;
  PushdownLevel predicate_pushdown{ PushdownLevel::None };
  bool sliceable{ false };
  PushdownLevel slice_pushdown{ PushdownLevel::None };
  // Empty means the field has no requested-type override. Its schema type is
  // still the default logical output type.
  std::vector<LogicalTypeId> output_types;
  bool interpolatable{ false };
  bool null_predicate{ false };
  // Empty means every adapter-declared result mode. A non-empty list allows
  // fields such as encoded message bytes to exist only in BinaryView mode.
  std::vector<QueryFragment::ResultMode> result_modes;
};

class QueryCapabilities {
public:
  QueryCapabilities(std::uint32_t interface_version, std::string adapter_id,
    std::string adapter_version, LogicalSchema schema,
    std::vector<FieldQueryCapability> fields,
    std::vector<DomainKind> domains, bool sequential_scan_available,
    bool layout_optional, OrderingGuarantee ordering,
    std::vector<QueryFragment::ResultMode> result_modes = {
      QueryFragment::ResultMode::LogicalValues },
    std::vector<QueryFragment::InterpolationMethod> interpolation_methods = {})
    : interface_version_(interface_version),
      adapter_id_(std::move(adapter_id)),
      adapter_version_(std::move(adapter_version)),
      schema_(std::move(schema)), fields_(std::move(fields)),
      domains_(std::move(domains)),
      sequential_scan_available_(sequential_scan_available),
      layout_optional_(layout_optional), ordering_(ordering),
      result_modes_(std::move(result_modes)),
      interpolation_methods_(std::move(interpolation_methods)) {}

  std::uint32_t interface_version() const { return interface_version_; }
  const std::string& adapter_id() const { return adapter_id_; }
  const std::string& adapter_version() const { return adapter_version_; }
  const LogicalSchema& schema() const { return schema_; }
  const std::vector<FieldQueryCapability>& fields() const { return fields_; }
  const std::vector<DomainKind>& domains() const { return domains_; }
  bool sequential_scan_available() const {
    return sequential_scan_available_;
  }
  bool layout_optional() const { return layout_optional_; }
  OrderingGuarantee ordering() const { return ordering_; }
  const std::vector<QueryFragment::ResultMode>& result_modes() const {
    return result_modes_;
  }
  const std::vector<QueryFragment::InterpolationMethod>&
  interpolation_methods() const { return interpolation_methods_; }

private:
  std::uint32_t interface_version_{ 1 };
  std::string adapter_id_;
  std::string adapter_version_;
  LogicalSchema schema_;
  std::vector<FieldQueryCapability> fields_;
  std::vector<DomainKind> domains_;
  bool sequential_scan_available_{ false };
  bool layout_optional_{ false };
  OrderingGuarantee ordering_{ OrderingGuarantee::None };
  std::vector<QueryFragment::ResultMode> result_modes_;
  std::vector<QueryFragment::InterpolationMethod> interpolation_methods_;
};

QueryStatus validate_query_capabilities(
  const QueryCapabilities& capabilities);

// Opaque, adapter-owned physical plan. GRIB messages, Arrow buffers, and
// TsFile chunks/pages remain behind this Seam.
struct FormatUnitLocator {
  std::string unit_id;
  std::uint64_t offset{0}, length{0};
};

class AdapterExecutionPlan {
public:
  virtual ~AdapterExecutionPlan() = default;

  // A bounded metadata-only discovery visitor. The adapter owns unit identity
  // and byte-range semantics. No decoded values or request capability escapes.
  virtual QueryStatus visit_unit_locators(
      const std::function<QueryStatus(const FormatUnitLocator&)>&) const {
    return QueryStatus::failure(QueryError::UnsupportedCapability,
      QueryPhase::Planning, "adapter plan does not support unit discovery");
  }

  // Optional, immutable description retained even for a zero-row result.
  // Keys are local to this plan; the frontend supplies a descriptor key prefix.
  virtual const QueryResultMetadata& result_metadata() const {
    static const QueryResultMetadata empty;
    return empty;
  }

  AdapterExecutionPlan(const AdapterExecutionPlan&) = delete;
  AdapterExecutionPlan& operator=(const AdapterExecutionPlan&) = delete;

protected:
  AdapterExecutionPlan() = default;
};

class AdapterPlan {
private:
  struct State {
    State(std::shared_ptr<const AdapterExecutionPlan> implementation,
        LogicalSchema adapter_output_schema,
        QueryExpressionPtr residual_predicate,
        std::vector<DomainSlice> residual_slices,
        std::vector<FieldRef> final_projection)
      : implementation(std::move(implementation)),
        adapter_output_schema(std::move(adapter_output_schema)),
        residual_predicate(std::move(residual_predicate)),
        residual_slices(std::move(residual_slices)),
        final_projection(std::move(final_projection)) {}

    std::shared_ptr<const AdapterExecutionPlan> implementation;
    LogicalSchema adapter_output_schema;
    QueryExpressionPtr residual_predicate;
    std::vector<DomainSlice> residual_slices;
    std::vector<FieldRef> final_projection;
  };

public:
  AdapterPlan(std::shared_ptr<const AdapterExecutionPlan> implementation,
    LogicalSchema adapter_output_schema,
    QueryExpressionPtr residual_predicate,
    std::vector<DomainSlice> residual_slices,
    std::vector<FieldRef> final_projection)
    : state_(std::make_shared<const State>(std::move(implementation),
        std::move(adapter_output_schema), std::move(residual_predicate),
        std::move(residual_slices), std::move(final_projection))) {}

  const std::shared_ptr<const AdapterExecutionPlan>& implementation() const {
    return state_->implementation;
  }
  const LogicalSchema& adapter_output_schema() const {
    return state_->adapter_output_schema;
  }
  const QueryExpressionPtr& residual_predicate() const {
    return state_->residual_predicate;
  }
  const std::vector<DomainSlice>& residual_slices() const {
    return state_->residual_slices;
  }
  const std::vector<FieldRef>& final_projection() const {
    return state_->final_projection;
  }

private:
  std::shared_ptr<const State> state_;
};

struct QueryExecutionOptions {
  std::size_t target_batch_rows{ 65536 };
  std::uint64_t target_batch_bytes{ 8ULL * 1024ULL * 1024ULL };
  std::size_t max_ranges{ 65536 };
  std::uint64_t max_layout_bytes{ 64ULL * 1024ULL * 1024ULL };
  std::uint64_t max_payload_bytes{ 1024ULL * 1024ULL * 1024ULL };
  std::uint64_t max_result_rows{ 100000000ULL };
};

class QueryBatchCursor {
public:
  virtual ~QueryBatchCursor() = default;

  virtual const LogicalSchema& schema() const = 0;
  virtual QueryResult<std::optional<QueryBatch>> next() = 0;
  virtual void cancel() = 0;
};

struct AdapterDomainSourceMatch {
  std::size_t source_ordinal{0};
  // Request-scoped, immutable adapter evidence; never retained in the domain.
  std::shared_ptr<const AdapterPlanningState> planning_state;
};
using AdapterDomainSelection = std::vector<AdapterDomainSourceMatch>;

class AdapterDomainIndex {
public:
  virtual ~AdapterDomainIndex() = default;
  virtual std::uint64_t retained_bytes() const = 0;
  // nullopt means the full-domain semantics cannot be certified: use the
  // ordinary planner. Ordinals refer to the original full candidate order.
  virtual QueryResult<std::optional<AdapterDomainSelection>> select(
    const QueryFragment&, const QueryLimits&) const = 0;
};

class FormatQueryAdapter {
public:
  virtual ~FormatQueryAdapter() = default;

  virtual const std::string& id() const = 0;
  virtual bool supports_unit_catalog(const QueryFragment&) const { return false; }
  virtual bool supports_domain_index(const QueryFragment&) const { return false; }
  virtual QueryResult<std::shared_ptr<const AdapterDomainIndex>> build_domain_index(
      const std::vector<std::shared_ptr<const QuerySource>>&,
      const QueryFragment&, const QueryLimits&, std::uint64_t) const {
    return QueryResult<std::shared_ptr<const AdapterDomainIndex>>::success(nullptr);
  }
  virtual QueryResult<QueryCapabilities> capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const = 0;
  virtual QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context,
    const QueryFragment& fragment) const = 0;
  virtual QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
    const QueryExecutionContext& context,
    std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const = 0;
};

struct PreparedSourceQuery {
  std::shared_ptr<const QuerySource> source;
  std::shared_ptr<const FormatQueryAdapter> adapter;
  std::optional<AdapterPlan> plan;
};

// Immutable output of QueryModule::prepare. Latest has already been resolved,
// and every entry therefore points at an exact File Version.
class PreparedQuery {
public:
  static QueryResult<PreparedQuery> create(QueryRequest request,
    std::optional<LogicalSchema> result_schema,
    std::vector<PreparedSourceQuery> sources);

  const QueryRequest& request() const { return request_; }
  const std::shared_ptr<const QueryReadContext>& read_context() const { return read_context_; }
  const std::optional<LogicalSchema>& result_schema() const {
    return result_schema_;
  }
  const std::vector<PreparedSourceQuery>& sources() const { return sources_; }

private:
  friend class QueryModule;
  void retain_read_context(std::shared_ptr<const QueryReadContext> context) { read_context_ = std::move(context); }
  PreparedQuery(QueryRequest request,
    std::optional<LogicalSchema> result_schema,
    std::vector<PreparedSourceQuery> sources)
    : request_(std::move(request)), result_schema_(std::move(result_schema)),
      sources_(std::move(sources)) {}

  QueryRequest request_;
  std::shared_ptr<const QueryReadContext> read_context_;
  std::optional<LogicalSchema> result_schema_;
  std::vector<PreparedSourceQuery> sources_;
};

} // namespace iotdbfs
