#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <set>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <new>
#include <list>
#include <map>
#include <mutex>

namespace iotdbfs {
// Only validated immutable values are published. Readers retain shared ownership
// across eviction; no source callback runs while this cache's mutex is held.
class Hdf5ValidatedLayoutCache {
public:
  explicit Hdf5ValidatedLayoutCache(Hdf5LayoutCacheConfig config) : config_(config) {}
  std::shared_ptr<const Hdf5LayoutDirectory> directory(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) return {};
    lru_.splice(lru_.begin(), lru_, it->second.position);
    return it->second.directory;
  }
  std::shared_ptr<const Hdf5ValidatedChunkPage> page(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) return {};
    lru_.splice(lru_.begin(), lru_, it->second.position);
    return it->second.page;
  }
  void put(const std::string& key, std::shared_ptr<const Hdf5LayoutDirectory> directory,
      std::shared_ptr<const Hdf5ValidatedChunkPage> page = {}) {
    // Both key copies and conservative container/control-block overhead are
    // charged in addition to all retained vector/string capacities. Evicted
    // values still pinned by active plans belong to those requests, not the LRU.
    const std::uint64_t bytes = sizeof(Entry) + 256 + 2 * (key.size() + 1) +
      (directory ? directory->retained_bytes() : page->retained_bytes());
    if (!config_.max_entries || bytes > config_.max_bytes) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.find(key) != entries_.end()) return;
    while (!lru_.empty() &&
        (entries_.size() >= config_.max_entries || used_bytes_ > config_.max_bytes - bytes)) {
      auto old = entries_.find(lru_.back());
      used_bytes_ -= old->second.bytes;
      entries_.erase(old); lru_.pop_back();
    }
    lru_.push_front(key);
    try {
      entries_.emplace(key, Entry{std::move(directory), std::move(page), bytes, lru_.begin()});
    } catch (...) { lru_.pop_front(); throw; }
    used_bytes_ += bytes;
  }
private:
  struct Entry {
    std::shared_ptr<const Hdf5LayoutDirectory> directory;
    std::shared_ptr<const Hdf5ValidatedChunkPage> page;
    std::uint64_t bytes;
    std::list<std::string>::iterator position;
  };
  const Hdf5LayoutCacheConfig config_;
  std::mutex mutex_;
  std::list<std::string> lru_;
  std::map<std::string, Entry> entries_;
  std::uint64_t used_bytes_{0};
};
std::shared_ptr<Hdf5ValidatedLayoutCache> make_hdf5_validated_layout_cache(
    Hdf5LayoutCacheConfig config) {
  return std::make_shared<Hdf5ValidatedLayoutCache>(config);
}
namespace {
template<class T> QueryResult<T> fail(QueryError code, const char* message,
    QueryPhase phase = QueryPhase::Planning) {
  return QueryResult<T>::failure(QueryStatus::failure(code, phase, message));
}
QueryStatus attribute_error(const QueryStatus& status, const QuerySource* source,
    const char* operation) {
  if (status.ok()) return status;
  auto detail = status.detail();
  if (source && !detail.source) {
    try {
      const auto& manifest = source->manifest();
      detail.source = QuerySourceReference{manifest.key.path, manifest.key.version,
        manifest.format_id.value_or("hdf5")};
    } catch (...) { /* Preserve the original error if source metadata fails. */ }
  }
  if (!detail.operation) detail.operation = operation;
  return QueryStatus::failure(status.code(), status.phase(), status.message(),
    std::move(detail));
}
template<class T, class F> QueryResult<T> guarded(F&& f, QueryPhase phase,
    const QuerySource* source, const char* operation) {
  try {
    auto result = f();
    if (!result.ok()) return QueryResult<T>::failure(
      attribute_error(result.status(), source, operation));
    return result;
  }
  catch (const std::bad_alloc&) {
    return QueryResult<T>::failure(attribute_error(QueryStatus::failure(
      QueryError::ResourceExhausted, phase, "HDF5 query allocation failed"),
      source, operation));
  } catch (...) {
    return QueryResult<T>::failure(attribute_error(QueryStatus::failure(
      QueryError::Internal, phase, "HDF5 query callback failed"),
      source, operation));
  }
}
bool matches(const StructuredBlobManifest& a, const StructuredBlobManifest& b) {
  return a.key == b.key && a.format_id == b.format_id && a.revision == b.revision &&
    a.payload_size == b.payload_size && a.layout_size == b.layout_size &&
    a.published == b.published && a.phase == b.phase;
}
QueryResult<std::string> layout_cache_key(const QuerySource& source) {
  auto verified = source.verified_layout_identity();
  if (!verified.ok()) return QueryResult<std::string>::failure(verified.status());
  std::string key;
  auto number = [&](std::uint64_t n) {
    for (unsigned i = 0; i < 8; ++i) key.push_back(static_cast<char>(n >> (i * 8)));
  };
  const auto& manifest = source.manifest();
  if (verified.value()) {
    if (verified.value()->empty())
      return fail<std::string>(QueryError::CorruptLayout, "verified HDF5 layout identity is empty");
    key = "iotdbfs-hdf5-verified-layout/1";
    number(verified.value()->size()); key += *verified.value();
    number(manifest.payload_size); number(manifest.layout_size);
    return QueryResult<std::string>::success(std::move(key));
  }
  key = "iotdbfs-hdf5-exact-layout/1";
  number(source.storage_namespace()); number(manifest.key.path.size()); key += manifest.key.path;
  number(manifest.key.version); number(manifest.revision);
  number(manifest.payload_size); number(manifest.layout_size);
  return QueryResult<std::string>::success(std::move(key));
}
std::string page_cache_key(const std::string& identity, const Hdf5ChunkPage& page) {
  auto key = identity;
  for (const auto n : {page.offset, page.length, page.record_count,
                        static_cast<std::uint64_t>(page.object_id.size())})
    for (unsigned i = 0; i < 8; ++i) key.push_back(static_cast<char>(n >> (i * 8)));
  key += page.object_id;
  key.append(reinterpret_cast<const char*>(page.checksum.data()), page.checksum.size());
  return key;
}
// Lossless even for a malformed bound; validation assigns the error class.
std::string request_key(const QueryFragment& f) {
  std::string out;
  auto append = [&](const std::string& s) { out += std::to_string(s.size()) + ":" + s; };
  auto scalar = [&](const LogicalValue& value) {
    append(std::to_string(static_cast<unsigned>(value.type())));
    std::visit([&](const auto& v) {
      using T = std::decay_t<decltype(v)>;
      if constexpr (std::is_same_v<T, std::monostate>) append("null");
      else if constexpr (std::is_same_v<T, std::string>) append(v);
      else if constexpr (std::is_same_v<T, BinaryValue>) append(std::string(v.begin(), v.end()));
      else append(std::string(reinterpret_cast<const char*>(&v), sizeof(v)));
    }, value.value());
  };
  for (const auto& field : f.projection) append(field.field_id);
  out += "|";
  for (const auto& slice : f.slices) {
    append(slice.dimension.field_id);
    for (const auto* bound : {&slice.lower, &slice.upper}) {
      if (!*bound) { append("none"); continue; }
      append("bound");
      append(std::to_string(static_cast<unsigned>((*bound)->kind)));
      scalar((*bound)->value);
    }
  }
  std::function<void(const QueryExpressionPtr&)> expression = [&](const QueryExpressionPtr& expr) {
    if (!expr) { append("null"); return; }
    append(std::to_string(expr->node.index()));
    std::visit([&](const auto& node) {
      using T = std::decay_t<decltype(node)>;
      if constexpr (std::is_same_v<T, LogicalExpression>) {
        append(std::to_string(static_cast<unsigned>(node.operation)));
        append(std::to_string(node.operands.size()));
        for (const auto& child : node.operands) expression(child);
      } else {
        append(node.field.field_id);
        if constexpr (std::is_same_v<T, ComparisonExpression>) {
          append(std::to_string(static_cast<unsigned>(node.operation))); scalar(node.value);
        } else if constexpr (std::is_same_v<T, InExpression>) {
          append(node.negated ? "1" : "0"); append(std::to_string(node.values.size()));
          for (const auto& value : node.values) scalar(value);
        } else append(node.negated ? "1" : "0");
      }
    }, expr->node);
  };
  expression(f.predicate);
  return out;
}
std::string semantics_key(const Hdf5SemanticConfig& config,
    const std::vector<std::shared_ptr<const Hdf5SemanticProfile>>& profiles) {
  std::string key;
  auto text = [&](const std::string& value) { key += std::to_string(value.size()) + ":" + value; };
  for (const auto& spec : config.gwosc_bindings) {
    text(std::to_string(spec.storage_namespace)); text(spec.source.path);
    text(std::to_string(spec.source.version)); text(spec.dataset_path);
  }
  for (const auto& profile : profiles) text(profile->id());
  return key;
}
class LayoutProvider final : public Hdf5DatasetProvider {
public:
  LayoutProvider(const Hdf5LayoutObject& object, std::string path)
    : object_(object), path_(std::move(path)) {}
  QueryResult<Hdf5DatasetDescriptor> describe(const std::string& path) const override {
    if (path != path_ || !object_.dataset)
      return fail<Hdf5DatasetDescriptor>(QueryError::UnknownField,
        "HDF5 selected dataset path is unavailable");
    auto descriptor = object_.dataset->descriptor;
    // An alias binds the caller's path while retaining the shared object token.
    descriptor.path = path;
    return QueryResult<Hdf5DatasetDescriptor>::success(std::move(descriptor));
  }
private:
  const Hdf5LayoutObject& object_;
  const std::string path_;
};
struct PlanningState final : AdapterPlanningState {
  PlanningState(Hdf5QueryIdentity identity, std::string key, std::string semantic_key,
      std::string layout_key,
      Hdf5QueryBinding binding, Hdf5DatasetLayout dataset,
      std::shared_ptr<const Hdf5LayoutDirectory> directory,
      Hdf5PreparedSelection prepared, PayloadReadCapabilities transport,
      Hdf5QueryMode mode)
    : identity(std::move(identity)), key(std::move(key)), semantic_key(std::move(semantic_key)),
      layout_key(std::move(layout_key)),
      binding(std::move(binding)), dataset(std::move(dataset)), directory(std::move(directory)),
      prepared(std::move(prepared)), transport(transport), mode(mode) {}
  Hdf5QueryIdentity identity;
  std::string key, semantic_key, layout_key;
  Hdf5QueryBinding binding;
  Hdf5DatasetLayout dataset;
  std::shared_ptr<const Hdf5LayoutDirectory> directory;
  Hdf5PreparedSelection prepared;
  PayloadReadCapabilities transport;
  Hdf5QueryMode mode;
};
class AttributedCursor final : public QueryBatchCursor {
public:
  AttributedCursor(std::unique_ptr<QueryBatchCursor> inner,
      std::shared_ptr<const QuerySource> source)
    : inner_(std::move(inner)), source_(std::move(source)) {}
  const LogicalSchema& schema() const override { return inner_->schema(); }
  QueryResult<std::optional<QueryBatch>> next() override {
    return guarded<std::optional<QueryBatch>>([&] { return inner_->next(); },
      QueryPhase::Execution, source_.get(), "hdf5.cursor.next");
  }
  void cancel() override { inner_->cancel(); }
private:
  std::unique_ptr<QueryBatchCursor> inner_;
  std::shared_ptr<const QuerySource> source_;
};
QueryResultMetadata result_descriptor(const PlanningState& state) {
  const auto& identity = state.identity;
  QueryResultMetadata metadata{
    {"protocol", "iotdbfs-hdf5-subarray/1"},
    {"hdf5.query_mode", hdf5_query_mode_name(state.mode)},
    {"hdf5.payload_backend", state.mode == Hdf5QueryMode::NativeHdf5 ? "filesystem" : "database"},
    {"source.path", identity.source.key.path},
    {"source.version", std::to_string(identity.source.key.version)},
    {"source.revision", std::to_string(identity.source.revision)},
    {"source.format", "hdf5"},
    {"source.payload_size", std::to_string(identity.source.payload_size)},
    {"source.layout_size", std::to_string(identity.source.layout_size)},
    {"source.storage_namespace", std::to_string(identity.storage_namespace)},
    {"source.storage_namespace_scope", "process_local"},
    {"dataset.path", identity.dataset_path},
    {"dataset.object_id", identity.dataset_id},
    {"dataset.value_type", identity.value_type == LogicalTypeId::Int32 ? "int32" : identity.value_type == LogicalTypeId::Float32 ? "float32" : "float64"},
    {"dataset.native_element_bytes", std::to_string(state.dataset.element_bytes)},
    {"output.value_type", state.binding.scientific_values() ? "float64" : identity.value_type == LogicalTypeId::Int32 ? "int32" : identity.value_type == LogicalTypeId::Float32 ? "float32" : "float64"},
    {"dataset.rank", std::to_string(identity.dataset_shape.size())},
    {"selection.row_count", std::to_string(state.binding.row_count())},
    {"order", "C"},
    {"view", identity.view},
  };
  std::string checksum;
  checksum.reserve(identity.layout_checksum.size() * 2);
  constexpr char hex[] = "0123456789abcdef";
  for (const auto byte : identity.layout_checksum) {
    checksum.push_back(hex[byte >> 4]); checksum.push_back(hex[byte & 15]);
  }
  metadata.emplace_back("layout.directory_sha256", std::move(checksum));
  metadata.emplace_back("layout.protocol",
    std::string("IOTDBFS-HDF5-LAYOUT/") +
    std::to_string(state.directory->protocol_version()));
  for (std::size_t axis = 0; axis < identity.dataset_shape.size(); ++axis) {
    const auto prefix = "axis." + std::to_string(axis) + ".";
    metadata.emplace_back(prefix + "field_id", hdf5_axis_field(axis).field_id);
    metadata.emplace_back(prefix + "type", "uint64");
    metadata.emplace_back(prefix + "full_extent", std::to_string(identity.dataset_shape[axis]));
    metadata.emplace_back(prefix + "selection_origin", std::to_string(state.binding.origin()[axis]));
    metadata.emplace_back(prefix + "selection_extent", std::to_string(state.binding.shape()[axis]));
  }
  if (state.binding.cf_binding()) {
    const auto& cf = *state.binding.cf_binding();
    auto number = [](double value) {
      std::ostringstream text; text.imbue(std::locale::classic());
      text << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
      return text.str();
    };
    metadata.emplace_back("coordinate.profile", cf.profile);
    if (state.binding.scientific_values()) {
      metadata.emplace_back("transform.kind", "cf-mask-scale-offset/1");
      metadata.emplace_back("transform.scale_factor", number(cf.scale_factor));
      metadata.emplace_back("transform.add_offset", number(cf.add_offset));
      metadata.emplace_back("transform.operation", "raw*scale_factor+add_offset;float64;no-fma");
      metadata.emplace_back("transform.mask_domain", "raw");
      metadata.emplace_back("transform.nonfinite", "masked");
      metadata.emplace_back("transform.units", cf.units);
      if (cf.valid_min) metadata.emplace_back("transform.valid_min", number(*cf.valid_min));
      if (cf.valid_max) metadata.emplace_back("transform.valid_max", number(*cf.valid_max));
      metadata.emplace_back("transform.missing_count", std::to_string(cf.missing_values.size()));
      for (std::size_t i = 0; i < cf.missing_values.size(); ++i)
        metadata.emplace_back("transform.missing." + std::to_string(i), number(cf.missing_values[i]));
    }
    for (const auto& axis : cf.axes) {
      const auto prefix = "coordinate." + axis.role + ".";
      metadata.emplace_back(prefix + "axis", std::to_string(axis.dimension));
      metadata.emplace_back(prefix + "object_id", axis.coordinate_object_id);
      metadata.emplace_back(prefix + "units", axis.units);
      metadata.emplace_back(prefix + "calendar", axis.calendar);
      if (axis.role == "time") {
        bool declared_calendar = false;
        for (const auto& object : state.directory->document().objects)
          if (object.id == axis.coordinate_object_id)
            declared_calendar = std::any_of(object.attributes.begin(), object.attributes.end(), [](const auto& attribute) { return attribute.name == "calendar"; });
        metadata.emplace_back(prefix + "calendar_source", declared_calendar ? "source_attribute" : "CF_default_standard");
      }
      metadata.emplace_back(prefix + "evidence", axis.evidence);
      metadata.emplace_back(prefix + "order", axis.ascending ? "ascending" : "descending");
      metadata.emplace_back(prefix + "transform", "none");
      metadata.emplace_back(prefix + "count", std::to_string(axis.values.size()));
    }
  }
  if (identity.gps_axis) {
    const auto& grid = *identity.gps_axis;
    auto number = [](double value) {
      std::ostringstream text;
      text.imbue(std::locale::classic());
      text << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
      return text.str();
    };
    metadata.emplace_back("coordinate.field_id", kHdf5GpsSecondsField);
    metadata.emplace_back("coordinate.type", "float64");
    metadata.emplace_back("coordinate.axis", "0");
    metadata.emplace_back("coordinate.time_system", "GPS");
    metadata.emplace_back("coordinate.unit", "second");
    metadata.emplace_back("coordinate.start", number(grid.start()));
    metadata.emplace_back("coordinate.spacing", number(grid.spacing()));
    metadata.emplace_back("coordinate.count", std::to_string(grid.count()));
    metadata.emplace_back("coordinate.units_provenance", grid.units_verified() ? "verified_attribute" : "application_declared");
    metadata.emplace_back("coordinate.label_provenance", grid.label_verified() ? "verified_attribute" : "application_declared");
    metadata.emplace_back("coordinate.mapping", "fma(ordinal,spacing,start)");
    metadata.emplace_back("coordinate.transform", "none");
  }
  return metadata;
}
class Hdf5Plan final : public AdapterExecutionPlan {
public:
  explicit Hdf5Plan(std::shared_ptr<const PlanningState> state)
    : state(std::move(state)), metadata_(result_descriptor(*this->state)) {}
  const QueryResultMetadata& result_metadata() const override { return metadata_; }
  const std::shared_ptr<const PlanningState> state;
private:
  const QueryResultMetadata metadata_;
};
QueryResult<std::shared_ptr<const PlanningState>> bind(const QueryPlanningContext& context,
    const QueryFragment& fragment, const Hdf5QueryKernelConfig& config,
    const Hdf5SemanticConfig& semantics,
    const std::vector<std::shared_ptr<const Hdf5SemanticProfile>>& profiles,
    const std::string& semantic_key, Hdf5ValidatedLayoutCache& cache,
    Hdf5QueryMode mode) {
  using Result = QueryResult<std::shared_ptr<const PlanningState>>;
  const auto& source = context.source();
  const auto& manifest = source.manifest();
  if (manifest.format_id != std::optional<std::string>("hdf5"))
    return fail<std::shared_ptr<const PlanningState>>(QueryError::UnsupportedCapability, "source is not HDF5");
  if (!manifest.published || manifest.phase != StructuredBlobPhase::Published)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::SourceNotPublished, "HDF5 source is not published");
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, manifest.key.path}};
  request.sources.versions = VersionSelection::exact_version(manifest.key.version);
  request.subset = fragment; request.limits = context.limits();
  auto valid = validate_query_request(request);
  if (!valid.ok()) return Result::failure(valid);
  if (!has_hdf5_query_fields(fragment))
    return fail<std::shared_ptr<const PlanningState>>(QueryError::InvalidQuery, "HDF5 dataset value projection is required");
  const auto key = request_key(fragment);
  // Even a request-local state hit must observe a revoked or frozen proof.
  auto identity_key = layout_cache_key(source);
  if (!identity_key.ok()) return Result::failure(identity_key.status());
  const auto cache_key = std::move(identity_key.value());
  if (context.state()) {
    auto state = std::dynamic_pointer_cast<const PlanningState>(context.state());
    if (!state || !matches(state->identity.source, manifest) ||
        state->identity.storage_namespace != source.storage_namespace() || state->key != key ||
        state->semantic_key != semantic_key || state->layout_key != cache_key || state->mode != mode)
      return fail<std::shared_ptr<const PlanningState>>(QueryError::StalePlan,
        "HDF5 planning context cannot be reused for another source or request");
    return Result::success(std::move(state));
  }
  if (!manifest.layout_size)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::LayoutUnavailable, "HDF5 layout is required");
  Hdf5LayoutLimits limits;
  limits.max_read_bytes = std::min(limits.max_read_bytes, context.limits().max_layout_bytes);
  limits.max_read_calls = std::min(limits.max_read_calls, context.limits().max_ranges);
  if (manifest.layout_size < kHdf5LayoutHeaderBytes)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::CorruptLayout, "truncated HDF5 layout header");
  if (!limits.max_read_calls || limits.max_read_bytes < kHdf5LayoutHeaderBytes)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::ResourceExhausted, "HDF5 layout identity exceeds planning budget");
  // Published versions have immutable layout bytes; their key is never reused
  // after erase. A cache miss validates the header and directory together.
  Hdf5LayoutRead read = [&](std::uint64_t offset, std::uint64_t length) {
    try {
      QueryResult<BinaryValue> result;
      if (offset == 0 && length == kHdf5LayoutHeaderBytes) {
        query_profile_count("hdf5.layout.cache.identity.calls");
        query_profile_count("hdf5.layout.cache.identity.bytes", length);
        QueryProfileTimer timer("hdf5.layout.cache.identity.read");
        result = source.read_layout(offset, length);
      } else result = source.read_layout(offset, length);
      if (!result.ok()) return QueryResult<BinaryValue>::failure(
        attribute_error(result.status(), &source, "read_layout"));
      return result;
    } catch (const std::bad_alloc&) {
      return QueryResult<BinaryValue>::failure(attribute_error(
        QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Storage,
          "HDF5 layout read allocation failed"), &source, "read_layout"));
    } catch (...) {
      return QueryResult<BinaryValue>::failure(attribute_error(
        QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage,
          "HDF5 layout source callback failed"), &source, "read_layout"));
    }
  };
  auto directory = cache.directory(cache_key);
  if (directory) {
    query_profile_count("hdf5.layout.cache.directory.hits");
    // Preserve logical request budgets even when no body SQL/read is needed.
    if (directory->directory_read_bytes() > limits.max_read_bytes ||
        directory->directory_read_calls() > limits.max_read_calls)
      return fail<std::shared_ptr<const PlanningState>>(QueryError::ResourceExhausted, "HDF5 cached directory exceeds planning budget");
  } else {
    query_profile_count("hdf5.layout.cache.directory.misses");
    auto parsed = read_hdf5_layout_directory(read, manifest.layout_size, manifest.payload_size, limits);
    if (!parsed.ok()) return Result::failure(parsed.status());
    directory = std::make_shared<const Hdf5LayoutDirectory>(std::move(parsed.value()));
    cache.put(cache_key, directory);
  }
  std::string dataset_path;
  bool scientific = false;
  for (const auto& field : fragment.projection) {
    auto path = parse_hdf5_value_field(field);
    if (path.ok()) { dataset_path = std::move(path.value()); scientific = is_hdf5_science_value_field(field); break; }
  }
  auto target_id = resolve_hdf5_layout_path(*directory, dataset_path);
  if (!target_id.ok()) return Result::failure(target_id.status());
  const auto* target_object = directory->object(target_id.value());
  if (!target_object || !target_object->dataset)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::CorruptLayout,
      "HDF5 dataset object is absent");
  LayoutProvider provider(*target_object, dataset_path);
  std::vector<LogicalField> attribute_fields;
  bool predicate_matches = true;
  if (fragment.predicate) {
    QueryProfileTimer timer("hdf5.dataset.predicate");
    query_profile_count("hdf5.dataset.predicate.evaluations");
    auto record = hdf5_dataset_catalog_record(*target_object, dataset_path);
    auto schema = build_dataset_attribute_schema(record, fragment.predicate);
    if (!schema.ok()) return Result::failure(schema.status());
    attribute_fields = schema.value().fields();
    auto matched = evaluate_dataset_attribute_predicate(record, fragment.predicate);
    if (!matched.ok()) return Result::failure(matched.status());
    predicate_matches = matched.value();
    if (!predicate_matches) query_profile_count("hdf5.dataset.predicate.rejected");
  }
  const bool semantic_coordinates =
    std::any_of(fragment.projection.begin(), fragment.projection.end(), [](const FieldRef& f) {
      return f.field_id == kHdf5TimeField || f.field_id == kHdf5LatitudeField ||
             f.field_id == kHdf5LongitudeField || f.field_id == kHdf5PressureField;
    }) ||
    std::any_of(fragment.slices.begin(), fragment.slices.end(), [](const DomainSlice& s) {
      return s.dimension.field_id == kHdf5TimeField ||
             s.dimension.field_id == kHdf5LatitudeField ||
             s.dimension.field_id == kHdf5LongitudeField ||
             s.dimension.field_id == kHdf5PressureField;
    });
  std::optional<Hdf5CfBinding> cf_binding;
  if (scientific || semantic_coordinates) {
    QueryProfileTimer timer("hdf5.dataset.semantic_binding");
    QueryStatus last = QueryStatus::failure(
      QueryError::UnsupportedCapability, QueryPhase::Planning,
      "no declared HDF5 semantic profile matches this dataset");
    for (const auto& profile : profiles) {
      if (!profile->declares(directory->document())) continue;
      auto compiled = profile->compile(directory->document(), target_id.value());
      if (compiled.ok()) {
        cf_binding = std::move(compiled.value());
        break;
      }
      if (compiled.status().code() != QueryError::UnsupportedCapability)
        return Result::failure(compiled.status());
      last = compiled.status();
    }
    if (!cf_binding) return Result::failure(last);
  }
  std::optional<Hdf5GpsTimeAxis> gps_axis;
  const bool requests_gps =
    std::any_of(fragment.projection.begin(), fragment.projection.end(), [](const FieldRef& f) { return f.field_id == kHdf5GpsSecondsField; }) ||
    std::any_of(fragment.slices.begin(), fragment.slices.end(), [](const DomainSlice& s) { return s.dimension.field_id == kHdf5GpsSecondsField; });
  if (requests_gps) {
    bool declared = false;
    for (const auto& spec : semantics.gwosc_bindings) {
      if (!(spec.source == manifest.key) || spec.storage_namespace != source.storage_namespace()) continue;
      auto configured = resolve_hdf5_layout_path(*directory, spec.dataset_path);
      if (configured.ok() && configured.value() == target_id.value()) { declared = true; break; }
    }
    if (!declared) return fail<std::shared_ptr<const PlanningState>>(QueryError::UnsupportedCapability,
      "GPS coordinates require an explicit binding for this source version and dataset");
    auto compiled = compile_hdf5_gwosc_axis(*target_object);
    if (!compiled.ok()) return Result::failure(compiled.status());
    gps_axis = std::move(compiled.value());
  }
  auto binding = bind_hdf5_query(provider, fragment, context.limits(), std::move(gps_axis), std::move(cf_binding), std::move(attribute_fields), predicate_matches);
  if (!binding.ok()) return Result::failure(binding.status());
  Hdf5DatasetLayout dataset = *target_object->dataset;
  if (mode == Hdf5QueryMode::IoTDBIndexed && !dataset.index_complete)
    return fail<std::shared_ptr<const PlanningState>>(QueryError::LayoutUnavailable, "HDF5 dataset index is unavailable");
  if (mode == Hdf5QueryMode::IoTDBIndexed &&
      dataset.storage == Hdf5StorageLayout::Chunked && binding.value().row_count()) {
    Hdf5LayoutReadBudget budget;
    Hdf5ChunkPageCache pages;
    pages.find = [&](const Hdf5ChunkPage& page) {
      return cache.page(page_cache_key(cache_key, page));
    };
    pages.insert = [&](const Hdf5ChunkPage& page,
        std::shared_ptr<const Hdf5ValidatedChunkPage> parsed) {
      cache.put(page_cache_key(cache_key, page), {}, std::move(parsed));
    };
    auto chunks = read_hdf5_layout_chunks(read, *directory, target_id.value(),
      binding.value().origin(), binding.value().shape(), budget, limits, &pages);
    if (!chunks.ok()) return Result::failure(chunks.status());
    dataset.chunks = std::move(chunks.value());
  }
  const auto transport = source.payload_read_capabilities();
  Hdf5PreparedSelection prepared;
  if (mode == Hdf5QueryMode::IoTDBIndexed) {
    auto selected = prepare_hdf5_selection(dataset, binding.value(), config, transport);
    if (!selected.ok()) {
      const auto& status = selected.status();
      return Result::failure(QueryStatus::failure(status.code(), QueryPhase::Planning,
        status.message(), status.detail()));
    }
    prepared = std::move(selected.value());
  }
  Hdf5QueryIdentity identity{manifest, source.storage_namespace(), directory->checksum(),
    target_id.value(), binding.value().descriptor().path, dataset.descriptor.value_type,
    dataset.descriptor.shape, "physical-ordinal-c-order/1", std::nullopt};
  if (binding.value().cf_binding()) {
    identity.view = binding.value().cf_binding()->profile +
      (binding.value().scientific_values() ? ":science" : ":physical");
  }
  if (binding.value().gps_axis()) {
    identity.view = kHdf5GwoscProfile;
    identity.gps_axis = binding.value().gps_axis();
  }
  auto state = std::make_shared<const PlanningState>(std::move(identity),
    key, semantic_key, cache_key, std::move(binding.value()),
    std::move(dataset), std::move(directory), std::move(prepared), transport, mode);
  context.set_state(state);
  return Result::success(std::move(state));
}
} // namespace

Hdf5FormatQueryAdapter::Hdf5FormatQueryAdapter(Hdf5QueryKernelConfig config,
    std::shared_ptr<QueryRuntime> runtime, Hdf5SemanticConfig semantics, Hdf5LayoutCacheConfig layout_cache,
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>> profiles,
    std::shared_ptr<Hdf5ValidatedLayoutCache> shared_layout_cache, Hdf5QueryMode mode)
    : config_(config), mode_(mode), runtime_(std::move(runtime)), semantics_(std::move(semantics)),
      profiles_(profiles.empty() ? default_hdf5_semantic_profiles() : std::move(profiles)),
      layout_cache_(shared_layout_cache ? std::move(shared_layout_cache)
                                        : make_hdf5_validated_layout_cache(layout_cache)) {
  if (config_.native_session_cache_enabled && !config_.native_session_pool)
    config_.native_session_pool = std::make_shared<Hdf5NativeSessionPool>();
  std::set<std::string> profile_ids;
  for (const auto& profile : profiles_)
    if (!profile || profile->id().empty() || !profile_ids.insert(profile->id()).second)
      throw std::invalid_argument("invalid or duplicate HDF5 semantic profile");
  std::set<std::tuple<std::uint64_t, std::string, std::int64_t, std::string>> declared;
  for (const auto& spec : semantics_.gwosc_bindings) {
    if (!spec.storage_namespace || spec.source.path.empty() || !make_hdf5_value_field(spec.dataset_path).ok() ||
        !declared.emplace(spec.storage_namespace, spec.source.path, spec.source.version, spec.dataset_path).second)
      throw std::invalid_argument("invalid or duplicate exact-source GWOSC binding");
  }
  semantics_key_ = semantics_key(semantics_, profiles_);
  if (!runtime_) {
    QueryRuntimeConfig limits;
    // Byte budget remains primary for small native chunks; keep metadata bounded.
    limits.max_entries = 65536;
    runtime_ = std::make_shared<QueryRuntime>(limits);
  }
}
const std::string& Hdf5FormatQueryAdapter::id() const {
  static const std::string value = "hdf5";
  return value;
}
QueryResult<QueryCapabilities> Hdf5FormatQueryAdapter::capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  return guarded<QueryCapabilities>([&] {
    auto state = bind(context, fragment, config_, semantics_, profiles_,
      semantics_key_, *layout_cache_, mode_);
    if (!state.ok()) return QueryResult<QueryCapabilities>::failure(state.status());
    return make_hdf5_query_capabilities(state.value()->binding);
  }, QueryPhase::Planning, &context.source(), "hdf5.capabilities");
}
QueryResult<AdapterPlan> Hdf5FormatQueryAdapter::prepare(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  return guarded<AdapterPlan>([&] {
    auto state = bind(context, fragment, config_, semantics_, profiles_,
      semantics_key_, *layout_cache_, mode_);
    if (!state.ok()) return QueryResult<AdapterPlan>::failure(state.status());
    const auto& binding = state.value()->binding;
    std::vector<LogicalField> fields;
    for (const auto& field : binding.projection()) fields.push_back(*binding.schema().find(field.field_id));
    auto schema = LogicalSchema::create(std::move(fields));
    if (!schema.ok()) return QueryResult<AdapterPlan>::failure(schema.status());
    return QueryResult<AdapterPlan>::success(AdapterPlan(std::make_shared<const Hdf5Plan>(state.value()),
      std::move(schema.value()), {}, {}, binding.projection()));
  }, QueryPhase::Planning, &context.source(), "hdf5.prepare");
}
QueryResult<std::unique_ptr<QueryBatchCursor>> Hdf5FormatQueryAdapter::execute(
    const QueryExecutionContext& context, std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const {
  using Result = QueryResult<std::unique_ptr<QueryBatchCursor>>;
  return guarded<std::unique_ptr<QueryBatchCursor>>([&]() -> Result {
    auto physical = std::dynamic_pointer_cast<const Hdf5Plan>(plan);
    if (!physical || physical->state->semantic_key != semantics_key_ || physical->state->mode != mode_ ||
        !context.source || !context.shared_source ||
        context.source->storage_namespace() != context.shared_source->storage_namespace() ||
        !matches(context.source->manifest(), context.shared_source->manifest()) ||
        physical->state->identity.storage_namespace != context.source->storage_namespace() ||
        !matches(physical->state->identity.source, context.source->manifest()))
      return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
        "HDF5 execution source identity differs from plan", QueryPhase::Execution);
    const auto& state = *physical->state;
    auto identity_key = layout_cache_key(*context.shared_source);
    if (!identity_key.ok()) return Result::failure(identity_key.status());
    if (identity_key.value() != state.layout_key)
      return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
        "HDF5 verified layout identity differs from plan", QueryPhase::Execution);
    const auto transport = context.shared_source->payload_read_capabilities();
    if (transport.multi_range != state.transport.multi_range ||
        transport.max_ranges != state.transport.max_ranges ||
        transport.max_bytes != state.transport.max_bytes)
      return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
        "HDF5 execution transport differs from planned source", QueryPhase::Execution);
    if (!options.max_ranges)
      return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::ResourceExhausted,
        "HDF5 execution range budget is empty", QueryPhase::Execution);
    // The plan retains the validated directory and selected pages. Revalidate
    // the proof above before a cursor can hit a decoded-value cache.
    // Length-prefixed fields avoid ambiguous source paths/object tokens. Alias
    // paths and request slices deliberately do not enter the canonical key.
    std::string identity = "iotdbfs-hdf5-decoded/2";
    auto number = [&](std::uint64_t n) {
      for (unsigned i = 0; i < 8; ++i) identity.push_back(static_cast<char>(n >> (i * 8)));
    };
    auto text = [&](const std::string& value) { number(value.size()); identity += value; };
    number(state.identity.storage_namespace);
    text(state.identity.source.key.path); number(state.identity.source.key.version);
    number(state.identity.source.revision); number(state.identity.source.payload_size);
    number(state.identity.source.layout_size); text(state.identity.dataset_id);
    identity.append(reinterpret_cast<const char*>(state.identity.layout_checksum.data()), state.identity.layout_checksum.size());
    number(config_.contiguous_tile_elements);
    number(config_.max_encoded_chunk_bytes); number(config_.max_decoded_chunk_bytes);
    number(config_.max_zlib_scratch_bytes);
    auto cursor = make_hdf5_query_cursor(
      mode_ == Hdf5QueryMode::NativeHdf5 ? context.source : context.shared_source,
      state.binding, state.dataset, options, config_, runtime_, std::move(identity),
      mode_ == Hdf5QueryMode::NativeHdf5 ? std::nullopt : std::optional<Hdf5PreparedSelection>(state.prepared), mode_);
    if (!cursor.ok()) return cursor;
    return Result::success(std::make_unique<AttributedCursor>(
      std::move(cursor.value()), context.shared_source));
  }, QueryPhase::Execution, context.source.get(), "hdf5.execute");
}
QueryResult<Hdf5QueryIdentity> hdf5_query_identity(std::shared_ptr<const AdapterExecutionPlan> plan) {
  auto physical = std::dynamic_pointer_cast<const Hdf5Plan>(plan);
  if (!physical) return fail<Hdf5QueryIdentity>(QueryError::InvalidQuery, "not an HDF5 execution plan");
  return QueryResult<Hdf5QueryIdentity>::success(physical->state->identity);
}
} // namespace iotdbfs
