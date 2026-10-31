#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryBatchBudget.h"
#include "iotdbfs/query/frontend/TypedAuthorizationCache.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"

#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/query/training/arrow/DatasetSnapshot.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/formats/hdf5/Hdf5GlobalAttributeIndex.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <poll.h>
#include <locale.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <initializer_list>
#include <optional>
#include <sstream>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {

using boost::property_tree::ptree;
void require_hdf5_object_fields(const ptree&, std::string_view,
    std::initializer_list<std::string_view>);
constexpr std::uint32_t kMaxFrame = 1024 * 1024;
constexpr std::size_t kWorkerCount = 4;
constexpr std::size_t kMaxPending = 16;
constexpr std::size_t kMaxTypedBatchItems = 64;
constexpr std::size_t kMaxFetchTimings = 8192;
std::uint64_t fetch_now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct ScopeExit {
  std::function<void()> action;
  ~ScopeExit() { if (action) action(); }
};

struct PreparedPublicationGuard {
  PreparedSelectorRegistry* registry;
  uid_t uid;
  const std::string& id;
  const PreparedSelectorRegistry::Payload& payload;
  bool published{false};
  ~PreparedPublicationGuard() {
    if (!published) registry->erase(uid, id, payload);
  }
};

// The owner context pins all frozen candidates, including predicate misses.
// This view is borrowed only for the synchronous authorization callback.
const std::vector<QuerySourceReference>& authorization_sources(
    const CatalogSelection& selection, std::vector<QuerySourceReference>& owned) {
  if (selection.read_context) return selection.read_context->sources();
  owned.reserve(selection.versions().size());
  for (const auto& manifest : selection.versions())
    owned.push_back({manifest.key.path, manifest.key.version,
      manifest.format_id.value_or("")});
  return owned;
}

QueryStatus invalid(std::string message) {
  return QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Validation,
    std::move(message));
}
QueryStatus io_error(std::string message) {
  return QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage,
    std::move(message));
}

const ptree& required_child(const ptree& tree, const char* name) {
  auto child = tree.get_child_optional(name);
  if (!child) throw std::invalid_argument(std::string("missing ") + name);
  return *child;
}
std::string required_string(const ptree& tree, const char* name) {
  auto value = tree.get_optional<std::string>(name);
  if (!value) throw std::invalid_argument(std::string("missing ") + name);
  return *value;
}
std::string optional_string(const ptree& tree, const char* name,
    std::string fallback = {}) {
  return tree.get<std::string>(name, std::move(fallback));
}
template<class T> T required_number(const ptree& tree, const char* name) {
  const auto raw = required_string(tree, name);
  T value{};
  const auto parsed = std::from_chars(raw.data(), raw.data() + raw.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size())
    throw std::invalid_argument(std::string("invalid integer ") + name);
  return value;
}
template<class T> T optional_number(const ptree& tree, const char* name,
    T fallback) {
  if (!tree.get_optional<std::string>(name)) return fallback;
  return required_number<T>(tree, name);
}
bool optional_bool(const ptree& tree, const char* name, bool fallback) {
  if (!tree.get_optional<std::string>(name)) return fallback;
  const auto raw = required_string(tree, name);
  if (raw == "true") return true;
  if (raw == "false") return false;
  throw std::invalid_argument(std::string("invalid boolean ") + name);
}
bool required_bool(const ptree& tree, const char* name) {
  if (!tree.get_optional<std::string>(name))
    throw std::invalid_argument(std::string("missing ") + name);
  return optional_bool(tree, name, false);
}
double required_finite_double(const ptree& tree, const char* name) {
  const auto& node = required_child(tree, name);
  if (!node.empty()) throw std::invalid_argument(std::string("invalid finite number ") + name);
  const auto& raw = node.data();
  // Keep the decimal grammar independent of the process locale. strtod_l may
  // set ERANGE for a representable subnormal, which is valid Float64 input.
  std::size_t i = 0;
  if (i < raw.size() && raw[i] == '-') ++i;
  bool nonzero = false;
  auto digit = [](char c) {return c >= '0' && c <= '9';};
  auto consume_digit = [&] {nonzero = nonzero || raw[i] != '0'; ++i;};
  if (i == raw.size() || !digit(raw[i]))
    throw std::invalid_argument(std::string("invalid finite number ") + name);
  if (raw[i] == '0') ++i;
  else while (i < raw.size() && digit(raw[i])) consume_digit();
  if (i < raw.size() && raw[i] == '.') {
    ++i; const auto first = i;
    while (i < raw.size() && digit(raw[i])) consume_digit();
    if (first == i) throw std::invalid_argument(std::string("invalid finite number ") + name);
  }
  if (i < raw.size() && (raw[i] == 'e' || raw[i] == 'E')) {
    ++i; if (i < raw.size() && (raw[i] == '+' || raw[i] == '-')) ++i;
    const auto first = i;
    while (i < raw.size() && digit(raw[i])) ++i;
    if (first == i) throw std::invalid_argument(std::string("invalid finite number ") + name);
  }
  if (i != raw.size()) throw std::invalid_argument(std::string("invalid finite number ") + name);
  struct NumericLocale {
    locale_t value = newlocale(LC_NUMERIC_MASK, "C", nullptr);
    ~NumericLocale() {if (value) freelocale(value);}
  } locale;
  if (!locale.value) throw std::runtime_error("cannot create numeric parsing locale");
  char* end = nullptr; errno = 0;
  const double value = strtod_l(raw.c_str(), &end, locale.value);
  if (end != raw.c_str() + raw.size() || !std::isfinite(value) ||
      (value == 0 && nonzero) || (errno != 0 && errno != ERANGE))
    throw std::invalid_argument(std::string("invalid finite number ") + name);
  return value;
}

template<class T> std::optional<T> nullable_number(const ptree& tree,
    const char* name) {
  const auto raw = tree.get_optional<std::string>(name);
  if (!raw || *raw == "null") return std::nullopt;
  return required_number<T>(tree, name);
}
std::optional<std::string> nullable_string(const ptree& tree, const char* name) {
  const auto raw = tree.get_optional<std::string>(name);
  if (!raw || *raw == "null") return std::nullopt;
  return required_string(tree, name);
}
template<class T> std::vector<T> array(const ptree& tree, const char* name,
    T (*parse)(const ptree&)) {
  std::vector<T> result;
  auto children = tree.get_child_optional(name);
  if (!children) return result;
  for (const auto& item : *children) {
    if (!item.first.empty())
      throw std::invalid_argument(std::string(name) + " must be an array");
    result.push_back(parse(item.second));
  }
  return result;
}
std::string parse_string(const ptree& node) { return node.data(); }
std::uint64_t parse_u64(const ptree& node) {
  ptree wrapper;
  wrapper.put("value", node.data());
  return required_number<std::uint64_t>(wrapper, "value");
}
StepInterval parse_interval(const ptree& tree) {
  return {required_number<std::uint64_t>(tree, "begin"),
    required_number<std::uint64_t>(tree, "end")};
}
LogicalTypeId parse_type(std::string_view name) {
  if (name == "boolean") return LogicalTypeId::Boolean;
  if (name == "int32") return LogicalTypeId::Int32;
  if (name == "int64") return LogicalTypeId::Int64;
  if (name == "uint32") return LogicalTypeId::UInt32;
  if (name == "uint64") return LogicalTypeId::UInt64;
  if (name == "float32") return LogicalTypeId::Float32;
  if (name == "float64") return LogicalTypeId::Float64;
  if (name == "utf8") return LogicalTypeId::Utf8;
  if (name == "binary") return LogicalTypeId::Binary;
  if (name == "timestamp_ns") return LogicalTypeId::TimestampNs;
  throw std::invalid_argument("unknown logical type");
}
LogicalValue parse_value(const ptree& tree) {
  const auto type = parse_type(required_string(tree, "type"));
  if (optional_bool(tree, "null", false)) return LogicalValue::null(type);
  if (type == LogicalTypeId::Boolean)
    return LogicalValue::boolean(required_bool(tree, "value"));
  if (type == LogicalTypeId::Int32)
    return LogicalValue::int32(required_number<std::int32_t>(tree, "value"));
  if (type == LogicalTypeId::Int64)
    return LogicalValue::int64(required_number<std::int64_t>(tree, "value"));
  if (type == LogicalTypeId::UInt32)
    return LogicalValue::uint32(required_number<std::uint32_t>(tree, "value"));
  if (type == LogicalTypeId::UInt64)
    return LogicalValue::uint64(required_number<std::uint64_t>(tree, "value"));
  if (type == LogicalTypeId::TimestampNs)
    return LogicalValue::timestamp_ns(required_number<std::int64_t>(tree, "value"));
  if (type == LogicalTypeId::Utf8)
    return LogicalValue::utf8(required_string(tree, "value"));
  if (type == LogicalTypeId::Binary) {
    const auto hex = required_string(tree, "value_hex");
    if (hex.size() % 2) throw std::invalid_argument("binary hex has odd length");
    BinaryValue bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
      unsigned int byte = 0;
      const auto parsed = std::from_chars(hex.data() + i, hex.data() + i + 2,
        byte, 16);
      if (parsed.ec != std::errc{} || parsed.ptr != hex.data() + i + 2)
        throw std::invalid_argument("invalid binary hex");
      bytes.push_back(static_cast<std::uint8_t>(byte));
    }
    return LogicalValue::binary(std::move(bytes));
  }
  const double value = required_finite_double(tree, "value");
  if (type == LogicalTypeId::Float32) {
    if (value > std::numeric_limits<float>::max() ||
        value < -std::numeric_limits<float>::max())
      throw std::invalid_argument("float32 out of range");
    return LogicalValue::float32(static_cast<float>(value));
  }
  return LogicalValue::float64(value);
}
ComparisonOperator comparison(std::string_view name) {
  if (name == "eq") return ComparisonOperator::Equal;
  if (name == "ne") return ComparisonOperator::NotEqual;
  if (name == "lt") return ComparisonOperator::Less;
  if (name == "le") return ComparisonOperator::LessEqual;
  if (name == "gt") return ComparisonOperator::Greater;
  if (name == "ge") return ComparisonOperator::GreaterEqual;
  throw std::invalid_argument("unknown comparison operator");
}
QueryExpressionPtr parse_expression(const ptree& tree, unsigned depth = 0) {
  if (depth > 32) throw std::invalid_argument("expression nesting too deep");
  const auto kind = required_string(tree, "kind");
  if (kind == "compare") return make_comparison(
    {required_string(tree, "field")},
    comparison(required_string(tree, "operator")),
    parse_value(required_child(tree, "value")));
  if (kind == "null") return make_is_null({required_string(tree, "field")},
    optional_bool(tree, "negated", false));
  if (kind == "in") return make_in({required_string(tree, "field")},
    array<LogicalValue>(tree, "values", parse_value),
    optional_bool(tree, "negated", false));
  LogicalOperator operation;
  if (kind == "and") operation = LogicalOperator::And;
  else if (kind == "or") operation = LogicalOperator::Or;
  else if (kind == "not") operation = LogicalOperator::Not;
  else throw std::invalid_argument("unknown expression kind");
  std::vector<QueryExpressionPtr> operands;
  for (const auto& child : required_child(tree, "operands")) {
    if (!child.first.empty()) throw std::invalid_argument("operands must be an array");
    operands.push_back(parse_expression(child.second, depth + 1));
  }
  return make_logical(operation, std::move(operands));
}
SourceSelector parse_selector(const ptree& tree) {
  query_profile_count("selector.compiles");
  SourceSelector result;
  result.paths = array<PathScope>(tree, "paths", [](const ptree& item) {
    const auto kind = optional_string(item, "kind", "exact");
    PathScopeKind parsed;
    if (kind == "exact") parsed = PathScopeKind::Exact;
    else if (kind == "subtree") parsed = PathScopeKind::Subtree;
    else throw std::invalid_argument("unknown path scope");
    return PathScope{parsed, required_string(item, "path")};
  });
  query_profile_count("selector.input_path_items", result.paths.size());
  if (const auto versions = tree.get_child_optional("versions")) {
    const auto kind = optional_string(*versions, "kind", "latest");
    if (kind == "latest") result.versions = VersionSelection::latest();
    else if (kind == "all") result.versions = VersionSelection::all();
    else if (kind == "exact") result.versions = VersionSelection::exact_version(
      required_number<std::int64_t>(*versions, "exact"));
    else if (kind == "between") result.versions = VersionSelection::between(
      required_number<std::int64_t>(*versions, "lower_inclusive"),
      required_number<std::int64_t>(*versions, "upper_exclusive"));
    else throw std::invalid_argument("unknown version selection");
  }
  if (const auto formats = tree.get_child_optional("formats")) {
    const auto adapters = optional_string(*formats, "adapters", "all");
    if (adapters == "all") result.formats.adapters = AdapterSelectionKind::All;
    else if (adapters == "listed") result.formats.adapters = AdapterSelectionKind::Listed;
    else throw std::invalid_argument("unknown adapter selection");
    result.formats.adapter_ids = array<std::string>(*formats, "adapter_ids", parse_string);
    result.formats.include_byte_passthrough = optional_bool(*formats,
      "include_byte_passthrough", true);
  }
  if (const auto predicate = tree.get_child_optional("file_predicate"))
    result.file_predicate = parse_expression(*predicate);
  return result;
}

// The protocol validates each new declaration once; later selector executions
// borrow this immutable object and still authorize the actual request peer.
SourceSelector parse_catalog_selector(const ptree& selector) {
  require_hdf5_object_fields(selector, "catalog sources", {"paths", "versions", "formats", "file_predicate"});
  if (auto paths = selector.get_child_optional("paths")) {
    if (!paths->data().empty()) throw std::invalid_argument("catalog paths must be an array");
    for (const auto& path : *paths) {
      if (!path.first.empty()) throw std::invalid_argument("catalog paths must be an array");
      require_hdf5_object_fields(path.second, "catalog path", {"kind", "path"});
    }
  }
  if (auto versions = selector.get_child_optional("versions"))
    require_hdf5_object_fields(*versions, "catalog versions", {"kind", "exact", "lower_inclusive", "upper_exclusive"});
  if (auto formats = selector.get_child_optional("formats"))
    require_hdf5_object_fields(*formats, "catalog formats", {"adapters", "adapter_ids", "include_byte_passthrough"});
  return parse_selector(selector);
}

struct DatasetCatalogTemplate {
  DatasetCatalogRequest declaration;
  std::shared_ptr<HotQueryCache::Binding> binding;
};
struct FormatCatalogTemplate {
  QueryRequest declaration;
  std::shared_ptr<HotQueryCache::Binding> binding;
};
class ImmutableTypedDeclaration;
struct TypedSingleTemplate {
  std::shared_ptr<const ImmutableTypedDeclaration> declaration;
};
struct TypedBatchTemplate {
  std::vector<std::shared_ptr<const ImmutableTypedDeclaration>> declarations;
};
struct QueryTemplateEpoch {
  std::weak_ptr<const StructuredBlobStorage> storage;
  std::weak_ptr<const PublishedVersionCatalog> catalog;
  std::weak_ptr<const QueryModule> module;
  std::uint64_t storage_namespace{0};
};
// Every payload in this registry has this one layout. Check its variant before
// using a declaration, including at the pre-existing catalog endpoints.
struct PreparedFrontendTemplate {
  std::variant<DatasetCatalogTemplate, FormatCatalogTemplate,
    TypedSingleTemplate, TypedBatchTemplate> declaration;
  QueryTemplateEpoch epoch; // Used only by pure query declarations, never a proof.
};

QueryStatus missing_query_template() {
  return QueryStatus::failure(QueryError::SourceNotFound, QueryPhase::Validation,
    "query template ID was not found");
}
bool is_query_template(const PreparedFrontendTemplate& value) {
  return std::holds_alternative<TypedSingleTemplate>(value.declaration) ||
    std::holds_alternative<TypedBatchTemplate>(value.declaration);
}

// Diagnostics observe the actual request ledger; they do not replay fees or
// infer work from logical source counts. Prefixes are distinct from backend
// HDF5 hot_query.fee_summary counters, which remain unchanged.
void profile_metadata_stats(const char* prefix, const QueryMetadataStageSummaryStats& after,
    const QueryMetadataStageSummaryStats& before, std::uint64_t bytes) noexcept {
  try {
    auto count = [&](const char* name, std::uint64_t amount) {
      query_profile_count((std::string(prefix) + name).c_str(), amount);
    };
    count("fee_entries_visited", after.full_fee_entries_visited - before.full_fee_entries_visited);
    count("fee_entries_skipped", after.borrowed_fee_entries_skipped - before.borrowed_fee_entries_skipped);
    count("summary_stages", after.successful_stages - before.successful_stages);
    count("memo_hits", after.memo_hits - before.memo_hits);
    count("overlay_entries_inspected", after.overlay_entries_inspected - before.overlay_entries_inspected);
    count("collector_exits", after.collector_exits - before.collector_exits);
    count("fallbacks", after.fallbacks - before.fallbacks);
    count("consumed_bytes", bytes);
  } catch (...) {} // Optional diagnostics never change query semantics.
}
class RequestMetadataPhaseProfile {
public:
  explicit RequestMetadataPhaseProfile(const char* prefix) noexcept : prefix_(prefix) {
    if (!active_query_profile) return;
    try {
      budget_ = current_query_metadata_budget();
      if (budget_) { before_ = budget_->stage_summary_stats(); bytes_ = budget_->consumed_bytes(); }
    } catch (...) { budget_.reset(); }
  }
  ~RequestMetadataPhaseProfile() { finish(); }
  void finish() noexcept {
    if (!budget_) return;
    try {
      profile_metadata_stats(prefix_, budget_->stage_summary_stats(), before_,
        budget_->consumed_bytes() - bytes_);
    } catch (...) {}
    budget_.reset();
  }
private:
  const char* prefix_;
  std::shared_ptr<QueryMetadataBudget> budget_;
  QueryMetadataStageSummaryStats before_;
  std::uint64_t bytes_{0};
};

std::uint64_t catalog_selector_reservation(const ptree& specification) {
  // Reserve before PathScopeSet allocates its immutable original/normalized
  // strings, key, order vector and temporary set. The JSON tree is bounded by
  // the existing frame limit; this separate bound covers cache promotion.
  std::uint64_t bytes = PreparedSelectorRegistry::entry_overhead_bytes + 16384;
  auto add = [&](std::uint64_t value, std::uint64_t factor) {
    if (value > (std::numeric_limits<std::uint64_t>::max() - bytes) / factor)
      throw std::length_error("compiled selector size overflow");
    bytes += value * factor;
  };
  if (auto paths = specification.get_child_optional("sources.paths"))
    for (const auto& item : *paths) { add(1, 256); add(required_string(item.second, "path").size(), 8); }
  if (auto formats = specification.get_child_optional("sources.formats.adapter_ids"))
    for (const auto& item : *formats) { add(1, 256); add(item.second.data().size(), 8); }
  if (auto path = specification.get_optional<std::string>("dataset_path")) add(path->size(), 8);
  if (auto format = specification.get_optional<std::string>("format_id")) add(format->size(), 8);
  if (auto projection = specification.get_child_optional("subset.projection"))
    for (const auto& item : *projection) { add(1, 256); add(item.second.data().size(), 8); }
  return bytes;
}
DomainBound parse_bound(const ptree& tree) {
  const auto kind = optional_string(tree, "kind", "inclusive");
  BoundKind parsed;
  if (kind == "inclusive") parsed = BoundKind::Inclusive;
  else if (kind == "exclusive") parsed = BoundKind::Exclusive;
  else throw std::invalid_argument("unknown bound kind");
  return {parse_value(required_child(tree, "value")), parsed};
}
DomainSlice parse_slice(const ptree& tree) {
  DomainSlice result;
  result.dimension = {required_string(tree, "dimension")};
  if (const auto lower = tree.get_child_optional("lower")) result.lower = parse_bound(*lower);
  if (const auto upper = tree.get_child_optional("upper")) result.upper = parse_bound(*upper);
  return result;
}
QueryLimits parse_query_limits(const ptree& tree) {
  QueryLimits result;
#define LIMIT(name) result.name = optional_number<decltype(result.name)>(tree, #name, result.name)
  LIMIT(max_sources); LIMIT(max_ranges); LIMIT(max_layout_bytes);
  LIMIT(max_payload_bytes); LIMIT(max_result_rows); LIMIT(target_batch_rows);
  LIMIT(target_batch_bytes);
#undef LIMIT
  return result;
}

void require_object_fields(const ptree& tree, std::string_view location,
    std::initializer_list<std::string_view> allowed) {
  if (!tree.data().empty())
    throw std::invalid_argument(std::string(location) + " must be an object");
  std::set<std::string> seen;
  for (const auto& child : tree) {
    if (std::find(allowed.begin(), allowed.end(), child.first) == allowed.end())
      throw std::invalid_argument("unexpected " + std::string(location) +
        " field: " + child.first);
    if (!seen.insert(child.first).second)
      throw std::invalid_argument("duplicate " + std::string(location) +
        " field: " + child.first);
  }
}
void require_hdf5_object_fields(const ptree& tree, std::string_view location,
    std::initializer_list<std::string_view> allowed) {
  require_object_fields(tree, "HDF5 " + std::string(location), allowed);
}

void validate_hdf5_subset_json(const ptree& subset) {
  // Property-tree parsing otherwise discards unrecognized keys. For HDF5's
  // unit-stride contract, silently ignoring stride/block/point/union syntax
  // would execute a different selection. Keep this strictness format-local.
  require_hdf5_object_fields(subset, "subset",
    {"projection", "predicate", "slices", "physical_array", "output"});
  if (const auto slices = subset.get_child_optional("slices")) {
    if (!slices->data().empty())
      throw std::invalid_argument("HDF5 slices must be an array");
    for (const auto& child : *slices) {
      require_hdf5_object_fields(child.second, "slice", {"dimension", "lower", "upper"});
      for (const auto* name : {"lower", "upper"}) {
        if (const auto bound = child.second.get_child_optional(name)) {
          require_hdf5_object_fields(*bound, "bound", {"kind", "value"});
          require_hdf5_object_fields(required_child(*bound, "value"), "bound value",
            {"type", "value", "null"});
        }
      }
    }
  }
  if (const auto output = subset.get_child_optional("output"))
    require_hdf5_object_fields(*output, "output", {"mode", "type_overrides", "interpolation"});
}

void validate_hdf5_registration_json(const ptree& root, const ptree& request) {
  require_hdf5_object_fields(root, "registration", {"kind", "request", "hdf5_binding"});
  require_hdf5_object_fields(request, "request", {"sources", "subset", "limits"});
  const auto& sources = required_child(request, "sources");
  require_hdf5_object_fields(sources, "sources", {"paths", "versions", "formats", "file_predicate"});
  if (const auto paths = sources.get_child_optional("paths")) {
    for (const auto& path : *paths)
      require_hdf5_object_fields(path.second, "source path", {"kind", "path"});
  }
  if (const auto versions = sources.get_child_optional("versions"))
    require_hdf5_object_fields(*versions, "source versions", {"kind", "exact"});
  if (const auto formats = sources.get_child_optional("formats"))
    require_hdf5_object_fields(*formats, "source formats", {"adapters", "adapter_ids", "include_byte_passthrough"});
  if (const auto limits = request.get_child_optional("limits"))
    require_hdf5_object_fields(*limits, "query limits", {"max_sources", "max_ranges", "max_layout_bytes", "max_payload_bytes", "max_result_rows", "target_batch_rows", "target_batch_bytes"});
  if (const auto subset = request.get_child_optional("subset"))
    validate_hdf5_subset_json(*subset);
  if (const auto binding = root.get_child_optional("hdf5_binding"))
    require_hdf5_object_fields(*binding, "binding", {"profile", "dataset_path"});
}

QueryRequest parse_query_request(const ptree& tree) {
  QueryRequest result;
  result.sources = parse_selector(required_child(tree, "sources"));
  if (const auto subset = tree.get_child_optional("subset")) {
    QueryFragment fragment;
    auto fields = array<std::string>(*subset, "projection", parse_string);
    for (auto& field : fields) fragment.projection.push_back({std::move(field)});
    if (const auto predicate = subset->get_child_optional("predicate"))
      fragment.predicate = parse_expression(*predicate);
    fragment.slices = array<DomainSlice>(*subset, "slices", parse_slice);
    if (const auto physical = subset->get_child_optional("physical_array")) {
      PhysicalArraySubject subject;
      subject.field_id = required_string(*physical, "field_id");
      subject.record_batch_index = required_number<std::uint64_t>(*physical,
        "record_batch_index");
      subject.row_index = required_number<std::uint64_t>(*physical, "row_index");
      subject.values = array<ArrayValuePath>(*physical, "values", [](const ptree& item) {
        return ArrayValuePath{array<std::uint64_t>(item, "child_indices", parse_u64)};
      });
      fragment.physical_array = std::move(subject);
    }
    if (const auto output = subset->get_child_optional("output")) {
      const auto mode = optional_string(*output, "mode", "logical_values");
      if (mode == "binary_view") fragment.output.mode = QueryFragment::ResultMode::BinaryView;
      else if (mode != "logical_values") throw std::invalid_argument("unknown output mode");
      fragment.output.type_overrides = array<QueryFragment::FieldTypeOverride>(
        *output, "type_overrides", [](const ptree& item) {
          return QueryFragment::FieldTypeOverride{
            {required_string(item, "field")}, parse_type(required_string(item, "type"))};
        });
      if (const auto interpolation = output->get_child_optional("interpolation")) {
        QueryFragment::GridInterpolation spec;
        const auto method = optional_string(*interpolation, "method", "bilinear");
        if (method != "bilinear") throw std::invalid_argument("unknown interpolation method");
        const auto border = optional_string(*interpolation, "border", "clamp");
        if (border == "nan") spec.border = QueryFragment::BorderMode::NaN;
        else if (border != "clamp") throw std::invalid_argument("unknown border mode");
        spec.axes = array<QueryFragment::InterpolationAxis>(*interpolation, "axes",
          [](const ptree& item) {
            QueryFragment::InterpolationAxis axis;
            axis.dimension = {required_string(item, "dimension")};
            axis.count = nullable_number<std::uint64_t>(item, "count");
            if (const auto spacing = item.get_child_optional("spacing"))
              axis.spacing = parse_value(*spacing);
            return axis;
          });
        fragment.output.interpolation = std::move(spec);
      }
    }
    if (has_hdf5_query_fields(fragment)) validate_hdf5_subset_json(*subset);
    result.subset = std::move(fragment);
  }
  if (const auto limits = tree.get_child_optional("limits"))
    result.limits = parse_query_limits(*limits);
  return result;
}

// A closed, source-free syntax certificate. No mutable request or ptree view
// escapes its factory. In particular, this is neither an Adapter plan nor an
// authorization result; execute still runs the complete ordinary planner.
class ImmutableTypedDeclaration final {
public:
  const QueryRequest& request() const { return request_; }
  static QueryResult<std::shared_ptr<const ImmutableTypedDeclaration>> parse(
      const ptree& root, bool batch_item) {
    using Result = QueryResult<std::shared_ptr<const ImmutableTypedDeclaration>>;
    if (required_string(root, "kind") != "typed")
      return Result::failure(invalid("query template items must be typed declarations"));
    const auto& spec = required_child(root, "request");
    auto request = [&] {
      QueryProfileTimer timer("request_parse");
      query_profile_count("query_template.declaration_parses");
      return parse_query_request(spec);
    }();
    const auto strict = [&] {
      if (root.get_child_optional("hdf5_binding") ||
          (request.subset && has_hdf5_query_fields(*request.subset)))
        validate_hdf5_registration_json(root, spec);
    };
    // Preserve the two existing predeclaration orders, rather than using one
    // generic-first parser for both single and batch registrations.
    if (!batch_item) strict();
    auto valid = [&] {
      QueryProfileTimer timer("request_validate");
      return validate_query_request(request);
    }();
    if (!valid.ok()) return Result::failure(valid);
    if (batch_item) strict();
    if (root.get_child_optional("hdf5_binding"))
      return Result::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
        QueryPhase::Validation, "query templates do not support custom HDF5 bindings"));
    const bool supported_version = request.sources.versions.kind == VersionSelectionKind::Exact ||
      (!batch_item && request.sources.versions.kind == VersionSelectionKind::Latest);
    if (request.sources.paths.empty() ||
        std::any_of(request.sources.paths.begin(), request.sources.paths.end(),
          [](const auto& path) { return path.kind != PathScopeKind::Exact; }) ||
        !supported_version)
      return Result::failure(invalid(batch_item ?
        "typed batch requires exact source paths and versions" :
        "query templates require exact paths and exact or latest versions"));
    return Result::success(std::shared_ptr<const ImmutableTypedDeclaration>(
      new ImmutableTypedDeclaration(std::move(request))));
  }
private:
  explicit ImmutableTypedDeclaration(QueryRequest request) : request_(std::move(request)) {}
  const QueryRequest request_;
};

std::uint64_t query_template_reservation(const ptree& declaration) {
  // Cover every retained AST/container/control block, not merely selector
  // strings. Normalized paths and identity strings are covered by the byte
  // multiplier. This deliberately overestimates; a large wire frame is not a
  // promise that its expanded immutable template fits the 1 MiB item quota.
  std::uint64_t bytes = PreparedSelectorRegistry::entry_overhead_bytes + 16384;
  auto add = [&](std::uint64_t count, std::uint64_t factor) {
    if (count > (std::numeric_limits<std::uint64_t>::max() - bytes) / factor)
      throw std::length_error("query template size overflow");
    bytes += count * factor;
  };
  std::vector<const ptree*> pending{&declaration};
  while (!pending.empty()) {
    const auto* node = pending.back(); pending.pop_back();
    add(1, 512); add(node->data().size(), 16);
    for (const auto& child : *node) {
      add(child.first.size(), 16);
      pending.push_back(&child.second);
    }
  }
  return bytes;
}

TrainingTimeAxis parse_time_axis(const ptree& tree) {
  TrainingTimeAxis axis;
  const auto kind = optional_string(tree, "kind", "step");
  if (kind == "step") axis.kind = TrainingTimeKind::Step;
  else if (kind == "regular_ns") axis.kind = TrainingTimeKind::RegularNanoseconds;
  else if (kind == "explicit_ns") axis.kind = TrainingTimeKind::ExplicitNanoseconds;
  else throw std::invalid_argument("unknown time axis kind");
  axis.start_ns = optional_number<std::int64_t>(tree, "start_ns", 0);
  axis.cadence_ns = optional_number<std::int64_t>(tree, "cadence_ns", 0);
  axis.timezone = optional_string(tree, "timezone");
  axis.timestamps_ns = array<std::int64_t>(tree, "timestamps_ns", [](const ptree& item) {
    ptree wrapper; wrapper.put("value", item.data());
    return required_number<std::int64_t>(wrapper, "value");
  });
  return axis;
}
SnapshotChannelBinding parse_snapshot_channel(const ptree& tree) {
  SnapshotChannelBinding channel;
  channel.field_id = required_string(tree, "field_id");
  channel.original_ordinal = required_number<std::uint64_t>(tree, "original_ordinal");
  channel.physical_ordinal = nullable_number<std::uint64_t>(tree, "physical_ordinal");
  channel.label = optional_string(tree, "label");
  channel.unit = nullable_string(tree, "unit");
  channel.length = required_number<std::uint64_t>(tree, "length");
  return channel;
}
SnapshotRowBinding parse_snapshot_row(const ptree& tree) {
  SnapshotRowBinding row;
  row.logical_row = required_number<std::uint64_t>(tree, "logical_row");
  row.shard_index = required_number<std::size_t>(tree, "shard_index");
  row.batch_index = optional_number<std::uint64_t>(tree, "batch_index", 0);
  row.row_index = optional_number<std::uint64_t>(tree, "row_index", 0);
  row.series_id = required_string(tree, "series_id");
  row.entity_id = optional_string(tree, "entity_id");
  row.steps = parse_interval(required_child(tree, "steps"));
  if (const auto time = tree.get_child_optional("time")) row.time = parse_time_axis(*time);
  row.target_field = optional_string(tree, "target_field", "target");
  row.declared_n_channels = nullable_number<std::uint64_t>(tree, "declared_n_channels");
  row.declared_length = nullable_number<std::uint64_t>(tree, "declared_length");
  const auto value_layout = optional_string(tree, "value_layout", "channel_major");
  if (value_layout == "step_major") row.value_layout = SnapshotValueLayout::StepMajorFlat;
  else if (value_layout != "channel_major") throw std::invalid_argument("invalid snapshot value_layout");
  if (const auto channels = tree.get_child_optional("channels"); channels && channels->size() > 4096)
    throw std::invalid_argument("row channels exceed 4096");
  row.channels = array<SnapshotChannelBinding>(tree, "channels", parse_snapshot_channel);
  return row;
}
DatasetSnapshotRequest parse_snapshot(const ptree& tree) {
  DatasetSnapshotRequest snapshot;
  snapshot.dataset_id = required_string(tree, "dataset_id");
  snapshot.selector = parse_selector(required_child(tree, "selector"));
  snapshot.shards = array<SnapshotShard>(tree, "shards", [](const ptree& item) {
    SnapshotShard shard;
    shard.filename = required_string(item, "filename");
    shard.key.path = required_string(item, "path");
    shard.key.version = required_number<std::int64_t>(item, "version");
    return shard;
  });
  struct ChannelTemplate { SnapshotChannelBinding channel; bool inherits_length; };
  struct ChannelTable { std::vector<ChannelTemplate> channels; std::uint64_t copy_bytes; };
  // Account templates and distinct resolved immutable tables once; rows
  // share the table while logical coverage counts remain separately bounded.
  // The conservative charge includes vector growth and allocator overhead;
  // it is separate from the prepared module's final metadata admission.
  constexpr std::uint64_t max_channel_bytes = 64ULL * 1024 * 1024;
  std::uint64_t expanded_channel_bytes = 0;
  auto charge_channels = [&](std::uint64_t bytes) {
    if (bytes > max_channel_bytes - expanded_channel_bytes)
      throw std::invalid_argument("expanded channel bytes exceed 64 MiB");
    expanded_channel_bytes += bytes;
  };
  auto channel_bytes = [](const SnapshotChannelBinding& channel) -> std::uint64_t {
    return 2 * sizeof(ChannelTemplate) + 64 +
      channel.field_id.capacity() + 33 + channel.label.capacity() + 33 +
      (channel.unit ? channel.unit->capacity() + 33 : 0);
  };
  std::unordered_map<std::string, ChannelTable> tables;
  if (const auto definitions = tree.get_child_optional("channel_tables")) {
    if (!definitions->data().empty()) throw std::invalid_argument("channel_tables must be an array");
    for (const auto& definition : *definitions) {
      if (!definition.first.empty()) throw std::invalid_argument("channel_tables must be an array");
      const auto id = required_string(definition.second, "id");
      const auto& channels = required_child(definition.second, "channels");
      if (id.empty() || channels.empty() || channels.size() > 4096 || !channels.data().empty())
        throw std::invalid_argument("invalid channel table id or channel count");
      std::vector<ChannelTemplate> parsed;
      std::uint64_t copy_bytes = 0;
      std::set<std::pair<std::string, std::uint64_t>> ordinals;
      for (const auto& item : channels) {
        if (!item.first.empty()) throw std::invalid_argument("table channels must be an array");
        auto declaration = item.second;
        const bool inherits = !declaration.get_child_optional("length");
        if (inherits) declaration.put("length", 0);
        auto channel = parse_snapshot_channel(declaration);
        if (channel.field_id.empty() || !ordinals.emplace(channel.field_id, channel.original_ordinal).second)
          throw std::invalid_argument("invalid or duplicate table channel");
        const auto bytes = channel_bytes(channel);
        charge_channels(bytes);
        copy_bytes += bytes;
        parsed.push_back({std::move(channel), inherits});
      }
      if (!tables.emplace(id, ChannelTable{std::move(parsed), copy_bytes}).second)
        throw std::invalid_argument("duplicate channel table id");
    }
  }
  std::size_t expanded_channels = 0;
  const auto& rows = required_child(tree, "rows");
  if (!rows.data().empty()) throw std::invalid_argument("rows must be an array");
  std::vector<std::pair<const ptree*, const std::vector<ChannelTemplate>*>> expansion;
  expansion.reserve(rows.size());
  for (const auto& item : rows) {
    if (!item.first.empty()) throw std::invalid_argument("rows must be an array");
    const auto& row_tree = item.second;
    const auto table_ref = row_tree.get_child_optional("channel_table_id");
    if (table_ref && row_tree.get_child_optional("channels"))
      throw std::invalid_argument("row channels and channel_table_id are mutually exclusive");
    const std::vector<ChannelTemplate>* table = nullptr;
    std::size_t count = 0;
    if (table_ref) {
      if (!table_ref->empty()) throw std::invalid_argument("channel_table_id must be a string");
      const auto found = tables.find(table_ref->data());
      if (found == tables.end()) throw std::invalid_argument("unknown channel table id");
      table = &found->second.channels;
      count = table->size();

    } else if (const auto channels = row_tree.get_child_optional("channels")) {
      count = channels->size();
      for (const auto& item : *channels) {
        // Explicit rows have no reference amplification, but consume the same
        // parser budget. Read scalar lengths without copying their strings.
        std::uint64_t bytes = 2 * sizeof(ChannelTemplate) + 64;
        for (const char* key : {"field_id", "label", "unit"})
          if (const auto value = item.second.get_child_optional(key)) bytes += value->data().size() + 33;
        charge_channels(bytes);
      }
    }
    if (count > 4096 || count > 1000000 - expanded_channels)
      throw std::invalid_argument("expanded channel count exceeds limit");
    expanded_channels += count;
    expansion.emplace_back(&row_tree, table);
  }
  std::map<std::pair<std::string, std::optional<std::uint64_t>>,
    std::shared_ptr<const std::vector<SnapshotChannelBinding>>> resolved;
  for (const auto& planned : expansion) {
    const auto& row_tree = *planned.first;
    const auto* table = planned.second;
    auto row = parse_snapshot_row(row_tree);
    if (table) {
      const bool inherits = std::any_of(table->begin(), table->end(),
        [](const ChannelTemplate& item) { return item.inherits_length; });
      if (inherits && !row.declared_length)
        throw std::invalid_argument("table channel length requires row declared_length");
      const auto key = std::make_pair(required_string(row_tree, "channel_table_id"),
        inherits ? row.declared_length : std::optional<std::uint64_t>{});
      auto found = resolved.find(key);
      if (found == resolved.end()) {
        auto channels = std::make_shared<std::vector<SnapshotChannelBinding>>();
        channels->reserve(table->size());
        for (const auto& item : *table) {
          auto channel = item.channel;
          if (item.inherits_length) channel.length = *row.declared_length;
          charge_channels(channel_bytes(channel));
          channels->push_back(std::move(channel));
        }
        found = resolved.emplace(key, std::move(channels)).first;
      }
      row.channel_table = found->second;
    }
    snapshot.rows.push_back(std::move(row));
  }
  snapshot.hf_state_json = nullable_string(tree, "hf_state_json");
  return snapshot;
}
TrainingChannelRef parse_channel_ref(const ptree& tree) {
  return {required_string(tree, "field_id"),
    required_number<std::uint64_t>(tree, "ordinal"),
    optional_string(tree, "label")};
}
MissingTreatment parse_treatment(std::string_view name) {
  if (name == "reject_window") return MissingTreatment::RejectWindow;
  if (name == "mask") return MissingTreatment::Mask;
  throw std::invalid_argument("unknown missing treatment");
}
TrainingViewSpec parse_view(const ptree& tree) {
  TrainingViewSpec view;
  view.view_id = required_string(tree, "view_id");
  view.experiment_id = optional_string(tree, "experiment_id");
  view.snapshot_fingerprint = optional_string(tree, "snapshot_fingerprint");
  view.input_channels = array<TrainingChannelRef>(tree, "input_channels", parse_channel_ref);
  view.target_channels = array<TrainingChannelRef>(tree, "target_channels", parse_channel_ref);
  view.input_length = required_number<std::uint64_t>(tree, "input_length");
  view.gap = optional_number<std::uint64_t>(tree, "gap", 0);
  view.target_length = optional_number<std::uint64_t>(tree, "target_length", 0);
  view.stride = required_number<std::uint64_t>(tree, "stride");
  const auto& split = required_child(tree, "split");
  view.split.split_id = required_string(split, "split_id");
  view.split.permitted_input = parse_interval(required_child(split, "permitted_input"));
  view.split.permitted_target = parse_interval(required_child(split, "permitted_target"));
  if (const auto missing = tree.get_child_optional("missing")) {
    view.missing.null_series = parse_treatment(optional_string(*missing,
      "null_series", "reject_window"));
    view.missing.missing_channel = parse_treatment(optional_string(*missing,
      "missing_channel", "reject_window"));
    view.missing.ragged_channel = parse_treatment(optional_string(*missing,
      "ragged_channel", "reject_window"));
    view.missing.null_value = parse_treatment(optional_string(*missing,
      "null_value", "reject_window"));
  }
  if (const auto transform = tree.get_child_optional("transform")) {
    const auto kind = optional_string(*transform, "kind", "identity");
    if (kind == "identity") view.transform.kind = TrainingTransformKind::Identity;
    else if (kind == "standardize") view.transform.kind = TrainingTransformKind::Standardize;
    else throw std::invalid_argument("unknown transform kind");
    view.transform.artifact_id = optional_string(*transform, "artifact_id");
    view.transform.statistics = array<TrainingChannelStatistics>(*transform,
      "statistics", [](const ptree& item) {
        TrainingChannelStatistics result;
        result.field_id = required_string(item, "field_id");
        result.ordinal = required_number<std::uint64_t>(item, "ordinal");
        result.mean = required_finite_double(item, "mean");
        result.stddev = required_finite_double(item, "stddev");
        return result;
      });
  }
  return view;
}
WindowRequest parse_window_request(const ptree& tree) {
  WindowRequest request;
  if (const auto anchors = tree.get_child_optional("anchors"))
    request.anchors = parse_interval(*anchors);
  request.coordinates = array<WindowCoordinate>(tree, "coordinates", [](const ptree& item) {
    WindowCoordinate result;
    result.series_id = required_string(item, "series_id");
    result.anchor = required_number<std::uint64_t>(item, "anchor");
    result.expected_sample_id = nullable_string(item, "expected_sample_id");
    return result;
  });
  request.max_windows = optional_number<std::uint64_t>(tree, "max_windows", 0);
  request.epoch = optional_number<std::uint64_t>(tree, "epoch", 0);
  request.seed = optional_number<std::uint64_t>(tree, "seed", 0);
  request.shard_index = optional_number<std::uint64_t>(tree, "shard_index", 0);
  request.shard_count = optional_number<std::uint64_t>(tree, "shard_count", 1);
  if (const auto continuation = tree.get_child_optional("continuation")) {
    WindowContinuation state;
    state.selection_identity = required_string(*continuation, "selection_identity");
    state.series_index = required_number<std::uint64_t>(*continuation, "series_index");
    state.next_anchor = required_number<std::uint64_t>(*continuation, "next_anchor");
    state.eligible_count = required_number<std::uint64_t>(*continuation, "eligible_count");
    request.continuation = std::move(state);
  }
  return request;
}
TrainingQueryLimits parse_training_limits(const ptree& tree) {
  TrainingQueryLimits result;
#define LIMIT(name) result.name = optional_number<std::uint64_t>(tree, #name, result.name)
  LIMIT(max_series); LIMIT(max_channels); LIMIT(max_steps_per_window);
  LIMIT(max_windows_per_batch); LIMIT(max_candidate_windows);
  LIMIT(max_result_bytes); LIMIT(max_provenance_spans);
#undef LIMIT
  return result;
}
QueryExecutionOptions parse_execution_options(const ptree& tree) {
  QueryExecutionOptions result;
#define OPTION(name) result.name = optional_number<decltype(result.name)>(tree, #name, result.name)
  OPTION(target_batch_rows); OPTION(target_batch_bytes); OPTION(max_ranges);
  OPTION(max_layout_bytes); OPTION(max_payload_bytes); OPTION(max_result_rows);
#undef OPTION
  return result;
}

bool read_exact(int fd, void* buffer, std::size_t size) {
  auto* bytes = static_cast<unsigned char*>(buffer);
  while (size) {
    const auto count = ::read(fd, bytes, size);
    if (count > 0) { bytes += count; size -= static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}
bool write_exact(int fd, const void* buffer, std::size_t size) {
  const auto* bytes = static_cast<const unsigned char*>(buffer);
  while (size) {
#ifdef MSG_NOSIGNAL
    const auto count = ::send(fd, bytes, size, MSG_NOSIGNAL);
#else
    const auto count = ::write(fd, bytes, size);
#endif
    if (count > 0) { bytes += count; size -= static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}
bool read_frame(int fd, std::string& payload) {
  std::uint32_t size_network = 0;
  if (!read_exact(fd, &size_network, sizeof(size_network))) return false;
  const auto size = ntohl(size_network);
  if (size == 0 || size > kMaxFrame) return false;
  payload.resize(size);
  return read_exact(fd, payload.data(), size);
}
bool write_frame(int fd, const std::string& payload) {
  if (payload.empty() || payload.size() > kMaxFrame) return false;
  const auto size = htonl(static_cast<std::uint32_t>(payload.size()));
  return write_exact(fd, &size, sizeof(size)) &&
    write_exact(fd, payload.data(), payload.size());
}
std::string json_quote(std::string_view raw) {
    std::string encoded = "\"";
    constexpr char hex[] = "0123456789abcdef";
    for (const unsigned char byte : raw) {
      if (byte == '"' || byte == '\\') {
        encoded.push_back('\\'); encoded.push_back(static_cast<char>(byte));
      } else if (byte < 0x20) {
        encoded += "\\u00";
        encoded.push_back(hex[byte >> 4]);
        encoded.push_back(hex[byte & 15]);
      } else encoded.push_back(static_cast<char>(byte));
    }
    encoded.push_back('"');
    return encoded;
}
std::string json_response(const QueryResult<QueryControlResponse>& result) {
  if (result.ok()) {
    const auto& response = result.value();
    std::string json = "{\"ok\":true,\"kind\":";
    switch (response.kind) {
    case QueryControlResponse::Kind::PublishedRoute:
      return json + "\"published_route\",\"path\":" + json_quote(response.path) + "}";
    case QueryControlResponse::Kind::PublishedRoutes: {
      json += "\"published_routes\",\"items\":[";
      for (std::size_t i = 0; i < response.items.size(); ++i) {
        if (i) json += ',';
        json += "{\"index\":" + std::to_string(response.items[i].index) +
          ",\"path\":" + json_quote(response.items[i].path) + "}";
      }
      return json + "]}";
    }
    case QueryControlResponse::Kind::PreparedWindow:
      return json + "\"prepared_window\",\"prepared_id\":" + json_quote(response.prepared_id) +
        ",\"snapshot_fingerprint\":" + json_quote(response.snapshot_fingerprint) +
        ",\"ttl_ms\":" + std::to_string(response.ttl_ms) + "}";
    case QueryControlResponse::Kind::PreparedCatalogSelector:
      return json + "\"prepared_catalog_selector\",\"selector_id\":" + json_quote(response.selector_id) +
        ",\"idle_ttl_ms\":" + std::to_string(response.ttl_ms) + "}";
    case QueryControlResponse::Kind::ReleasedCatalogSelector:
      return json + "\"released_catalog_selector\",\"released\":" +
        (response.released ? "true}" : "false}");
    case QueryControlResponse::Kind::PreparedQueryTemplate:
      return json + "\"prepared_query_template\",\"template_id\":" + json_quote(response.template_id) +
        ",\"idle_ttl_ms\":" + std::to_string(response.ttl_ms) + "}";
    case QueryControlResponse::Kind::ReleasedQueryTemplate:
      return json + "\"released_query_template\",\"released\":" +
        (response.released ? "true}" : "false}");
    case QueryControlResponse::Kind::FormatCatalog: {
      json += "\"format_catalog\",\"units\":[";
      bool first = true;
      for (const auto& item : response.format_units.units) {
        if (!first) json += ',';
        first = false;
        json += "{\"path\":" + json_quote(item.source.path) +
          ",\"version\":" + std::to_string(item.source.version) +
          ",\"format_id\":" + json_quote(item.source.format_id) +
          ",\"revision\":" + std::to_string(item.revision) +
          ",\"unit_id\":" + json_quote(item.unit.unit_id) +
          ",\"offset\":" + std::to_string(item.unit.offset) +
          ",\"length\":" + std::to_string(item.unit.length) + "}";
      }
      return json + "]}";
    }
    case QueryControlResponse::Kind::DatasetCatalog: {
      const auto& catalog = response.datasets;
      json += "\"dataset_catalog\",\"selected_sources\":" + std::to_string(catalog.selected_sources) +
        ",\"time_candidate_sources\":" + std::to_string(catalog.time_candidate_sources) +
        ",\"candidate_datasets\":" + std::to_string(catalog.candidate_datasets) +
        ",\"sql_queries\":" + std::to_string(catalog.sql_queries) +
        ",\"catalog_read_bytes\":" + std::to_string(catalog.catalog_read_bytes) + ",\"datasets\":[";
      bool first = true;
      for (const auto& item : catalog.datasets) {
        if (!first) json += ',';
        first = false;
        json += "{\"path\":" + json_quote(item.source.path) +
          ",\"version\":" + std::to_string(item.source.version) +
          ",\"format_id\":" + json_quote(item.source.format_id) +
          ",\"revision\":" + std::to_string(item.revision) +
          ",\"object_id\":" + json_quote(item.object_id) +
          ",\"dataset_path\":" + json_quote(item.dataset_path) +
          ",\"layout_digest\":" + json_quote(item.layout_digest) +
          ",\"layout_protocol\":" + json_quote(item.layout_protocol) + "}";
      }
      return json + "]}";
    }
    case QueryControlResponse::Kind::ReleasedWindow:
    case QueryControlResponse::Kind::ReleasedResult:
      return json + (response.kind == QueryControlResponse::Kind::ReleasedWindow ?
        "\"released_window\"" : "\"released_result\"") + ",\"released\":" +
        (response.released ? "true}" : "false}");
    }
  }
  return "{\"ok\":false,\"code\":" +
    std::to_string(static_cast<int>(result.status().code())) +
    ",\"phase\":" +
    std::to_string(static_cast<int>(result.status().phase())) +
    ",\"message\":" + json_quote(result.status().message()) + "}";
}

std::string runtime_delta_json(const QueryRuntimeStats& before,
    const QueryRuntimeStats& after) {
  std::ostringstream out;
  out << '{';
  bool first = true;
  auto add = [&](const char* name, std::uint64_t value) {
    if (!first) out << ',';
    first = false;
    out << json_quote(name) << ':' << value;
  };
#define PROFILE_DELTA(name) add(#name, after.name - before.name)
  PROFILE_DELTA(hits); PROFILE_DELTA(misses); PROFILE_DELTA(waits);
  PROFILE_DELTA(producer_count); PROFILE_DELTA(decode_count);
  PROFILE_DELTA(decoded_bytes); PROFILE_DELTA(decompressions);
  PROFILE_DELTA(read_bytes); PROFILE_DELTA(read_ranges);
  PROFILE_DELTA(copy_bytes); PROFILE_DELTA(evictions); PROFILE_DELTA(redecodes);
  PROFILE_DELTA(payload_read_calls); PROFILE_DELTA(payload_read_nanoseconds);
  PROFILE_DELTA(payload_read_bytes); PROFILE_DELTA(payload_read_ranges);
  PROFILE_DELTA(fragment_load_nanoseconds);
  PROFILE_DELTA(fragment_queue_nanoseconds); PROFILE_DELTA(fragment_admission_nanoseconds);
  PROFILE_DELTA(fragment_memory_wait_nanoseconds);
  PROFILE_DELTA(decode_filter_nanoseconds); PROFILE_DELTA(decode_inflate_nanoseconds);
  PROFILE_DELTA(decode_shuffle_nanoseconds); PROFILE_DELTA(decode_checksum_nanoseconds);
  PROFILE_DELTA(decode_conversion_nanoseconds);
  PROFILE_DELTA(decode_nanoseconds); PROFILE_DELTA(decode_cpu_nanoseconds);
  PROFILE_DELTA(decode_cpu_samples);
#undef PROFILE_DELTA
  out << '}';
  return out.str();
}

std::string iotdb_profile_delta_json(const IoTDBProfileSnapshot& before,
    const IoTDBProfileSnapshot& after) {
  std::ostringstream out;
  out << '{';
  bool first = true;
#define ADD(name) if (!first) out << ','; first = false; out << json_quote(#name) << ':' << after.name - before.name;
  IOTDBFS_IOTDB_PROFILE_COUNTERS(ADD)
#undef ADD
  out << '}';
  return out.str();
}

std::string payload_shape_delta_json(const QueryRuntimeStats& before,
    const QueryRuntimeStats& after) {
  std::ostringstream out;
  out << '[';
  bool first = true;
  for (std::size_t index = 0; index < after.payload_read_shape.size(); ++index) {
    const auto& old = before.payload_read_shape[index];
    const auto& now = after.payload_read_shape[index];
    if (now.calls == old.calls) continue;
    if (!first) out << ',';
    first = false;
    out << "{\"range_bin\":" << index / 5
        << ",\"byte_bin\":" << index % 5
        << ",\"calls\":" << now.calls - old.calls
        << ",\"nanoseconds\":" << now.nanoseconds - old.nanoseconds
        << ",\"bytes\":" << now.bytes - old.bytes
        << ",\"ranges\":" << now.ranges - old.ranges << '}';
  }
  out << ']';
  return out.str();
}

std::string query_profile_json(std::uint64_t sequence, std::size_t request_bytes,
    const QueryResult<QueryControlResponse>& result, const QueryProfileRecord& record,
    const std::string& operation, const std::string& prepared_id,
    const QueryRuntimeStats& before, const QueryRuntimeStats& after,
    const HotQueryCacheStats& hot_before, const HotQueryCacheStats& hot_after,
    const IoTDBProfileSnapshot& iotdb_before, const IoTDBProfileSnapshot& iotdb_after,
    const QueryRuntimeConfig& runtime_config,
    const TrainingExecutionOptions& training_execution,
    const Hdf5QueryKernelConfig& hdf5_execution, Hdf5QueryMode hdf5_mode,
    std::size_t result_sender_threads,
    std::chrono::steady_clock::time_point start,
    std::chrono::steady_clock::time_point end) {
  auto ns = [](auto duration) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
  };
  std::ostringstream out;
  out << "{\"protocol\":\"iotdbfs-query-profile/1\",\"pid\":" << ::getpid()
      << ",\"sequence\":" << sequence << ",\"request_bytes\":" << request_bytes
      << ",\"ok\":" << (result.ok() ? "true" : "false")
      << ",\"operation\":" << json_quote(operation)
      << ",\"prepared_id\":" << json_quote(prepared_id)
      << ",\"steady_start_ns\":" << ns(start.time_since_epoch())
      << ",\"steady_end_ns\":" << ns(end.time_since_epoch())
      << ",\"server_total_ns\":" << ns(end - start)
      << ",\"runtime_config\":{\"memory_budget_bytes\":" << runtime_config.memory_budget_bytes
      << ",\"max_entries\":" << runtime_config.max_entries
      << ",\"max_inflight\":" << runtime_config.max_inflight
      << ",\"worker_threads\":" << runtime_config.worker_threads
      << ",\"query_task_workers\":" << runtime_config.query_tasks.worker_threads
      << ",\"query_task_max_queued\":" << runtime_config.query_tasks.max_queued_tasks
      << ",\"query_task_max_batch\":" << runtime_config.query_tasks.max_batch_size
      << ",\"result_sender_threads\":" << result_sender_threads
      << "},\"server_training_execution\":{\"read_parallelism\":" << training_execution.read_parallelism
      << ",\"reuse_prepared_validation\":" << (training_execution.reuse_prepared_validation ? "true" : "false")
      << ",\"compiled_transform\":" << (training_execution.compiled_transform ? "true" : "false")
      << "},\"server_hdf5_execution\":{\"mode\":" << json_quote(hdf5_query_mode_name(hdf5_mode))
      << ",\"payload_backend\":" << json_quote(hdf5_mode == Hdf5QueryMode::NativeHdf5 ? "filesystem" : "database")
      << ",\"read_parallelism\":" << hdf5_execution.read_parallelism
      << ",\"native_cache_enabled\":" << (hdf5_execution.native_session_cache_enabled ? "true" : "false")
      << ",\"native_cache_files\":" << (hdf5_execution.native_session_cache_enabled ? 8 : 0)
      << ",\"native_cache_datasets\":" << (hdf5_execution.native_session_cache_enabled ? 8 : 0)
      << ",\"native_metadata_bytes_per_file\":" << (hdf5_execution.native_session_cache_enabled ? 2 * 1024 * 1024 : 8 * 1024 * 1024)
      << ",\"native_raw_chunk_bytes_per_dataset\":" << (hdf5_execution.native_session_cache_enabled ? 256 * 1024 : 0)
      << ",\"native_pool_limit_bytes\":" << (hdf5_execution.native_session_cache_enabled ? 64 * 1024 * 1024 : 0)
      << ",\"max_batch_fragments\":" << hdf5_execution.max_batch_fragments
      << ",\"max_batch_encoded_bytes\":" << hdf5_execution.max_batch_encoded_bytes << '}';
  auto hot_gauges = [&](const char* name, const HotQueryCacheStats& stats) {
    out << ',' << json_quote(name) << ":{";
    bool first_hot = true;
#define HOT_GAUGE(field) if (!first_hot) out << ','; first_hot = false; out << json_quote(#field) << ':' << stats.field;
    HOT_GAUGE(resident_bytes) HOT_GAUGE(domains) HOT_GAUGE(proofs)
    HOT_GAUGE(handles) HOT_GAUGE(handle_bytes) HOT_GAUGE(domain_bytes)
    HOT_GAUGE(proof_bytes) HOT_GAUGE(auxiliary_bytes)
    HOT_GAUGE(resident_domains) HOT_GAUGE(inflight_domains) HOT_GAUGE(retired_domains)
    HOT_GAUGE(resident_domain_bytes) HOT_GAUGE(inflight_domain_bytes) HOT_GAUGE(retired_domain_bytes)
    HOT_GAUGE(domain_hits) HOT_GAUGE(domain_builds) HOT_GAUGE(proof_hits)
    HOT_GAUGE(proof_builds) HOT_GAUGE(opt_outs) HOT_GAUGE(evictions)
    HOT_GAUGE(pressure_requests) HOT_GAUGE(slot_evictions) HOT_GAUGE(pressure_domain_evictions)
    HOT_GAUGE(admission_bypasses) HOT_GAUGE(retention_slots) HOT_GAUGE(retained_values)
    HOT_GAUGE(refunded_bytes) HOT_GAUGE(retention_registry_bytes)
#undef HOT_GAUGE
    out << '}';
  };
  hot_gauges("hot_cache_before", hot_before);
  hot_gauges("hot_cache_after", hot_after);
  if (result.ok() && result.value().kind == QueryControlResponse::Kind::PublishedRoute)
    out << ",\"path\":" << json_quote(result.value().path);
  if (result.ok() && result.value().kind == QueryControlResponse::Kind::PublishedRoutes) {
    out << ",\"items\":[";
    for (std::size_t i = 0; i < result.value().items.size(); ++i) {
      if (i) out << ',';
      out << "{\"index\":" << result.value().items[i].index << ",\"path\":"
        << json_quote(result.value().items[i].path) << '}';
    }
    out << ']';
  }
  if (!result.ok()) out << ",\"error_code\":" << static_cast<int>(result.status().code())
      << ",\"error_phase\":" << static_cast<int>(result.status().phase());
  out << ",\"measurement_semantics\":{"
      "\"server_total\":\"register_json plus response JSON and final catalog admission; excludes socket frame IO and profile sink\","
      "\"stages\":\"inclusive synchronous durations; nested stages overlap\","
      "\"typed_encode\":\"cursor execution plus IPC encoding\","
      "\"typed_ipc_encode\":\"schema, array construction, stream writes and finish; excludes cursor.next\","
      "\"runtime_delta\":\"shared runtime cumulative counter difference over registration; concurrent requests overlap\","
      "\"runtime_timing\":\"cumulative operation time; parallel and nested durations are not additive wall time\","
      "\"runtime_after_gauges\":\"cache_owned_bytes,pinned_bytes,inflight_bytes,temporary_bytes,live_bytes,payload_reads_active\","
      "\"runtime_after_high_watermarks\":\"payload_reads_peak,peak_bytes: runtime lifetime, not request deltas\","
      "\"hot_cache\":\"mount-wide live reservation snapshots including retired pins, not RSS; auxiliary_bytes includes handle_bytes; sampling outside server_total is included in client request latency\","
      "\"payload_reads_peak\":\"transport calls in flight; not IoTDB session count\"}";
  out << ",\"stages_ns\":{";
  bool first = true;
  for (const auto& stage : record.nanoseconds) {
    if (!first) out << ',';
    first = false;
    out << json_quote(stage.first) << ':' << stage.second;
  }
  out << "},\"counters\":{";
  first = true;
  for (const auto& counter : record.counters) {
    if (!first) out << ',';
    first = false;
    out << json_quote(counter.first) << ':' << counter.second;
  }
  out << "},\"runtime_delta_scope\":\"shared-runtime\",\"runtime_delta\":"
      << runtime_delta_json(before, after)
      << ",\"iotdb_delta_scope\":\"daemon-wide; all IoTDB clients/backends; concurrent requests overlap\""
      << ",\"iotdb_semantics\":{"
         "\"version_store_query_calls\":\"daemon-wide direct Store SDK submissions; add to query_calls for total observed SDK query submissions, not wire RPCs\","
         "\"query_nanoseconds\":\"client executeQuery including lease acquisition and initial SDK submission; excludes later result consumption\","
         "\"metadata_query_nanoseconds\":\"backend metadata executeQuery subset of query_nanoseconds; excludes later result consumption\","
         "\"session_acquire_nanoseconds\":\"acquire call wall including pool mutex and availability wait, including timeouts\","
         "\"session_leases_active\":\"current acquired pool leases including result consumption and close\","
         "\"session_leases_peak\":\"daemon lifetime high watermark, not request-local\","
         "\"payload_requested_bytes\":\"validated backend transport attempts, including failures\","
         "\"payload_returned_bytes\":\"bytes returned by successful backend reads, not server disk IO\","
         "\"payload_nanoseconds\":\"backend read wall including query, result consumption, and close; overlaps query and acquire time\"}"
      << ",\"iotdb_delta\":" << iotdb_profile_delta_json(iotdb_before, iotdb_after)
      << ",\"iotdb_after\":{\"session_leases_active\":" << iotdb_after.session_leases_active
      << ",\"session_leases_peak\":" << iotdb_after.session_leases_peak << '}'
      << ",\"payload_read_shape\":" << payload_shape_delta_json(before, after)
      << ",\"runtime_after\":{";
  out << "\"decode_count\":" << after.decode_count
      << ",\"read_bytes\":" << after.read_bytes
      << ",\"producer_count\":" << after.producer_count
      << ",\"hits\":" << after.hits
      << ",\"waits\":" << after.waits
      << ",\"cache_owned_bytes\":" << after.cache_owned_bytes
      << ",\"pinned_bytes\":" << after.pinned_bytes
      << ",\"inflight_bytes\":" << after.inflight_bytes
      << ",\"temporary_bytes\":" << after.temporary_bytes
      << ",\"live_bytes\":" << after.live_bytes
      << ",\"payload_reads_active\":" << after.payload_reads_active
      << ",\"payload_reads_peak\":" << after.payload_reads_peak
      << ",\"peak_bytes\":" << after.peak_bytes << "}}";
  return out.str();
}
#if defined(__linux__)
std::vector<gid_t> peer_supplementary_groups(pid_t pid, uid_t uid, gid_t gid) {
  std::vector<gid_t> groups;
  // SO_PEERCRED does not include supplementary groups. Read the peer's own
  // kernel-maintained group list, never the daemon's or a passwd database's
  // possibly broader account membership. If the process has exited or changed
  // identity since connect, keep only the credentials supplied by the socket.
  std::ifstream status("/proc/" + std::to_string(pid) + "/status");
  if (!status) return groups;
  bool uid_matches = false;
  bool gid_matches = false;
  bool found_groups = false;
  std::string line;
  while (std::getline(status, line)) {
    if (line.compare(0, 4, "Uid:") == 0 || line.compare(0, 4, "Gid:") == 0) {
      std::istringstream fields(line.substr(4));
      unsigned long long real = 0;
      unsigned long long effective = 0;
      if (!(fields >> real >> effective)) return {};
      if (line[0] == 'U') uid_matches = real == uid && effective == uid;
      else gid_matches = real == gid && effective == gid;
    } else if (line.compare(0, 7, "Groups:") == 0) {
      std::istringstream fields(line.substr(7));
      unsigned long long group = 0;
      while (fields >> group) {
        if (group > std::numeric_limits<gid_t>::max()) return {};
        if (group != gid) groups.push_back(static_cast<gid_t>(group));
      }
      if (!fields.eof()) return {};
      found_groups = true;
    }
  }
  if (!status.eof() || !uid_matches || !gid_matches || !found_groups)
    return {};
  return normalized_authorization_groups(gid, std::move(groups));
}
#endif

std::optional<QueryRegistrationPeer> peer_credentials(int fd) {
#if defined(__APPLE__) || defined(__FreeBSD__)
  uid_t uid = static_cast<uid_t>(-1);
  gid_t gid = static_cast<gid_t>(-1);
  if (getpeereid(fd, &uid, &gid) == 0)
    return QueryRegistrationPeer{uid, gid, {}};
  return std::nullopt;
#elif defined(SO_PEERCRED)
  struct PeerCred { pid_t pid; uid_t uid; gid_t gid; } credentials{};
  socklen_t length = sizeof(credentials);
  if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length) == 0 &&
      length == sizeof(credentials))
    return QueryRegistrationPeer{credentials.uid, credentials.gid,
#if defined(__linux__)
      peer_supplementary_groups(credentials.pid, credentials.uid,
        credentials.gid)
#else
      {}
#endif
    };
  return std::nullopt;
#else
  return std::nullopt;
#endif
}
bool set_socket_timeout(int fd, int option, std::chrono::milliseconds duration) {
  timeval timeout{};
  timeout.tv_sec = duration.count() / 1000;
  timeout.tv_usec = (duration.count() % 1000) * 1000;
  return ::setsockopt(fd, SOL_SOCKET, option, &timeout, sizeof(timeout)) == 0;
}
void set_io_timeout(int fd) {
  set_socket_timeout(fd, SO_RCVTIMEO, std::chrono::seconds(3));
  set_socket_timeout(fd, SO_SNDTIMEO, std::chrono::seconds(3));
}
std::optional<std::string_view> registered_id(std::string_view path) {
  const auto start = path.find("?id=");
  if (start == std::string_view::npos) return std::nullopt;
  const auto id = path.substr(start + 4, 48);
  if (id.size() != 48 ||
      !std::all_of(id.begin(), id.end(), [](char byte) {
        return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
      })) return std::nullopt;
  return id;
}
void unlink_if_same_socket(const std::string& path, dev_t device, ino_t inode) {
  struct stat current{};
  if (::lstat(path.c_str(), &current) == 0 && S_ISSOCK(current.st_mode) &&
      current.st_dev == device && current.st_ino == inode)
    ::unlink(path.c_str());
}
bool private_writable_directory(const std::string& path) {
  struct stat metadata{};
  if (path.empty() || ::lstat(path.c_str(), &metadata) != 0 ||
      !S_ISDIR(metadata.st_mode) || metadata.st_uid != ::geteuid() ||
      (metadata.st_mode & 0777) != 0700)
    return false;
  std::string pattern = path + "/.iotdbfs-query-probe-XXXXXX";
  std::vector<char> candidate(pattern.begin(), pattern.end());
  candidate.push_back('\0');
  const int fd = ::mkstemp(candidate.data());
  if (fd < 0) return false;
  ::close(fd);
  return ::unlink(candidate.data()) == 0;
}
std::string mount_socket_name(std::string_view mount_root) {
  while (mount_root.size() > 1 && mount_root.back() == '/')
    mount_root.remove_suffix(1);
  std::uint64_t hash = 14695981039346656037ULL;
  for (const unsigned char byte : mount_root) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  constexpr char digits[] = "0123456789abcdef";
  std::string name = "iotdbfs-";
  for (int shift = 60; shift >= 0; shift -= 4)
    name.push_back(digits[(hash >> shift) & 0xf]);
  return name + ".sock";
}
std::string full_path(const std::string& root, const std::string& route) {
  std::string prefix = root;
  while (prefix.size() > 1 && prefix.back() == '/') prefix.pop_back();
  if (prefix == "/") return route;
  return prefix + route;
}

} // namespace

struct QueryRegistrationSocketServer::ParsedTypedRequest {
  QueryRequest declaration;
  // Borrowed only by the synchronous ordinary typed path. Templates never
  // retain binding input, and reject custom bindings in their static factory.
  const ptree* binding{nullptr};
  // Templates lend compiled paths/predicates to the route declaration. Keep
  // the original registry allocation charged for every such borrowed lifetime.
  PreparedSelectorRegistry::Payload declaration_lifetime;
};

QueryResult<std::string> default_query_registration_socket_path(
    std::string_view mount_root, std::string_view runtime_dir) {
  if (mount_root.empty() || mount_root.front() != '/')
    return QueryResult<std::string>::failure(
      invalid("query mount root must be absolute"));
  const auto uid = std::to_string(::geteuid());
  std::vector<std::string> directories;
  if (!runtime_dir.empty()) directories.emplace_back(runtime_dir);
  directories.push_back("/run/user/" + uid);
  directories.push_back("/run/iotdbfs/" + uid);
  const auto name = mount_socket_name(mount_root);
  for (const auto& directory : directories) {
    const auto path = directory + "/" + name;
    if (path.size() < sizeof(sockaddr_un::sun_path) &&
        private_writable_directory(directory))
      return QueryResult<std::string>::success(path);
  }
  const auto fallback = "/tmp/iotdbfs-" + uid;
  if (::mkdir(fallback.c_str(), 0700) == 0)
    ::chmod(fallback.c_str(), 0700);
  const auto path = fallback + "/" + name;
  if (path.size() < sizeof(sockaddr_un::sun_path) &&
      private_writable_directory(fallback))
    return QueryResult<std::string>::success(path);
  return QueryResult<std::string>::failure(io_error(
    "no private writable runtime directory for registration socket"));
}

QueryRegistrationSocketServer::QueryRegistrationSocketServer(std::string socket_path,
    std::string mount_root, std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<const PublishedVersionCatalog> catalog,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    std::shared_ptr<FuseTypedQueryBridge> bridge, Authorization authorize,
    mode_t socket_mode, TrainingExecutionOptions training_execution,
    PreparedTrainingRegistry::Config prepared_config,
    Hdf5QueryKernelConfig hdf5_execution, DatasetDiscovery discover,
    std::shared_ptr<Hdf5ValidatedLayoutCache> hdf5_layout_cache,
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>> hdf5_profiles,
    BeginAuthorizationPublication begin_authorization_publication,
    bool owner_metadata_budget, Hdf5QueryMode hdf5_mode,
    std::shared_ptr<const QueryModule> query_module,
    std::shared_ptr<HotQueryCache> selector_pool,
    PrepareSelectorBinding prepare_selector_binding, SelectSources select_sources,
    BoundedResultSender::Config result_sender_config)
  : socket_path_(std::move(socket_path)), mount_root_(std::move(mount_root)),
    frontend_(std::move(frontend)), catalog_(std::move(catalog)),
    storage_(std::move(storage)), runtime_(std::move(runtime)),
    bridge_(std::move(bridge)),
    authorize_(std::move(authorize)),
    begin_authorization_publication_(std::move(begin_authorization_publication)),
    discover_(std::move(discover)), prepare_selector_binding_(std::move(prepare_selector_binding)),
    select_sources_(std::move(select_sources)), socket_mode_(socket_mode),
    training_execution_(training_execution), hdf5_execution_(std::move(hdf5_execution)),
    hdf5_layout_cache_(hdf5_layout_cache ? std::move(hdf5_layout_cache)
                                    : make_hdf5_validated_layout_cache()),
    hdf5_profiles_(hdf5_profiles.empty() ? default_hdf5_semantic_profiles()
                                            : std::move(hdf5_profiles)),
    owner_metadata_budget_(owner_metadata_budget), hdf5_mode_(hdf5_mode),
    query_module_(std::move(query_module)), result_sender_(result_sender_config),
    result_sender_threads_(result_sender_config.sender_threads) {
  prepared_ = std::make_unique<PreparedTrainingRegistry>(std::move(prepared_config),
    [this](const std::string& path) { retire_path(path); });
  if (selector_pool)
    profile_hot_pool_ = selector_pool;
  if (selector_pool)
    selectors_ = std::make_unique<PreparedSelectorRegistry>(std::move(selector_pool));
  if (const auto* path = std::getenv("IOTDBFS_QUERY_PROFILE_JSONL"); path && *path)
    profile_output_.open(path, std::ios::out | std::ios::app);
  profile_enabled_ = profile_output_.is_open();
  if (profile_enabled_) iotdb_profile_counters.timing_enabled.store(true, std::memory_order_relaxed);
  // Diagnostics allocate once before serving and write only after all request
  // and sender lifetimes drain. They never add JSON/file IO to the send loop.
  if (const auto* path = std::getenv("IOTDBFS_RESULT_FETCH_TRACE_JSONL"); path && *path) {
    try {
      fetch_timings_.reserve(kMaxFetchTimings);
      fetch_timing_output_.open(path, std::ios::out | std::ios::app);
      fetch_timing_enabled_ = fetch_timing_output_.is_open();
    } catch (...) { fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed); }
  }
}

QueryRegistrationSocketServer::~QueryRegistrationSocketServer() { stop(); }

QueryStatus QueryRegistrationSocketServer::start() {
  if (running_) return invalid("registration socket already running");
  if (!frontend_ || !catalog_ || !storage_ || !runtime_ || !bridge_)
    return invalid("registration socket requires frontend, catalog, storage, runtime, and bridge");
  if (result_sender_threads_ != 1 && result_sender_threads_ != 2)
    return invalid("result fetch sender threads must be 1 or 2");
  if (hdf5_execution_.read_parallelism != 1 && hdf5_execution_.read_parallelism != 2 &&
      hdf5_execution_.read_parallelism != 4)
    return invalid("HDF5 read parallelism must be 1, 2, or 4");
  if (training_execution_.window_group_size == 0 || training_execution_.window_group_size > 64 ||
      training_execution_.read_parallelism < 1 || training_execution_.read_parallelism > 4)
    return invalid("training execution requires window group size in [1,64] and read parallelism in [1,4]");
  if (runtime_->config().max_inflight < 64 * training_execution_.read_parallelism ||
      runtime_->config().worker_threads < training_execution_.read_parallelism)
    return invalid("training read parallelism requires at least 64 inflight fragments and one cache worker per wave");
  if (socket_mode_ != 0600 && socket_mode_ != 0666)
    return invalid("registration socket mode must be 0600 or 0666");
  if (socket_mode_ != 0600 && !authorize_)
    return invalid("shared registration socket requires source authorization");
  if (socket_path_.empty() || socket_path_.size() >= sizeof(sockaddr_un::sun_path) ||
      mount_root_.empty() || mount_root_.front() != '/')
    return invalid("socket path or mount root is invalid");
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
  // A persistent lock file serializes recovery with another mount starting at
  // the same path. flock is released by the kernel even after a hard crash.
  const auto lock_path = socket_path_ + ".lock";
  const int lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (lock_fd < 0) return io_error("cannot open registration socket lock: " +
    std::string(std::strerror(errno)));
  struct stat lock_metadata{};
  if (::fstat(lock_fd, &lock_metadata) != 0 || !S_ISREG(lock_metadata.st_mode) ||
      lock_metadata.st_uid != ::geteuid() || lock_metadata.st_nlink != 1 ||
      ::fchmod(lock_fd, 0600) != 0 || ::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
    ::close(lock_fd);
    return io_error("registration socket lock is unavailable");
  }
  const auto fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    ::close(lock_fd);
    return io_error("cannot create registration socket");
  }
  const auto bind_socket = [&] {
    const auto old_umask = ::umask(0077);
    const int result = ::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    const int bind_errno = errno;
    ::umask(old_umask);
    return std::pair<int, int>{result, bind_errno};
  };
  auto [bound, bind_errno] = bind_socket();
  if (bound != 0 && bind_errno == EADDRINUSE) {
    struct stat stale{};
    if (::lstat(socket_path_.c_str(), &stale) == 0 && S_ISSOCK(stale.st_mode) &&
        stale.st_uid == ::geteuid()) {
      const int probe = ::socket(AF_UNIX, SOCK_STREAM, 0);
      if (probe >= 0) {
        const int connected = ::connect(probe, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        const int connect_errno = errno;
        ::close(probe);
        struct stat current{};
        if (connected != 0 && connect_errno == ECONNREFUSED &&
            ::lstat(socket_path_.c_str(), &current) == 0 &&
            S_ISSOCK(current.st_mode) && current.st_uid == stale.st_uid &&
            current.st_dev == stale.st_dev && current.st_ino == stale.st_ino &&
            ::unlink(socket_path_.c_str()) == 0)
          std::tie(bound, bind_errno) = bind_socket();
      }
    }
  }
  if (bound != 0) {
    ::close(fd);
    ::close(lock_fd);
    return io_error("cannot bind registration socket: " + std::string(std::strerror(bind_errno)));
  }
  struct stat bound_metadata{};
  if (::lstat(socket_path_.c_str(), &bound_metadata) != 0 ||
      !S_ISSOCK(bound_metadata.st_mode) || bound_metadata.st_uid != ::geteuid()) {
    ::close(fd);
    ::close(lock_fd);
    return io_error("cannot verify registration socket after bind");
  }
  if (::chmod(socket_path_.c_str(), socket_mode_) != 0 || ::listen(fd, 16) != 0) {
    ::close(fd);
    unlink_if_same_socket(socket_path_, bound_metadata.st_dev, bound_metadata.st_ino);
    ::close(lock_fd);
    return io_error("cannot secure or listen on registration socket");
  }
  listener_ = fd;
  lock_fd_ = lock_fd;
  socket_dev_ = bound_metadata.st_dev;
  socket_ino_ = bound_metadata.st_ino;
  running_ = true;
  try {
    const auto sender = result_sender_.start();
    if (!sender.ok()) throw std::runtime_error(sender.message());
    for (std::size_t i = 0; i < kWorkerCount; ++i)
      workers_.emplace_back([this] { serve_worker(); });
    accept_thread_ = std::thread([this] { serve(); });
  }
  catch (...) {
    running_ = false;
    result_sender_.stop();
    queue_ready_.notify_all();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    workers_.clear();
    ::close(listener_);
    listener_ = -1;
    unlink_if_same_socket(socket_path_, socket_dev_, socket_ino_);
    ::close(lock_fd_);
    lock_fd_ = -1;
    return io_error("cannot start registration socket worker");
  }
  return QueryStatus::success();
}

void QueryRegistrationSocketServer::stop() {
  if (!running_.exchange(false)) return;
  if (accept_thread_.joinable()) accept_thread_.join();
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    for (const auto& pending : pending_) ::close(pending.first);
    pending_.clear();
  }
  queue_ready_.notify_all();
  for (auto& worker : workers_) if (worker.joinable()) worker.join();
  workers_.clear();
  // Stop delivery before waiting for request pins. Tasks still awaiting query
  // slots will see stopped delivery admission; registry teardown waits for all.
  result_sender_.stop();
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    requests_finished_.wait(lock, [this] { return outstanding_requests_ == 0; });
  }
  flush_fetch_timings();
  if (selectors_) selectors_->clear();
  prepared_->clear();
  if (listener_ >= 0) { ::close(listener_); listener_ = -1; }
  unlink_if_same_socket(socket_path_, socket_dev_, socket_ino_);
  if (lock_fd_ >= 0) { ::close(lock_fd_); lock_fd_ = -1; }
}

void QueryRegistrationSocketServer::serve() {
  while (running_) {
    if (selectors_) selectors_->reap();
    prepared_->reap();
    pollfd poll_socket{listener_, POLLIN, 0};
    const auto polled = ::poll(&poll_socket, 1, 100);
    if (polled < 0 && errno == EINTR) continue;
    if (polled <= 0 || !(poll_socket.revents & POLLIN)) continue;
    const auto fd = ::accept(listener_, nullptr, nullptr);
    if (fd < 0) continue;
    const auto accepted_ns = fetch_timing_enabled_ ? fetch_now_ns() : 0;
    set_io_timeout(fd);
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (pending_.size() >= kMaxPending || !running_) {
        ::close(fd);
        continue;
      }
      pending_.emplace_back(fd, accepted_ns);
    }
    queue_ready_.notify_one();
  }
}

struct QueryRegistrationSocketServer::Connection {
  Connection(int value, std::uint64_t accepted) : fd(value), accept_ns(accepted) {}
  ~Connection() { ::close(fd); }
  int fd;
  std::uint64_t accept_ns;
};
// Only syntax is parsed on the transport worker. The immutable tree (or the
// original parser exception) survives scheduling/reclassification, while all
// validation, authorization and charged work stay on the public query worker.
struct QueryRegistrationSocketServer::ParsedControlRequest {
  explicit ParsedControlRequest(std::string& request, bool timing)
      : request_bytes(request.size()) {
    const auto begin = timing ? fetch_now_ns() : 0;
    try {
      // Do not retain a partially built tree after malformed input or OOM.
      ptree parsed;
      std::istringstream input(request);
      boost::property_tree::read_json(input, parsed);
      root.swap(parsed);
    } catch (const boost::property_tree::json_parser_error&) {
      error = std::current_exception();
    } catch (...) {
      error = std::current_exception();
      // The old transport probe swallowed transient failures (notably OOM),
      // then resource inspection and validation each retried parsing. Keep
      // those original retry opportunities without retaining raw frames on
      // the normal path. Moving the string itself needs no allocation.
      retry_required = true;
      retry_input = std::move(request);
    }
    if (timing) parse_ns = fetch_now_ns() - begin;
  }
  const ptree& get() const {
    if (error) std::rethrow_exception(error);
    return root;
  }
  const ptree& for_validation(ptree& retried_root) const {
    if (!retry_required) return get();
    std::istringstream input(retry_input);
    boost::property_tree::read_json(input, retried_root);
    return retried_root;
  }
  ptree root;
  const std::size_t request_bytes;
  std::exception_ptr error;
  bool retry_required{false};
  std::string retry_input;
  std::uint64_t parse_ns{0};
};

struct QueryRegistrationSocketServer::TaskResourcePlan {
  std::vector<QueryTaskScheduler::ResourceRequirement> resources;
  bool observes_handle{false};
  std::string handle_id;
  std::weak_ptr<const void> observed_payload;
};

void QueryRegistrationSocketServer::serve_worker() {
  for (;;) {
    int fd = -1;
    std::uint64_t accepted_ns = 0;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_ready_.wait(lock, [this] { return !running_ || !pending_.empty(); });
      if (!running_) return;
      fd = pending_.front().first;
      accepted_ns = pending_.front().second;
      pending_.pop_front();
    }
    // An encoding/allocation failure closes this connection after its local
    // rollback guards run; it must not terminate the daemon's worker process.
    ScopeExit close_on_allocation_failure{[fd] { ::close(fd); }};
    try {
      auto connection = std::make_shared<Connection>(fd, accepted_ns);
      close_on_allocation_failure.action = {};
      serve_connection(std::move(connection));
    } catch (...) {}
  }
}

void QueryRegistrationSocketServer::serve_connection(std::shared_ptr<Connection> connection) {
  const auto fd = connection->fd;
  const auto transport_start_ns = fetch_timing_enabled_ ? fetch_now_ns() : 0;
  const auto peer = peer_credentials(fd);
  if (!peer) return;
  if (socket_mode_ == 0600 && peer->uid != ::geteuid()) return;
  std::string request;
  if (!read_frame(fd, request)) return;
  const auto request_ready_ns = fetch_timing_enabled_ ? fetch_now_ns() : 0;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    ++outstanding_requests_;
  }
  // A failed allocation, rejected submission, or task exception all release
  // the same server-local lifetime pin. No global runtime shutdown is needed.
  auto completion = std::shared_ptr<void>(nullptr, [this](void*) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    --outstanding_requests_;
    requests_finished_.notify_all();
  });
  const auto parsed = std::make_shared<const ParsedControlRequest>(request, profile_enabled_);
  bool fetch = false;
  try { fetch = parsed->get().get<std::string>("kind", "") == "result_fetch"; }
  catch (...) {} // Re-throw at the original worker-side validation boundary.
  if (fetch) {
    auto reservation = result_sender_.reserve();
    if (!reservation) {
      write_frame(fd, json_response(QueryResult<QueryControlResponse>::failure(
        QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Execution,
          "result fetch admission is full or stopped"))));
      return;
    }
    std::shared_ptr<BoundedResultSender::TimingRecord> timing;
    if (fetch_timing_enabled_) {
      try {
        timing = std::make_shared<BoundedResultSender::TimingRecord>();
        timing->accept_ns = connection->accept_ns;
        timing->transport_start_ns = transport_start_ns;
        timing->request_ready_ns = request_ready_ns;
        timing->submit_ns = fetch_now_ns();
      } catch (...) { fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed); }
    }
    // This pin is passed intact to the I/O reactor. Merely releasing a public
    // query slot must not refund the delivery reservation or close its socket.
    auto lifetime = std::make_shared<std::pair<std::shared_ptr<Connection>,
      std::shared_ptr<void>>>(connection, completion);
    auto submitted = runtime_->query_tasks().submit({
      [this, connection, lifetime, reservation, timing, parsed, peer = *peer]
          (const CancellationToken&) {
        if (timing) timing->task_start_ns = fetch_now_ns();
        return serve_fetch(connection->fd, *parsed, peer, reservation, lifetime, timing);
      }, {}, QueryTaskScheduler::Clock::time_point::max(), {}});
    if (!submitted.ok()) {
      write_frame(fd, json_response(QueryResult<QueryControlResponse>::failure(submitted.status())));
      if (timing) {
        timing->error = submitted.status().code();
        timing->cleanup_ns = fetch_now_ns();
        record_fetch_timing(std::move(*timing));
      }
    }
    return;
  }
  const auto queued_at = std::chrono::steady_clock::now();
  auto plan = std::make_shared<TaskResourcePlan>(task_resources(*parsed, peer->uid));
  auto submitted = runtime_->query_tasks().submit({
    [this, connection, completion, parsed, peer = *peer, queued_at, plan](const CancellationToken&) {
      try { return serve_request(connection->fd, *parsed, peer, queued_at, plan.get()); }
      catch (const PreparedSelectorRegistry::ObservationChanged&) {
        *plan = task_resources(*parsed, peer.uid);
        throw QueryTaskScheduler::ResourcesChanged{plan->resources};
      }
    }, {}, QueryTaskScheduler::Clock::time_point::max(), plan->resources});
  if (!submitted.ok())
    write_frame(fd, json_response(QueryResult<QueryControlResponse>::failure(submitted.status())));
}

QueryStatus QueryRegistrationSocketServer::serve_fetch(int fd,
    const ParsedControlRequest& parsed, const QueryRegistrationPeer& peer,
    const std::shared_ptr<BoundedResultSender::Reservation>& reservation,
    std::shared_ptr<void> lifetime,
    std::shared_ptr<BoundedResultSender::TimingRecord> timing) {
  auto status = QueryStatus::success();
  try {
    // Fresh authorization and its fee ledger run on a public query worker.
    // Only already encoded immutable bytes enter the delivery reactor.
    std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
    if (owner_metadata_budget_) metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
    ptree retried_root;
    const auto& root = parsed.for_validation(retried_root);
    require_object_fields(root, "result fetch", {"kind", "version", "path"});
    if (required_string(root, "kind") != "result_fetch" ||
        required_number<unsigned>(root, "version") != 1)
      throw std::invalid_argument("unsupported result fetch protocol");
    const auto path = required_string(root, "path");
    if (timing) {
      try {
        // A malformed client path must not amplify the bounded diagnostic
        // buffer into 8192 full request frames. This does not limit the API.
        if (path.size() > 4096) {
          fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed);
          timing.reset();
        } else timing->path = path;
      }
      catch (...) {
        fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed);
        timing.reset();
      }
    }
    const auto prefix = mount_root_.back() == '/' ? mount_root_ : mount_root_ + '/';
    if (path.compare(0, prefix.size(), prefix) != 0)
      throw std::invalid_argument("result fetch path is outside this mount");
    const auto route = path.substr(prefix.size() - 1);
    const auto marker = route.rfind("&result=");
    if (marker == std::string::npos || route.size() - marker != 72 ||
        !std::all_of(route.begin() + marker + 8, route.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        })) throw std::invalid_argument("result fetch requires an immutable result path");
    if (timing) timing->admit_start_ns = fetch_now_ns();
    auto opened = bridge_->acquire_exact(route.substr(0, marker), route.substr(marker + 8),
      [this, &peer](const std::vector<QuerySourceReference>& sources) {
        return authorize_ ? authorize_(peer, sources, nullptr) : QueryStatus::success();
      }, QueryPrincipal{peer.uid, peer.gid, peer.supplementary_groups});
    if (timing) timing->admit_end_ns = fetch_now_ns();
    if (!opened.ok()) status = opened.status();
    else {
      const auto budget = metadata_scope ? metadata_scope->status(QueryPhase::Encoding)
        : QueryStatus::success();
      if (!budget.ok()) status = budget;
      else {
        const auto header = "{\"ok\":true,\"kind\":\"result_fetch\",\"version\":1,\"size\":" +
          std::to_string(opened.value()->size()) + "}";
        BoundedResultSender::TimingSink sink;
        if (timing) sink = [this](BoundedResultSender::TimingRecord&& record) {
          record_fetch_timing(std::move(record));
        };
        status = result_sender_.send(reservation, opened.value(), fd,
          std::move(lifetime), header, timing, std::move(sink));
        if (status.ok()) return status;
      }
    }
  } catch (const std::invalid_argument& error) {
    status = invalid(error.what());
  } catch (const std::bad_alloc&) {
    status = QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Execution, "result fetch allocation failed");
  } catch (const std::exception& error) {
    status = io_error(error.what());
  }
  // A failed fetch only releases this open/transfer. Shared publications and
  // their registration-response rollback semantics remain unchanged.
  write_frame(fd, json_response(QueryResult<QueryControlResponse>::failure(status)));
  if (timing) {
    timing->error = status.code();
    timing->cleanup_ns = fetch_now_ns();
    record_fetch_timing(std::move(*timing));
  }
  return status;
}

void QueryRegistrationSocketServer::record_fetch_timing(
    BoundedResultSender::TimingRecord&& timing) noexcept {
  try {
    std::lock_guard<std::mutex> lock(fetch_timing_mutex_);
    if (fetch_timings_.size() < kMaxFetchTimings)
      fetch_timings_.push_back(std::move(timing));
    else fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed);
  } catch (...) { fetch_timings_dropped_.fetch_add(1, std::memory_order_relaxed); }
}

void QueryRegistrationSocketServer::flush_fetch_timings() noexcept {
  if (!fetch_timing_enabled_) return;
  try {
    std::lock_guard<std::mutex> lock(fetch_timing_mutex_);
    for (const auto& record : fetch_timings_) {
      fetch_timing_output_ << "{\"kind\":\"result_fetch_timing\",\"path\":"
        << json_quote(record.path) << ",\"sender_threads\":" << result_sender_threads_;
#define FETCH_TIME(name) fetch_timing_output_ << ",\"" #name "\":" << record.name
      FETCH_TIME(accept_ns); FETCH_TIME(transport_start_ns); FETCH_TIME(request_ready_ns);
      FETCH_TIME(submit_ns); FETCH_TIME(task_start_ns); FETCH_TIME(admit_start_ns);
      FETCH_TIME(admit_end_ns); FETCH_TIME(enqueue_ns); FETCH_TIME(dequeue_ns);
      FETCH_TIME(first_send_ns); FETCH_TIME(last_send_ns); FETCH_TIME(cleanup_ns);
      FETCH_TIME(send_cpu_ns); FETCH_TIME(send_calls); FETCH_TIME(bytes_sent);
      FETCH_TIME(eagain); FETCH_TIME(short_writes); FETCH_TIME(shard);
      FETCH_TIME(send_span_count); FETCH_TIME(send_spans_dropped);
#undef FETCH_TIME
      fetch_timing_output_ << ",\"completed\":" << (record.completed ? "true" : "false")
        << ",\"error\":" << static_cast<unsigned>(record.error) << ",\"send_spans\":[";
      for (std::size_t i = 0; i < record.send_span_count; ++i) {
        if (i) fetch_timing_output_ << ',';
        fetch_timing_output_ << "{\"begin_ns\":" << record.send_spans[i].begin_ns
          << ",\"end_ns\":" << record.send_spans[i].end_ns
          << ",\"requested_bytes\":" << record.send_spans[i].requested_bytes
          << ",\"result_bytes\":" << record.send_spans[i].result_bytes
          << ",\"error_no\":" << record.send_spans[i].error_no << '}';
      }
      fetch_timing_output_ << "]}\n";
    }
    fetch_timing_output_ << "{\"kind\":\"summary\",\"records\":" << fetch_timings_.size()
      << ",\"dropped\":" << fetch_timings_dropped_.load(std::memory_order_relaxed)
      << ",\"sender_threads\":" << result_sender_threads_ << "}\n";
    fetch_timing_output_.flush();
    fetch_timings_.clear();
    fetch_timings_dropped_.store(0, std::memory_order_relaxed);
  } catch (...) {} // Trace IO is best effort and cannot change query outcomes.
}

QueryRegistrationSocketServer::TaskResourcePlan
QueryRegistrationSocketServer::task_resources(const ParsedControlRequest& parsed, uid_t uid) const {
  // Indexed HDF queries do not enter the native library. Neither source
  // selection nor format planning belongs in this uncharged scheduling hint.
  if (hdf5_mode_ != Hdf5QueryMode::NativeHdf5) return {};
  TaskResourcePlan plan;
  const auto needs_native = [&] {
    const auto selector_needs_native = [](const SourceSelector& sources) {
      return sources.formats.adapters != AdapterSelectionKind::Listed ||
        std::find(sources.formats.adapter_ids.begin(), sources.formats.adapter_ids.end(),
          "hdf5") != sources.formats.adapter_ids.end();
    };
    const auto declaration_needs_native = [](const ptree& declaration) {
      if (declaration.get_child_optional("hdf5_binding")) return true;
      const auto formats = declaration.get_child_optional("request.sources.formats");
      if (!formats || formats->get<std::string>("adapters", "all") != "listed") return true;
      const auto ids = formats->get_child_optional("adapter_ids");
      if (!ids) return true; // An uncertain shape is conservatively admitted.
      for (const auto& id : *ids)
        if (!id.first.empty() || !id.second.empty() || id.second.data() == "hdf5") return true;
      return false;
    };
    try {
      ptree retried_root;
      const auto& root = parsed.for_validation(retried_root);
      const auto kind = root.get<std::string>("kind", "");
      if (kind == "typed" || kind == "format_catalog") return declaration_needs_native(root);
      if (kind == "typed_batch") {
        const auto items = root.get_child_optional("items");
        if (!items) return true;
        for (const auto& item : *items) if (declaration_needs_native(item.second)) return true;
        return false;
      }
      if (kind == "query_template_execute" || kind == "format_catalog_by_selector") {
        const auto id = root.get<std::string>(
          kind == "query_template_execute" ? "template_id" : "selector_id", "");
        // inspect does not renew a handle or resolve a source/version. The
        // execution path still performs its original acquire/epoch/admission.
        const auto payload = selectors_ ? selectors_->inspect(uid, id) : nullptr;
        plan.observes_handle = true;
        plan.handle_id = id;
        plan.observed_payload = payload;
        if (!payload) return false; // Missing/expired handles fail before native work.
        const auto prepared = std::static_pointer_cast<const PreparedFrontendTemplate>(payload);
        if (const auto* single = std::get_if<TypedSingleTemplate>(&prepared->declaration))
          return selector_needs_native(single->declaration->request().sources);
        if (const auto* batch = std::get_if<TypedBatchTemplate>(&prepared->declaration)) {
          for (const auto& item : batch->declarations)
            if (selector_needs_native(item->request().sources)) return true;
          return false;
        }
        if (const auto* catalog = std::get_if<FormatCatalogTemplate>(&prepared->declaration))
          return selector_needs_native(catalog->declaration.sources);
        return false;
      }
      // Declaration preparation, release, dataset artifacts and Arrow training
      // operations do not use HDF5's native library.
      return false;
    } catch (...) {
      // Never replace the ordinary validation error or mutate its precedence.
      return true;
    }
  }();
  if (needs_native) plan.resources.push_back({hdf5_query_task_resource(), 1});
  return plan;
}

QueryStatus QueryRegistrationSocketServer::serve_request(int fd, const ParsedControlRequest& parsed,
    const QueryRegistrationPeer& authenticated_peer, std::chrono::steady_clock::time_point queued_at,
    const TaskResourcePlan* plan) {
  // All thread-local scopes start on the query worker, never on the transport
  // worker. Existing nested batch budgets and execution leases remain local.
  const auto* peer = &authenticated_peer;
  std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
  if (owner_metadata_budget_) metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
  const bool profiling = profile_enabled_;
  QueryProfileRecord profile;
  const auto sequence = profiling ? ++profile_sequence_ : 0;
  const auto before = profiling ? runtime_->stats() : QueryRuntimeStats{};
  const auto hot_before = profiling && profile_hot_pool_ ? profile_hot_pool_->stats() : HotQueryCacheStats{};
  const auto iotdb_before = profiling ? iotdb_profile_snapshot() : IoTDBProfileSnapshot{};
  const auto start = profiling ? std::chrono::steady_clock::now()
    : std::chrono::steady_clock::time_point{};
  std::string operation, prepared_id;
  auto profile_scope = std::make_unique<ScopedQueryProfile>(profiling ? &profile : nullptr);
  if (profiling) query_profile_count("query_tasks.queue_nanoseconds",
    std::chrono::duration_cast<std::chrono::nanoseconds>(start - queued_at).count());
  auto result = register_json({}, *peer, operation, prepared_id, true, plan, &parsed);
  struct SelectorPublicationRollback {
    PreparedSelectorRegistry* registry;
    uid_t uid;
    QueryResult<QueryControlResponse>& result;
    bool delivered{false};
    ~SelectorPublicationRollback() {
      if (!delivered && registry && result.ok()) {
        if (result.value().kind == QueryControlResponse::Kind::PreparedCatalogSelector)
          registry->erase(uid, result.value().selector_id, result.value().prepared_payload);
        else if (result.value().kind == QueryControlResponse::Kind::PreparedQueryTemplate)
          registry->erase(uid, result.value().template_id, result.value().prepared_payload);
      }
    }
  } selector_publication{selectors_.get(), peer->uid, result};
  auto retire_result = [&] {
    if (!result.ok()) return;
    if (result.value().kind == QueryControlResponse::Kind::PublishedRoute)
      retire_path(result.value().path);
    else if (result.value().kind == QueryControlResponse::Kind::PublishedRoutes)
      for (const auto& item : result.value().items) retire_path(item.path);
    else if (result.value().kind == QueryControlResponse::Kind::PreparedCatalogSelector && selectors_)
      selectors_->erase(peer->uid, result.value().selector_id, result.value().prepared_payload);
    else if (result.value().kind == QueryControlResponse::Kind::PreparedQueryTemplate && selectors_)
      selectors_->erase(peer->uid, result.value().template_id, result.value().prepared_payload);
  };
  if (metadata_scope && !metadata_scope->status(QueryPhase::Encoding).ok()) {
    retire_result();
    result = QueryResult<QueryControlResponse>::failure(metadata_scope->status(QueryPhase::Encoding));
  }
  auto response_bytes = json_response(result);
  const auto catalog_context = result.ok() ?
    (result.value().kind == QueryControlResponse::Kind::DatasetCatalog ? result.value().datasets.read_context :
     result.value().kind == QueryControlResponse::Kind::FormatCatalog ? result.value().format_units.read_context : nullptr) : nullptr;
  if (catalog_context) {
    RequestMetadataPhaseProfile final_admit_profile("publication.admit_metadata.");
    // Encoding and socket I/O are outside the namespace gate. The decision
    // to expose these frozen catalog bytes is serialized with namespace commit.
    QueryStatus admission_status;
    try {
      auto admitted = catalog_context->admit(
        {peer->uid, peer->gid, peer->supplementary_groups});
      if (!admitted.ok()) admission_status = admitted.status();
      if (metadata_scope && !metadata_scope->status(QueryPhase::Encoding).ok())
        admission_status = metadata_scope->status(QueryPhase::Encoding);
    } catch (const std::exception& error) {
      admission_status = QueryStatus::failure(QueryError::ReadFailure,
        QueryPhase::Catalog, error.what());
    }
    if (metadata_scope && !metadata_scope->status(QueryPhase::Encoding).ok())
      admission_status = metadata_scope->status(QueryPhase::Encoding);
    if (!admission_status.ok()) {
      result = QueryResult<QueryControlResponse>::failure(std::move(admission_status));
      response_bytes = json_response(result);
    }
  }
  // Include JSON encoding and the final catalog admission in request evidence.
  // Network backpressure remains outside both the gate and measured request.
  if (profiling && metadata_scope)
    profile_metadata_stats("request_metadata.", metadata_scope->budget()->stage_summary_stats(),
      {}, metadata_scope->budget()->consumed_bytes());
  profile_scope.reset();
  const auto end = profiling ? std::chrono::steady_clock::now()
    : std::chrono::steady_clock::time_point{};
  const auto after = profiling ? runtime_->stats() : QueryRuntimeStats{};
  const auto hot_after = profiling && profile_hot_pool_ ? profile_hot_pool_->stats() : HotQueryCacheStats{};
  const auto iotdb_after = profiling ? iotdb_profile_snapshot() : IoTDBProfileSnapshot{};
  selector_publication.delivered = write_frame(fd, response_bytes);
  if (!selector_publication.delivered && result.ok()) {
    if (result.value().kind == QueryControlResponse::Kind::PreparedWindow)
      prepared_->erase(result.value().prepared_id, peer->uid);
    else if (result.value().kind == QueryControlResponse::Kind::PublishedRoute) {
      if (!prepared_id.empty()) prepared_->release_result(prepared_id, peer->uid, result.value().path);
      retire_path(result.value().path);
    } else if (result.value().kind == QueryControlResponse::Kind::PublishedRoutes)
      retire_result();
  }
  if (profiling) {
    try {
      const auto line = query_profile_json(sequence, parsed.request_bytes, result,
        profile, operation, prepared_id, before, after, hot_before, hot_after, iotdb_before, iotdb_after, runtime_->config(), training_execution_, hdf5_execution_, hdf5_mode_, result_sender_threads_, start, end);
      std::lock_guard<std::mutex> lock(profile_mutex_);
      profile_output_ << line << '\n';
      profile_output_.flush();
    } catch (...) {} // A diagnostic sink failure must not fail registration.
  }
  return result.status();
}

void QueryRegistrationSocketServer::retire_path(const std::string& path) {
  const auto prefix = full_path(mount_root_, "");
  if (path.compare(0, prefix.size(), prefix) != 0) return;
  auto route = path.substr(prefix.size());
  const auto suffix = route.rfind("&result=");
  if (suffix != std::string::npos) route.resize(suffix);
  if (const auto id = registered_id(route)) frontend_->erase(*id);
  bridge_->drop(route);
}

QueryRegistrationSocketServer::AuthorizationPublicationCommit
QueryRegistrationSocketServer::begin_authorization_publication(
    const QueryRegistrationPeer& peer, const CatalogSelection* selection) {
  if (!begin_authorization_publication_) return {};
  try { return begin_authorization_publication_(peer, selection); }
  catch (...) { return {}; } // Optional caching must not change authorization.
}

QueryResult<std::string> QueryRegistrationSocketServer::publish_route(
    std::string route, const QueryRegistrationPeer& peer,
    const CatalogSelection* authorized_selection,
    AuthorizationPublicationCommit observed_authorization) {
  QueryProfileTimer timer("publish_route");
  // Owner materialization and cached opens admit the read context directly.
  // Construct strict fallback membership only if its callback is consumed.
  std::optional<std::set<std::tuple<std::string, std::int64_t, std::string>>> authorized_sources;
  auto published = bridge_->acquire(route,
    [this, peer, authorized_selection, &authorized_sources, &observed_authorization](const std::vector<QuerySourceReference>& sources) {
      // Reuse only this registration's successful check of the same exact
      // versions. This callback and selection never enter the result cache.
      if (authorized_selection && !authorized_sources) {
        authorized_sources.emplace();
        for (const auto& manifest : authorized_selection->versions())
          authorized_sources->emplace(manifest.key.path, manifest.key.version,
            manifest.format_id.value_or(""));
      }
      if (authorized_sources && std::all_of(sources.begin(), sources.end(),
          [&](const auto& source) {
            return authorized_sources->count(
              {source.path, source.version, source.format_id}) != 0;
          })) {
        query_profile_count("registration_authorization_reuse");
        return QueryStatus::success();
      }
      if (!authorize_) return QueryStatus::success();
      auto observation = begin_authorization_publication(peer, nullptr);
      auto status = authorize_(peer, sources, nullptr);
      observed_authorization = status.ok() ? std::move(observation) : AuthorizationPublicationCommit{};
      return status;
    }, authorized_selection, QueryPrincipal{peer.uid, peer.gid, peer.supplementary_groups});
  if (!published.ok()) {
    if (const auto id = registered_id(route)) frontend_->erase(*id);
    bridge_->drop(route);
    return QueryResult<std::string>::failure(published.status());
  }
  const auto immutable_route = route + "&result=" + published.value()->id();
  if (observed_authorization) {
    try {
      if (observed_authorization(immutable_route))
        query_profile_count("authorization_publication_prefill");
    } catch (...) {} // Optional caching must not change publication success.
  }
  return QueryResult<std::string>::success(full_path(mount_root_, immutable_route));
}

QueryResult<QueryControlResponse> QueryRegistrationSocketServer::discover_format_catalog(
    const QueryRequest& request, const QueryRegistrationPeer& peer,
    const std::shared_ptr<HotQueryCache::Binding>& binding) {
  using Result = QueryResult<QueryControlResponse>;
  if (!query_module_) return Result::failure(invalid("format discovery module is unavailable"));
  if (binding && !select_sources_) return Result::failure(invalid("prepared source selection is unavailable"));
  auto status = query_module_->validate_format_catalog_request(request);
  if (!status.ok()) return Result::failure(status);
  RequestMetadataPhaseProfile select_profile("format_catalog.select_metadata.");
  auto selected = binding ? select_sources_(request.sources, request.limits.max_sources, binding) : query_module_->select(request);
  select_profile.finish();
  if (!selected.ok()) return Result::failure(selected.status());
  if (authorize_) {
    std::vector<QuerySourceReference> owned_sources;
    const auto& sources = authorization_sources(selected.value(), owned_sources);
    QueryProfileTimer timer("format_catalog_authorize");
    RequestMetadataPhaseProfile auth_profile("format_catalog.authorize_metadata.");
    status = authorize_(peer, sources, &selected.value());
    if (!status.ok()) return Result::failure(status);
  }
  std::shared_ptr<QueryExecutionLease> lease;
  std::unique_ptr<ScopedQueryExecutionLease> execution_scope;
  if (selected.value().read_context) {
    RequestMetadataPhaseProfile admit_profile("format_catalog.admit_metadata.");
    auto admitted = selected.value().read_context->admit({peer.uid, peer.gid, peer.supplementary_groups});
    if (!admitted.ok()) return Result::failure(admitted.status());
    lease = admitted.value()->execution_lease();
    if (lease) execution_scope = std::make_unique<ScopedQueryExecutionLease>(lease);
  }
  RequestMetadataPhaseProfile prepare_profile("format_catalog.prepare_metadata.");
  auto prepared = query_module_->prepare(request, &selected.value());
  prepare_profile.finish();
  if (!prepared.ok()) return Result::failure(prepared.status());
  RequestMetadataPhaseProfile locator_profile("format_catalog.locators_metadata.");
  auto found = query_module_->format_catalog(prepared.value(), kMaxFrame);
  locator_profile.finish();
  if (!found.ok()) return Result::failure(found.status());
  QueryControlResponse response;
  response.kind = QueryControlResponse::Kind::FormatCatalog;
  response.format_units = std::move(found.value());
  auto result = Result::success(std::move(response));
  if (json_response(result).size() > kMaxFrame)
    return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Encoding, "format discovery response exceeds control frame limit"));
  return result;
}

QueryResult<QueryControlResponse> QueryRegistrationSocketServer::execute_validated_typed(
    ParsedTypedRequest parsed, const QueryRegistrationPeer& peer) {
  using Result = QueryResult<QueryControlResponse>;
  try {
    auto& declaration = parsed.declaration;
    const auto* binding = parsed.binding;
    std::optional<CatalogSelection> authorized_selection;
    AuthorizationPublicationCommit observed_authorization;
    if (authorize_) {
      auto selected = [&] {
        QueryProfileTimer timer("registration_catalog");
        query_profile_count("registration_catalog_calls");
        return catalog_->scan(declaration.sources, declaration.limits.max_sources);
      }();
      if (!selected.ok()) return Result::failure(selected.status());
      std::vector<QuerySourceReference> owned_sources;
      const auto& sources = authorization_sources(selected.value(), owned_sources);
      observed_authorization = begin_authorization_publication(peer, &selected.value());
      auto checked = [&] {
        QueryProfileTimer timer("registration_authorize");
        return authorize_(peer, sources, &selected.value());
      }();
      if (!checked.ok()) return Result::failure(checked);
      authorized_selection = std::move(selected.value());
    }
    std::shared_ptr<const QueryModule> route_module;
    if (binding) {
      QueryProfileTimer timer("hdf5_binding_prepare");
      if (!declaration.subset || !has_hdf5_query_fields(*declaration.subset) ||
          declaration.sources.paths.size() != 1 ||
          declaration.sources.paths.front().kind != PathScopeKind::Exact ||
          declaration.sources.versions.kind != VersionSelectionKind::Exact ||
          !declaration.sources.versions.exact)
        return Result::failure(invalid("HDF5 binding requires one exact HDF5 source and version"));
      if (required_string(*binding, "profile") != kHdf5GwoscProfile)
        return Result::failure(invalid("unsupported HDF5 binding profile"));
      const auto path = required_string(*binding, "dataset_path");
      if (!make_hdf5_value_field(path).ok())
        return Result::failure(invalid("invalid HDF5 binding dataset path"));
      const auto& fragment = *declaration.subset;
      const bool gps_used = std::any_of(fragment.projection.begin(), fragment.projection.end(),
          [](const auto& field) {return field.field_id == kHdf5GpsSecondsField;}) ||
        std::any_of(fragment.slices.begin(), fragment.slices.end(),
          [](const auto& slice) {return slice.dimension.field_id == kHdf5GpsSecondsField;});
      if (!gps_used)
        return Result::failure(invalid("HDF5 binding requires a GPS coordinate projection or slice"));
      Hdf5SemanticConfig semantics;
      semantics.gwosc_bindings.push_back({
        {declaration.sources.paths.front().path, *declaration.sources.versions.exact},
        path, storage_->storage_namespace()});
      auto created = QueryModule::create(catalog_, storage_, {
        std::make_shared<Hdf5FormatQueryAdapter>(hdf5_execution_, runtime_,
          std::move(semantics), Hdf5LayoutCacheConfig{}, hdf5_profiles_,
          hdf5_layout_cache_, hdf5_mode_)});
      if (!created.ok()) return Result::failure(created.status());
      route_module = std::move(created.value());
    }
    auto registered = [&] {
      QueryProfileTimer timer("route_register");
      return frontend_->register_typed(std::move(declaration), std::move(route_module),
        std::move(parsed.declaration_lifetime));
    }();
    if (!registered.ok()) return Result::failure(registered.status());
    auto published = publish_route(std::move(registered.value()), peer,
      authorized_selection ? &*authorized_selection : nullptr, std::move(observed_authorization));
    if (!published.ok()) return Result::failure(published.status());
    QueryControlResponse response; response.path = std::move(published.value());
    return Result::success(std::move(response));
  } catch (const QueryMetadataBudgetExceeded& error) {
    return Result::failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Encoding, error.what()));
  } catch (const std::exception& error) {
    // This boundary is inside any caller's batch scopes, just like the old
    // recursive typed dispatcher. Do not lose a swallowed sticky budget error.
    if (auto budget = current_query_metadata_budget(); budget && !budget->status(QueryPhase::Encoding).ok())
      return Result::failure(budget->status(QueryPhase::Encoding));
    return Result::failure(invalid(error.what()));
  }
}

QueryResult<QueryControlResponse> QueryRegistrationSocketServer::register_json(
    const std::string& json, const QueryRegistrationPeer& peer,
    std::string& operation, std::string& prepared_id, bool record_total, const TaskResourcePlan* plan,
    const ParsedControlRequest* parsed) {
  std::optional<QueryProfileTimer> total_timer;
  if (record_total) total_timer.emplace("registration_total");
  query_profile_count("registration_attempts");
  using Result = QueryResult<QueryControlResponse>;
  auto path_response = [](QueryResult<std::string> path) {
    if (!path.ok()) return Result::failure(path.status());
    QueryControlResponse result; result.path = std::move(path.value());
    return Result::success(std::move(result));
  };
  try {
    ptree owned_root;
    const ptree* root_ptr = nullptr;
    {
      // json_parse remains a synchronous worker stage. For socket frames the
      // full parse is already complete; account its transport cost separately
      // rather than adding an out-of-scope duration to server_total.
      QueryProfileTimer timer("json_parse");
      if (parsed) {
        query_profile_count("control_json.parse_attempts");
        query_profile_count("control_json.transport_parse_ns", parsed->parse_ns);
        query_profile_count("control_json.reuses", parsed->retry_required ? 0 : 1);
        query_profile_count("control_json.worker_retry_parse_attempts", parsed->retry_required ? 1 : 0);
        root_ptr = &parsed->for_validation(owned_root);
      } else {
        // Batch recursion keeps its existing serialization/parser boundary.
        std::istringstream input(json);
        boost::property_tree::read_json(input, owned_root);
        root_ptr = &owned_root;
        query_profile_count("control_json.batch_parse_attempts");
      }
    }
    const auto& root = *root_ptr;
    const auto kind = required_string(root, "kind");
    operation = kind;
    if (kind != "typed" && root.get_child_optional("hdf5_binding"))
      return Result::failure(invalid("HDF5 binding is only supported by typed registrations"));
    if (kind == "query_template_prepare") {
      require_object_fields(root, "query template prepare", {"kind", "declaration"});
      const auto& declaration = required_child(root, "declaration");
      if (!selectors_) return Result::failure(QueryStatus::failure(
        QueryError::UnsupportedCapability, QueryPhase::Validation,
        "query templates require a shared hot cache"));
      auto registered = selectors_->insert(peer.uid, query_template_reservation(declaration), [&]() {
        using PayloadResult = QueryResult<PreparedSelectorRegistry::Payload>;
        try {
          auto prepared = std::make_shared<PreparedFrontendTemplate>();
          const auto declaration_kind = required_string(declaration, "kind");
          if (declaration_kind == "typed") {
            require_object_fields(declaration, "typed declaration", {"kind", "request", "hdf5_binding"});
            auto parsed = ImmutableTypedDeclaration::parse(declaration, false);
            if (!parsed.ok()) return PayloadResult::failure(parsed.status());
            prepared->declaration = TypedSingleTemplate{std::move(parsed.value())};
          } else if (declaration_kind == "typed_batch") {
            require_object_fields(declaration, "typed batch", {"kind", "items"});
            const auto& items = required_child(declaration, "items");
            if (!items.data().empty() || items.empty() || items.size() > kMaxTypedBatchItems)
              return PayloadResult::failure(invalid("typed batch requires 1 to 64 items"));
            TypedBatchTemplate batch;
            batch.declarations.reserve(items.size());
            std::set<std::pair<std::string, std::int64_t>> sources;
            const QueryLimits ceilings;
            for (const auto& item : items) {
              if (!item.first.empty()) return PayloadResult::failure(invalid("typed batch items must be an array"));
              require_object_fields(item.second, "typed batch item", {"kind", "request", "hdf5_binding"});
              auto parsed = ImmutableTypedDeclaration::parse(item.second, true);
              if (!parsed.ok()) return PayloadResult::failure(parsed.status());
              const auto& request = parsed.value()->request();
              for (const auto& path : request.sources.paths) {
                sources.emplace(path.path, *request.sources.versions.exact);
                if (sources.size() > ceilings.max_sources)
                  return PayloadResult::failure(QueryStatus::failure(QueryError::ResourceExhausted,
                    QueryPhase::Validation, "typed batch source budget exhausted"));
              }
              batch.declarations.push_back(std::move(parsed.value()));
            }
            prepared->declaration = std::move(batch);
          } else return PayloadResult::failure(invalid("query templates require typed or typed_batch declarations"));
          prepared->epoch = {storage_, catalog_, query_module_, storage_->storage_namespace()};
          return PayloadResult::success(std::move(prepared));
        } catch (const std::bad_alloc&) { throw; }
        catch (const std::exception& error) { return PayloadResult::failure(invalid(error.what())); }
      });
      if (!registered.ok()) return Result::failure(registered.status());
      auto acquired = selectors_->acquire(peer.uid, registered.value());
      if (!acquired.ok()) return Result::failure(missing_query_template());
      PreparedPublicationGuard rollback{selectors_.get(), peer.uid, registered.value(), acquired.value()};
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::PreparedQueryTemplate;
      response.template_id = registered.value();
      response.prepared_payload = acquired.value();
      response.ttl_ms = selectors_->ttl_ms();
      query_profile_count("query_template.prepares");
      auto result = Result::success(std::move(response));
      rollback.published = true;
      return result;
    }
    if (kind == "query_template_execute" || kind == "query_template_release") {
      require_object_fields(root, "query template operation", {"kind", "template_id"});
      const auto id = required_string(root, "template_id");
      auto acquired = selectors_ ? selectors_->acquire(peer.uid, id,
        kind == "query_template_execute" && plan && plan->observes_handle && plan->handle_id == id ?
          &plan->observed_payload : nullptr) :
        QueryResult<PreparedSelectorRegistry::Payload>::failure(missing_query_template());
      const auto prepared = acquired.ok() ?
        std::static_pointer_cast<const PreparedFrontendTemplate>(acquired.value()) : nullptr;
      if (kind == "query_template_release") {
        QueryControlResponse response;
        response.kind = QueryControlResponse::Kind::ReleasedQueryTemplate;
        response.released = prepared && is_query_template(*prepared) &&
          selectors_->erase(peer.uid, id, acquired.value());
        query_profile_count("query_template.releases");
        return Result::success(std::move(response));
      }
      if (!prepared || !is_query_template(*prepared)) return Result::failure(missing_query_template());
      if (prepared->epoch.storage.lock() != storage_ ||
          prepared->epoch.catalog.lock() != catalog_ ||
          prepared->epoch.module.lock() != query_module_ ||
          prepared->epoch.storage_namespace != storage_->storage_namespace())
        return Result::failure(QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog,
          "query template owner instance has changed"));
      query_profile_count("query_template.executes");
      query_profile_count("selector.compiles", 0);
      query_profile_count("selector.input_path_items", 0);
      if (const auto* single = std::get_if<TypedSingleTemplate>(&prepared->declaration)) {
        query_profile_count("query_template.single_executes");
        query_profile_count("query_template.batch_executes", 0);
        query_profile_count("query_template.declaration_reuses");
        return execute_validated_typed({single->declaration->request(), nullptr, acquired.value()}, peer);
      }
      const auto& batch = std::get<TypedBatchTemplate>(prepared->declaration);
      ScopedQueryMetadataBudget metadata_scope;
      ScopedQueryBatchBudget batch_scope(std::make_shared<QueryBatchBudget>());
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::PublishedRoutes;
      ScopeExit rollback{[&] { for (const auto& item : response.items) retire_path(item.path); }};
      try {
        response.items.reserve(batch.declarations.size());
        query_profile_count("query_template.single_executes", 0);
        query_profile_count("query_template.batch_executes");
        query_profile_count("query_template.declaration_reuses", batch.declarations.size());
        query_profile_count("typed_batch_items", batch.declarations.size());
        for (std::size_t index = 0; index < batch.declarations.size(); ++index) {
          query_profile_count("registration_attempts");
          auto item = execute_validated_typed(
            {batch.declarations[index]->request(), nullptr, acquired.value()}, peer);
          if (!item.ok()) return Result::failure(item.status());
          // Capacity was reserved before publication, so moving the route into
          // this rollback-owned slot cannot allocate.
          response.items.push_back({index, std::move(item.value().path)});
          if (!metadata_scope.status(QueryPhase::Encoding).ok())
            return Result::failure(metadata_scope.status(QueryPhase::Encoding));
        }
        auto result = Result::success(response);
        if (json_response(result).size() > kMaxFrame)
          return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
            QueryPhase::Encoding, "typed batch response exceeds control frame limit"));
        rollback.action = {};
        return result;
      } catch (const QueryMetadataBudgetExceeded& error) {
        return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Encoding, error.what()));
      } catch (const std::exception& error) {
        if (!metadata_scope.status(QueryPhase::Encoding).ok())
          return Result::failure(metadata_scope.status(QueryPhase::Encoding));
        return Result::failure(invalid(error.what()));
      }
    }
    if (kind == "typed_batch") {
      require_object_fields(root, "typed batch", {"kind", "items"});
      const auto& items = required_child(root, "items");
      if (!items.data().empty() || items.empty() || items.size() > kMaxTypedBatchItems)
        return Result::failure(invalid("typed batch requires 1 to 64 items"));
      // Validate the whole declaration before publishing any route. Exact
      // scopes keep discovery's chosen versions frozen throughout this call.
      std::vector<std::string> declarations;
      declarations.reserve(items.size());
      std::set<std::pair<std::string, std::int64_t>> sources;
      const QueryLimits ceilings;
      for (const auto& item : items) {
        if (!item.first.empty()) return Result::failure(invalid("typed batch items must be an array"));
        require_object_fields(item.second, "typed batch item", {"kind", "request", "hdf5_binding"});
        if (required_string(item.second, "kind") != "typed")
          return Result::failure(invalid("typed batch accepts typed registrations only"));
        const auto& request_json = required_child(item.second, "request");
        auto declaration = parse_query_request(request_json);
        auto checked = validate_query_request(declaration);
        if (!checked.ok()) return Result::failure(checked);
        if (item.second.get_child_optional("hdf5_binding") ||
            (declaration.subset && has_hdf5_query_fields(*declaration.subset)))
          validate_hdf5_registration_json(item.second, request_json);
        if (declaration.sources.versions.kind != VersionSelectionKind::Exact ||
            !declaration.sources.versions.exact || declaration.sources.paths.empty() ||
            std::any_of(declaration.sources.paths.begin(), declaration.sources.paths.end(),
              [](const auto& path) { return path.kind != PathScopeKind::Exact; }))
          return Result::failure(invalid("typed batch requires exact source paths and versions"));
        for (const auto& path : declaration.sources.paths) {
          sources.emplace(path.path, *declaration.sources.versions.exact);
          if (sources.size() > ceilings.max_sources)
            return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
              QueryPhase::Validation, "typed batch source budget exhausted"));
        }
        std::ostringstream serialized;
        boost::property_tree::write_json(serialized, item.second, false);
        declarations.push_back(serialized.str());
      }
      ScopedQueryMetadataBudget metadata_scope;
      ScopedQueryBatchBudget batch_scope(std::make_shared<QueryBatchBudget>());
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::PublishedRoutes;
      response.items.reserve(declarations.size());
      ScopeExit rollback{[&] {
        for (const auto& item : response.items) retire_path(item.path);
      }};
      query_profile_count("typed_batch_items", declarations.size());
      for (std::size_t index = 0; index < declarations.size(); ++index) {
        std::string item_operation, item_prepared_id;
        auto item = register_json(declarations[index], peer, item_operation, item_prepared_id, false);
        if (!item.ok()) return Result::failure(item.status());
        response.items.push_back({index, std::move(item.value().path)});
        if (!metadata_scope.status(QueryPhase::Encoding).ok())
          return Result::failure(metadata_scope.status(QueryPhase::Encoding));
      }
      auto result = Result::success(response);
      if (json_response(result).size() > kMaxFrame)
        return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Encoding, "typed batch response exceeds control frame limit"));
      rollback.action = {};
      return result;
    }
    if (kind == "format_catalog") {
      require_object_fields(root, "format discovery", {"kind", "request"});
      const auto& spec = required_child(root, "request");
      require_object_fields(spec, "format discovery request", {"sources", "subset", "limits"});
      // Reuse the strict selector/subset vocabulary, without making this
      // metadata endpoint accept unknown fields or silently discard semantics.
      const auto& selector = required_child(spec, "sources");
      require_object_fields(selector, "format sources", {"paths", "versions", "formats", "file_predicate"});
      if (const auto paths = selector.get_child_optional("paths"))
        for (const auto& path : *paths)
          require_object_fields(path.second, "source path", {"kind", "path"});
      if (const auto versions = selector.get_child_optional("versions"))
        require_object_fields(*versions, "source versions", {"kind", "exact", "lower_inclusive", "upper_exclusive"});
      if (const auto formats = selector.get_child_optional("formats"))
        require_object_fields(*formats, "source formats", {"adapters", "adapter_ids", "include_byte_passthrough"});
      if (const auto limits = spec.get_child_optional("limits"))
        require_object_fields(*limits, "query limits", {"max_sources", "max_ranges", "max_layout_bytes", "max_payload_bytes", "max_result_rows", "target_batch_rows", "target_batch_bytes"});
      if (const auto subset = spec.get_child_optional("subset")) {
        require_object_fields(*subset, "format subset",
          {"projection", "predicate", "slices", "physical_array", "output"});
        if (const auto output = subset->get_child_optional("output"))
          require_object_fields(*output, "format output", {"mode", "type_overrides", "interpolation"});
      }
      if (!query_module_) return Result::failure(invalid("format discovery module is unavailable"));
      return discover_format_catalog(parse_query_request(spec), peer);
    }
    if (kind == "catalog_selector_prepare") {
      require_object_fields(root, "selector prepare", {"kind", "request"});
      const auto& spec = required_child(root, "request");
      const auto format = optional_string(spec, "format_id", "hdf5");
      const bool dataset_template = format == "hdf5";
      if (dataset_template)
        require_object_fields(spec, "dataset selector template", {"sources", "format_id", "dataset_path", "max_sources"});
      else {
        require_object_fields(spec, "format selector template", {"sources", "format_id", "subset", "limits"});
        // Predicate and all other execution intent remain request-local. A
        // format's Adapter alone interprets whether this projection is supported.
        require_object_fields(required_child(spec, "subset"), "static format subset", {"projection"});
        if (auto limits = spec.get_child_optional("limits"))
          require_object_fields(*limits, "static format limits", {"max_sources", "max_ranges", "max_layout_bytes", "max_payload_bytes", "max_result_rows", "target_batch_rows", "target_batch_bytes"});
      }
      const auto& static_sources = required_child(spec, "sources");
      if (static_sources.get_child_optional("file_predicate"))
        return Result::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
          QueryPhase::Validation, "prepared catalog selectors do not support file predicates"));
      if (!selectors_ || !prepare_selector_binding_ ||
          !(discover_ || (query_module_ && select_sources_)) ||
          (!dataset_template && (!query_module_ || !select_sources_)))
        return Result::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
          QueryPhase::Validation, "catalog selector handles require an owner discovery service"));
      auto registered = selectors_->insert(peer.uid, catalog_selector_reservation(spec), [&]() {
        using PayloadResult = QueryResult<PreparedSelectorRegistry::Payload>;
        try {
          auto prepared = std::make_shared<PreparedFrontendTemplate>();
          if (dataset_template) {
            DatasetCatalogRequest declaration;
            declaration.sources = parse_catalog_selector(static_sources);
            declaration.format_id = format;
            if (spec.get_child_optional("dataset_path")) declaration.dataset_path = required_string(spec, "dataset_path");
            declaration.max_sources = optional_number<std::size_t>(spec, "max_sources", 1024);
            auto valid = validate_dataset_catalog_request(declaration);
            if (!valid.ok()) return PayloadResult::failure(valid);
            prepared->declaration = DatasetCatalogTemplate{std::move(declaration), {}};
          } else {
            // parse_query_request compiles sources once; validate their strict
            // vocabulary first without producing a second compiled selector.
            require_object_fields(static_sources, "format sources", {"paths", "versions", "formats", "file_predicate"});
            if (auto paths = static_sources.get_child_optional("paths"))
              for (const auto& path : *paths)
                require_object_fields(path.second, "format source path", {"kind", "path"});
            if (auto versions = static_sources.get_child_optional("versions"))
              require_object_fields(*versions, "format versions", {"kind", "exact", "lower_inclusive", "upper_exclusive"});
            if (auto formats = static_sources.get_child_optional("formats"))
              require_object_fields(*formats, "format adapters", {"adapters", "adapter_ids", "include_byte_passthrough"});
            auto declaration = parse_query_request(spec);
            if (declaration.sources.formats.adapters != AdapterSelectionKind::Listed ||
                declaration.sources.formats.adapter_ids.size() != 1 ||
                declaration.sources.formats.adapter_ids.front() != format ||
                declaration.sources.formats.include_byte_passthrough)
              return PayloadResult::failure(invalid("selector format must match one explicit adapter"));
            auto valid = query_module_->validate_format_catalog_request(declaration);
            if (!valid.ok()) return PayloadResult::failure(valid);
            prepared->declaration = FormatCatalogTemplate{std::move(declaration), {}};
          }
          auto binding = prepare_selector_binding_();
          if (!binding.ok()) return PayloadResult::failure(binding.status());
          if (!binding.value()) return PayloadResult::failure(invalid("selector binding is unavailable"));
          if (auto* dataset = std::get_if<DatasetCatalogTemplate>(&prepared->declaration))
            dataset->binding = std::move(binding.value());
          else std::get<FormatCatalogTemplate>(prepared->declaration).binding = std::move(binding.value());
          return PayloadResult::success(std::move(prepared));
        } catch (const QueryMetadataBudgetExceeded& error) {
          return PayloadResult::failure(QueryStatus::failure(
            QueryError::ResourceExhausted, QueryPhase::Validation, error.what()));
        } catch (const std::bad_alloc&) {
          throw;
        } catch (const std::exception& error) {
          return PayloadResult::failure(invalid(error.what()));
        }
      });
      if (!registered.ok()) return Result::failure(registered.status());
      auto acquired = selectors_->acquire(peer.uid, registered.value());
      if (!acquired.ok()) return Result::failure(acquired.status());
      PreparedPublicationGuard rollback{selectors_.get(), peer.uid, registered.value(), acquired.value()};
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::PreparedCatalogSelector;
      response.selector_id = registered.value();
      response.prepared_payload = acquired.value();
      response.ttl_ms = selectors_->ttl_ms();
      query_profile_count("selector.prepares");
      const auto resident = selectors_->stats();
      query_profile_count("selector.resident_entries", resident.resident_entries);
      query_profile_count("selector.reserved_bytes", resident.resident_bytes);
      auto result = Result::success(std::move(response));
      rollback.published = true;
      return result;
    }
    if (kind == "catalog_selector_release") {
      require_hdf5_object_fields(root, "selector release", {"kind", "selector_id"});
      const auto id = required_string(root, "selector_id");
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::ReleasedCatalogSelector;
      if (selectors_) {
        auto acquired = selectors_->acquire(peer.uid, id);
        if (acquired.ok()) {
          const auto prepared = std::static_pointer_cast<const PreparedFrontendTemplate>(acquired.value());
          response.released = !is_query_template(*prepared) && selectors_->erase(peer.uid, id, acquired.value());
        }
      }
      query_profile_count("selector.releases");
      return Result::success(std::move(response));
    }
    if (kind == "format_catalog_by_selector") {
      require_object_fields(root, "format selector discovery", {"kind", "selector_id", "predicate"});
      if (!selectors_) return Result::failure(QueryStatus::failure(QueryError::SourceNotFound,
        QueryPhase::Validation, "prepared selector ID was not found"));
      const auto id = required_string(root, "selector_id");
      auto acquired = selectors_->acquire(peer.uid, id,
        plan && plan->observes_handle && plan->handle_id == id ? &plan->observed_payload : nullptr);
      if (!acquired.ok()) return Result::failure(acquired.status());
      const auto prepared = std::static_pointer_cast<const PreparedFrontendTemplate>(std::move(acquired.value()));
      const auto* declaration = std::get_if<FormatCatalogTemplate>(&prepared->declaration);
      if (!declaration) return Result::failure(invalid("selector does not describe format unit discovery"));
      auto request = declaration->declaration;
      request.subset->predicate = parse_expression(required_child(root, "predicate"));
      query_profile_count("selector.handle_queries");
      query_profile_count("selector.compiles", 0);
      query_profile_count("selector.input_path_items", 0);
      query_profile_count("hot_query.selector_long_key_bytes", 0);
      return discover_format_catalog(request, peer, declaration->binding);
    }
    if (kind == "dataset_catalog" || kind == "dataset_catalog_by_selector") {
      DatasetCatalogRequest request;
      std::shared_ptr<const PreparedFrontendTemplate> prepared;
      std::shared_ptr<HotQueryCache::Binding> selector_binding;
      if (kind == "dataset_catalog_by_selector") {
        require_hdf5_object_fields(root, "selector discovery",
          {"kind", "selector_id", "predicate", "max_datasets", "max_catalog_bytes"});
        if (!selectors_) return Result::failure(QueryStatus::failure(
          QueryError::SourceNotFound, QueryPhase::Validation, "prepared selector ID was not found"));
        auto acquired = selectors_->acquire(peer.uid, required_string(root, "selector_id"));
        if (!acquired.ok()) return Result::failure(acquired.status());
        prepared = std::static_pointer_cast<const PreparedFrontendTemplate>(std::move(acquired.value()));
        const auto* declaration = std::get_if<DatasetCatalogTemplate>(&prepared->declaration);
        if (!declaration) return Result::failure(invalid("selector does not describe dataset discovery"));
        request = declaration->declaration;
        selector_binding = declaration->binding;
        request.predicate = parse_expression(required_child(root, "predicate"));
        if (!Hdf5GlobalAttributeIndex::supports(request.predicate))
          return Result::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
            QueryPhase::Validation, "prepared catalog discovery requires supported complete equality predicates"));
        request.max_datasets = optional_number<std::size_t>(root, "max_datasets", request.max_datasets);
        request.max_catalog_bytes = optional_number<std::uint64_t>(root, "max_catalog_bytes", request.max_catalog_bytes);
        query_profile_count("selector.handle_queries");
        query_profile_count("selector.compiles", 0);
        query_profile_count("selector.input_path_items", 0);
        query_profile_count("hot_query.selector_long_key_bytes", 0);
      } else {
        require_hdf5_object_fields(root, "dataset discovery", {"kind", "request"});
        const auto& spec = required_child(root, "request");
        require_hdf5_object_fields(spec, "dataset discovery request",
          {"sources", "format_id", "dataset_path", "predicate", "time_range", "max_sources", "max_datasets", "max_catalog_bytes"});
        request.sources = parse_catalog_selector(required_child(spec, "sources"));
        request.format_id = optional_string(spec, "format_id", "hdf5");
        if (spec.get_child_optional("dataset_path")) request.dataset_path = required_string(spec, "dataset_path");
        if (auto expression = spec.get_child_optional("predicate")) request.predicate = parse_expression(*expression);
        if (auto range = spec.get_child_optional("time_range")) {
          require_hdf5_object_fields(*range, "dataset UTC time range", {"lower_unix_s", "upper_unix_s"});
          request.time_range = DatasetCatalogRequest::UtcTimeRange{
            required_number<std::int64_t>(*range, "lower_unix_s"), required_number<std::int64_t>(*range, "upper_unix_s")};
        }
        request.max_sources = optional_number<std::size_t>(spec, "max_sources", 1024);
        request.max_datasets = optional_number<std::size_t>(spec, "max_datasets", 1024);
        request.max_catalog_bytes = optional_number<std::uint64_t>(spec, "max_catalog_bytes", request.max_catalog_bytes);
      }
      auto valid = validate_dataset_catalog_request(request);
      if (!valid.ok()) return Result::failure(valid);
      DatasetCatalogAuthorizer authorize = [&](const std::vector<QuerySourceReference>& sources,
          const CatalogSelection& selection) {
        return authorize_ ? authorize_(peer, sources, &selection) : QueryStatus::success();
      };
      auto found = discover_ ? discover_(request, authorize, selector_binding) :
        discover_datasets_reference(*catalog_, request,
          [&](const StructuredBlobManifest& manifest) {
            auto artifact = storage_->load_dataset_catalog(manifest.key);
            if (!artifact.ok()) return QueryResult<DatasetCatalogArtifact>::failure(
              QueryStatus::failure(artifact.status().code() == StructuredBlobError::NotFound ?
                QueryError::LayoutUnavailable : QueryError::ReadFailure, QueryPhase::Storage, artifact.status().message()));
            return QueryResult<DatasetCatalogArtifact>::success(std::move(artifact.value()));
          }, authorize);
      if (!found.ok()) return Result::failure(found.status());
      QueryControlResponse response;
      response.kind = QueryControlResponse::Kind::DatasetCatalog;
      response.datasets = std::move(found.value());
      auto result = Result::success(std::move(response));
      if (json_response(result).size() > kMaxFrame)
        return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Encoding, "dataset discovery response exceeds control frame limit"));
      return result;
    }
    if (kind == "window_execute" || kind == "window_release" || kind == "window_result_release") {
      std::set<std::string> seen_fields;
      for (const auto& field : root) {
        if (!seen_fields.insert(field.first).second)
          return Result::failure(invalid("duplicate prepared operation field"));
        if (field.first != "kind" && field.first != "prepared_id" &&
            !(kind == "window_execute" && field.first == "request") &&
            !(kind == "window_result_release" && field.first == "path"))
          return Result::failure(invalid("unexpected field in prepared window operation"));
      }
      prepared_id = required_string(root, "prepared_id");
      QueryControlResponse response;
      if (kind == "window_release") {
        response.kind = QueryControlResponse::Kind::ReleasedWindow;
        response.released = prepared_->erase(prepared_id, peer.uid);
        return Result::success(std::move(response));
      }
      if (kind == "window_result_release") {
        response.kind = QueryControlResponse::Kind::ReleasedResult;
        response.released = prepared_->release_result(prepared_id, peer.uid, required_string(root, "path"));
        return Result::success(std::move(response));
      }
      auto module = prepared_->acquire(prepared_id, peer.uid);
      if (!module.ok()) return Result::failure(module.status());
      auto request = parse_window_request(required_child(root, "request"));
      auto registered = frontend_->register_windows(std::move(module.value()), std::move(request));
      if (!registered.ok()) return Result::failure(registered.status());
      auto published = publish_route(std::move(registered.value()), peer);
      if (!published.ok()) return Result::failure(published.status());
      auto tracked = prepared_->track_route(prepared_id, peer.uid, published.value());
      if (!tracked.ok()) { retire_path(published.value()); return Result::failure(tracked); }
      return path_response(std::move(published));
    }
    if (kind == "typed") {
      const auto& request_json = required_child(root, "request");
      auto declaration = [&] {
        QueryProfileTimer timer("request_parse");
        return parse_query_request(request_json);
      }();
      const auto binding = root.get_child_optional("hdf5_binding");
      if (binding || (declaration.subset && has_hdf5_query_fields(*declaration.subset)))
        validate_hdf5_registration_json(root, request_json);
      auto checked = [&] {
        QueryProfileTimer timer("request_validate");
        return validate_query_request(declaration);
      }();
      if (!checked.ok()) return QueryResult<QueryControlResponse>::failure(checked);
      return execute_validated_typed({std::move(declaration), binding ? &*binding : nullptr}, peer);
    }
    if (kind == "window" || kind == "window_prepare") {
      // Preparation reads every shard and expands semantic bindings: serialize
      // this bounded, expensive phase independently of execute workers.
      std::unique_lock<std::mutex> preparation_lock(preparation_mutex_, std::defer_lock);
      {
        QueryProfileTimer timer("preparation_queue_wait");
        preparation_lock.lock();
      }
      if (kind == "window_prepare" && root.get_child_optional("request"))
        return Result::failure(invalid("window_prepare does not accept a window request"));
      if (const auto configured = root.get_child_optional("execution")) {
        if (configured->get_child_optional("read_parallelism") ||
            configured->get_child_optional("reuse_prepared_validation") ||
            configured->get_child_optional("compiled_transform"))
          return QueryResult<QueryControlResponse>::failure(invalid(
            "training execution optimization is a server-owned setting"));
      }
      auto snapshot_request = [&] {
        QueryProfileTimer timer("snapshot_parse");
        return parse_snapshot(required_child(root, "snapshot"));
      }();
      auto view = [&] {
        QueryProfileTimer timer("view_parse");
        return parse_view(required_child(root, "view"));
      }();
      auto request = [&] {
        QueryProfileTimer timer("request_parse");
        return kind == "window" ? parse_window_request(required_child(root, "request")) : WindowRequest{};
      }();
      auto limits = root.get_child_optional("limits")
        ? parse_training_limits(required_child(root, "limits"))
        : TrainingQueryLimits{};
      auto checked = [&] {
        QueryProfileTimer timer("request_validate");
        return kind == "window" ? validate_window_request(request, limits) : QueryStatus::success();
      }();
      if (!checked.ok()) return QueryResult<QueryControlResponse>::failure(checked);
      auto prepared = [&] {
        QueryProfileTimer timer("snapshot_prepare");
        return prepare_dataset_snapshot(snapshot_request, *catalog_, storage_, 1024,
          QueryPrincipal{peer.uid, peer.gid, peer.supplementary_groups});
      }();
      if (!prepared.ok()) return QueryResult<QueryControlResponse>::failure(prepared.status());
      if (authorize_ && !prepared.value().read_context) {
        QueryProfileTimer timer("snapshot_authorize");
        std::vector<QuerySourceReference> sources;
        for (const auto& manifest : prepared.value().manifests)
          sources.push_back({manifest.key.path, manifest.key.version,
            manifest.format_id.value_or("")});
        checked = authorize_(peer, sources, nullptr);
        if (!checked.ok()) return QueryResult<QueryControlResponse>::failure(checked);
      }
      if (view.snapshot_fingerprint.empty())
        view.snapshot_fingerprint = prepared.value().fingerprint;
      std::shared_ptr<QueryExecutionLease> preparation_lease;
      std::unique_ptr<ScopedQueryExecutionLease> execution_scope;
      if (prepared.value().read_context) {
        auto admitted = prepared.value().read_context->admit(
          {peer.uid, peer.gid, peer.supplementary_groups});
        if (!admitted.ok()) return Result::failure(admitted.status());
        preparation_lease = admitted.value()->execution_lease();
        if (preparation_lease) execution_scope = std::make_unique<ScopedQueryExecutionLease>(preparation_lease);
      }
      auto index = [&] {
        QueryProfileTimer timer("coverage_index_prepare");
        return prepare_dataset_coverage_index(prepared.value(), storage_);
      }();
      if (!index.ok()) return QueryResult<QueryControlResponse>::failure(index.status());
      auto options = root.get_child_optional("options")
        ? parse_execution_options(required_child(root, "options"))
        : QueryExecutionOptions{};
      TrainingExecutionOptions execution = training_execution_;
      if (const auto configured = root.get_child_optional("execution")) {
        execution.window_group_size = optional_number<std::uint64_t>(*configured,
          "window_group_size", execution.window_group_size);
        execution.prefetch = optional_bool(*configured, "prefetch", execution.prefetch);
      }
      auto module = [&] {
        QueryProfileTimer timer("module_prepare");
        return TrainingQueryModule::prepare(std::move(prepared.value()),
          std::move(index.value()), storage_, runtime_, std::move(view),
          limits, options, {}, execution);
      }();
      if (!module.ok()) return QueryResult<QueryControlResponse>::failure(module.status());
      if (kind == "window_prepare") {
        // Registry budgeting/eviction may destroy large snapshots; do it
        // before the short namespace gate and expose the ID only afterward.
        auto registered = prepared_->insert(peer.uid, module.value());
        if (!registered.ok()) return Result::failure(registered.status());
        try {
          QueryControlResponse response;
          response.kind = QueryControlResponse::Kind::PreparedWindow;
          response.prepared_id = registered.value();
          prepared_id = response.prepared_id;
          response.snapshot_fingerprint = module.value()->snapshot().fingerprint;
          response.ttl_ms = prepared_->ttl_ms();
          std::shared_ptr<QueryAdmission> admission;
          if (module.value()->snapshot().read_context) {
            auto admitted = module.value()->snapshot().read_context->admit(
              {peer.uid, peer.gid, peer.supplementary_groups});
            if (!admitted.ok()) {
              prepared_->erase(registered.value(), peer.uid);
              return Result::failure(admitted.status());
            }
            admission = std::move(admitted.value());
            if (auto budget = current_query_metadata_budget(); budget && !budget->status(QueryPhase::Encoding).ok()) {
              admission.reset();
              prepared_->erase(registered.value(), peer.uid);
              return Result::failure(budget->status(QueryPhase::Encoding));
            }
          }
          return Result::success(std::move(response));
        } catch (...) {
          prepared_->erase(registered.value(), peer.uid);
          throw;
        }
      }
      preparation_lock.unlock();
      auto registered = [&] {
        QueryProfileTimer timer("route_register");
        return frontend_->register_windows(std::move(module.value()),
          std::move(request));
      }();
      if (!registered.ok()) return Result::failure(registered.status());
      return path_response(publish_route(std::move(registered.value()), peer));
    }
    return QueryResult<QueryControlResponse>::failure(invalid("unknown registration kind"));
  } catch (const QueryMetadataBudgetExceeded& error) {
    return QueryResult<QueryControlResponse>::failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Encoding, error.what()));
  } catch (const std::exception& error) {
    if (auto budget = current_query_metadata_budget(); budget && !budget->status(QueryPhase::Encoding).ok())
      return QueryResult<QueryControlResponse>::failure(budget->status(QueryPhase::Encoding));
    return QueryResult<QueryControlResponse>::failure(invalid(error.what()));
  }
}

QueryResult<QueryControlResponse> submit_query_control(
    const std::string& socket_path, const std::string& json_request,
    std::chrono::milliseconds response_timeout) {
  if (json_request.empty() || json_request.size() > kMaxFrame)
    return QueryResult<QueryControlResponse>::failure(invalid("registration frame length is invalid"));
  if (socket_path.empty() || socket_path.size() >= sizeof(sockaddr_un::sun_path))
    return QueryResult<QueryControlResponse>::failure(invalid("socket path is invalid"));
  if (response_timeout <= std::chrono::milliseconds::zero() ||
      response_timeout > std::chrono::hours(24))
    return QueryResult<QueryControlResponse>::failure(invalid(
      "registration response timeout must be between 1 ms and 24 hours"));
  const auto fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return QueryResult<QueryControlResponse>::failure(io_error("cannot create client socket"));
  if (!set_socket_timeout(fd, SO_SNDTIMEO, std::chrono::seconds(3)) ||
      !set_socket_timeout(fd, SO_RCVTIMEO, response_timeout)) {
    ::close(fd);
    return QueryResult<QueryControlResponse>::failure(io_error(
      "cannot configure registration socket timeout"));
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    const auto message = std::string("cannot connect to registration socket: ") +
      std::strerror(errno);
    ::close(fd);
    return QueryResult<QueryControlResponse>::failure(io_error(message));
  }
  if (!write_frame(fd, json_request)) {
    ::close(fd);
    return QueryResult<QueryControlResponse>::failure(io_error("cannot send registration request"));
  }
  std::string response;
  const auto received = read_frame(fd, response);
  ::close(fd);
  if (!received) return QueryResult<QueryControlResponse>::failure(io_error(
    "cannot receive registration response"));
  try {
    std::istringstream input(response);
    ptree root;
    boost::property_tree::read_json(input, root);
    if (optional_bool(root, "ok", false)) {
      QueryControlResponse result;
      const auto kind = required_string(root, "kind");
      if (kind == "published_route") result.path = required_string(root, "path");
      else if (kind == "published_routes") {
        result.kind = QueryControlResponse::Kind::PublishedRoutes;
        result.items = array<QueryControlResponse::Item>(root, "items", [](const ptree& item) {
          return QueryControlResponse::Item{required_number<std::size_t>(item, "index"),
            required_string(item, "path")};
        });
        if (result.items.empty() || result.items.size() > kMaxTypedBatchItems)
          throw std::invalid_argument("invalid typed batch response length");
        for (std::size_t i = 0; i < result.items.size(); ++i)
          if (result.items[i].index != i || result.items[i].path.empty())
            throw std::invalid_argument("invalid typed batch response index or path");
      }
      else if (kind == "prepared_window") {
        result.kind = QueryControlResponse::Kind::PreparedWindow;
        result.prepared_id = required_string(root, "prepared_id");
        result.snapshot_fingerprint = required_string(root, "snapshot_fingerprint");
        result.ttl_ms = required_number<std::uint64_t>(root, "ttl_ms");
      } else if (kind == "prepared_catalog_selector") {
        result.kind = QueryControlResponse::Kind::PreparedCatalogSelector;
        result.selector_id = required_string(root, "selector_id");
        result.ttl_ms = required_number<std::uint64_t>(root, "idle_ttl_ms");
      } else if (kind == "prepared_query_template") {
        result.kind = QueryControlResponse::Kind::PreparedQueryTemplate;
        result.template_id = required_string(root, "template_id");
        result.ttl_ms = required_number<std::uint64_t>(root, "idle_ttl_ms");
      } else if (kind == "released_query_template") {
        result.kind = QueryControlResponse::Kind::ReleasedQueryTemplate;
        result.released = required_bool(root, "released");
      } else if (kind == "released_catalog_selector") {
        result.kind = QueryControlResponse::Kind::ReleasedCatalogSelector;
        result.released = required_bool(root, "released");
      } else if (kind == "format_catalog") {
        result.kind = QueryControlResponse::Kind::FormatCatalog;
        result.format_units.units = array<FormatCatalogMatch>(root, "units", [](const ptree& item) {
          FormatCatalogMatch match;
          match.source = {required_string(item, "path"), required_number<std::int64_t>(item, "version"), required_string(item, "format_id")};
          match.revision = required_number<std::uint64_t>(item, "revision");
          match.unit = {required_string(item, "unit_id"), required_number<std::uint64_t>(item, "offset"), required_number<std::uint64_t>(item, "length")};
          if (match.source.path.empty() || match.source.version < 0 || match.source.format_id.empty() ||
              match.unit.unit_id.empty() || !match.unit.length ||
              match.unit.offset > UINT64_MAX - match.unit.length)
            throw std::invalid_argument("invalid format unit location");
          return match;
        });
      } else if (kind == "dataset_catalog") {
        result.kind = QueryControlResponse::Kind::DatasetCatalog;
        result.datasets.selected_sources = required_number<std::uint64_t>(root, "selected_sources");
        result.datasets.time_candidate_sources =
          required_number<std::uint64_t>(root, "time_candidate_sources");
        result.datasets.candidate_datasets = required_number<std::uint64_t>(root, "candidate_datasets");
        result.datasets.sql_queries = required_number<std::uint64_t>(root, "sql_queries");
        result.datasets.catalog_read_bytes = required_number<std::uint64_t>(root, "catalog_read_bytes");
        result.datasets.datasets = array<DatasetCatalogMatch>(root, "datasets", [](const ptree& item) {
          DatasetCatalogMatch match;
          match.source = {required_string(item, "path"), required_number<std::int64_t>(item, "version"), required_string(item, "format_id")};
          match.revision = required_number<std::uint64_t>(item, "revision");
          match.object_id = required_string(item, "object_id");
          match.dataset_path = required_string(item, "dataset_path");
          match.layout_digest = required_string(item, "layout_digest");
          match.layout_protocol = required_string(item, "layout_protocol");
          return match;
        });
      } else if (kind == "released_window" || kind == "released_result") {
        result.kind = kind == "released_window" ? QueryControlResponse::Kind::ReleasedWindow : QueryControlResponse::Kind::ReleasedResult;
        result.released = required_bool(root, "released");
      } else throw std::invalid_argument("unknown control response kind");
      return QueryResult<QueryControlResponse>::success(std::move(result));
    }
    const auto code = required_number<int>(root, "code");
    const auto phase = required_number<int>(root, "phase");
    if (code <= static_cast<int>(QueryError::None) ||
        code > static_cast<int>(QueryError::Internal) ||
        phase < static_cast<int>(QueryPhase::None) ||
        phase > static_cast<int>(QueryPhase::Encoding))
      throw std::invalid_argument("invalid response status");
    return QueryResult<QueryControlResponse>::failure(QueryStatus::failure(
      static_cast<QueryError>(code), static_cast<QueryPhase>(phase),
      required_string(root, "message")));
  } catch (const std::exception& error) {
    return QueryResult<QueryControlResponse>::failure(io_error(
      std::string("invalid registration response: ") + error.what()));
  }
}

QueryResult<std::string> submit_query_registration(const std::string& socket_path,
    const std::string& json_request, std::chrono::milliseconds response_timeout) {
  // Reject non-path operations before sending: otherwise a caller using this
  // convenience API could create a prepared ID it cannot receive or release.
  try {
    std::istringstream input(json_request);
    ptree declaration;
    boost::property_tree::read_json(input, declaration);
    const auto kind = required_string(declaration, "kind");
    if (kind != "typed" && kind != "window" && kind != "window_execute")
      return QueryResult<std::string>::failure(invalid("operation requires submit_query_control"));
  } catch (const std::exception& error) {
    return QueryResult<std::string>::failure(invalid(error.what()));
  }
  auto result = submit_query_control(socket_path, json_request, response_timeout);
  if (!result.ok()) return QueryResult<std::string>::failure(result.status());
  if (result.value().kind != QueryControlResponse::Kind::PublishedRoute)
    return QueryResult<std::string>::failure(invalid("control operation did not return a published route"));
  return QueryResult<std::string>::success(std::move(result.value().path));
}
} // namespace iotdbfs
