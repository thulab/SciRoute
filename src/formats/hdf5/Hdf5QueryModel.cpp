#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"

#include <algorithm>
#include <array>
#include <set>
#include <functional>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>

namespace iotdbfs {
namespace {
QueryStatus fail(QueryError code, std::string message,
    QueryErrorDetail detail = {}) {
  return QueryStatus::failure(code, QueryPhase::Validation,
    std::move(message), std::move(detail));
}
bool starts(const std::string& text, const char* prefix) {
  return text.compare(0, std::char_traits<char>::length(prefix), prefix) == 0;
}
bool unreserved(unsigned char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
    (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~';
}
bool valid_utf8(const std::string& value) {
  for (std::size_t i = 0; i < value.size();) {
    const auto first = static_cast<unsigned char>(value[i++]);
    if (first < 0x80) continue;
    std::uint32_t point = 0;
    std::size_t continuation = 0;
    std::uint32_t minimum = 0;
    if (first >= 0xC2 && first <= 0xDF) {
      point = first & 0x1F; continuation = 1; minimum = 0x80;
    } else if (first >= 0xE0 && first <= 0xEF) {
      point = first & 0x0F; continuation = 2; minimum = 0x800;
    } else if (first >= 0xF0 && first <= 0xF4) {
      point = first & 7; continuation = 3; minimum = 0x10000;
    } else return false;
    if (continuation > value.size() - i) return false;
    while (continuation--) {
      auto byte = static_cast<unsigned char>(value[i++]);
      if ((byte & 0xC0) != 0x80) return false;
      point = (point << 6) | (byte & 0x3F);
    }
    if (point < minimum || point > 0x10FFFF ||
        (point >= 0xD800 && point <= 0xDFFF)) return false;
  }
  return true;
}
QueryStatus validate_path(const std::string& path) {
  if (path.size() > kHdf5MaxDatasetPathBytes)
    return fail(QueryError::ResourceExhausted, "HDF5 dataset path exceeds byte limit");
  if (path.size() < 2 || path.front() != '/' || path.back() == '/' ||
      path.find('\0') != std::string::npos || !valid_utf8(path))
    return fail(QueryError::InvalidQuery, "HDF5 dataset requires an absolute non-root UTF-8 path");
  std::size_t begin = 1;
  while (begin < path.size()) {
    auto end = path.find('/', begin);
    if (end == std::string::npos) end = path.size();
    auto length = end - begin;
    if (!length || (length == 1 && path[begin] == '.') ||
        (length == 2 && path[begin] == '.' && path[begin + 1] == '.'))
      return fail(QueryError::InvalidQuery, "HDF5 dataset path has an empty or dot component");
    begin = end + 1;
  }
  return QueryStatus::success();
}
int hex_digit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}
QueryResult<std::size_t> parse_axis(const FieldRef& field) {
  const std::string prefix = kHdf5AxisFieldPrefix;
  if (field.field_id.size() != prefix.size() + 1 ||
      !starts(field.field_id, kHdf5AxisFieldPrefix) ||
      field.field_id.back() < '0' || field.field_id.back() > '3')
    return QueryResult<std::size_t>::failure(fail(QueryError::UnknownField,
      "unknown HDF5 ordinal axis field"));
  return QueryResult<std::size_t>::success(field.field_id.back() - '0');
}
QueryResult<std::uint64_t> product(const std::vector<std::uint64_t>& shape) {
  if (std::find(shape.begin(), shape.end(), 0) != shape.end())
    return QueryResult<std::uint64_t>::success(0);
  std::uint64_t n = 1;
  for (auto extent : shape) {
    if (extent > std::numeric_limits<std::uint64_t>::max() / n)
      return QueryResult<std::uint64_t>::failure(fail(QueryError::ResourceExhausted,
        "HDF5 element count overflows UInt64"));
    n *= extent;
  }
  return QueryResult<std::uint64_t>::success(n);
}
QueryStatus byte_count(std::uint64_t count, std::uint64_t width) {
  if (width && count > std::numeric_limits<std::uint64_t>::max() / width)
    return fail(QueryError::ResourceExhausted, "HDF5 output byte count overflows UInt64");
  return QueryStatus::success();
}
QueryStatus validate_limits(const QueryLimits& limits) {
  if (!limits.max_ranges || !limits.max_result_rows || !limits.target_batch_rows ||
      !limits.target_batch_bytes)
    return fail(QueryError::InvalidQuery, "HDF5 query limits must be positive");
  return QueryStatus::success();
}
bool cf_coordinate(const std::string& field) {
  return field == kHdf5TimeField || field == kHdf5LatitudeField ||
    field == kHdf5LongitudeField || field == kHdf5PressureField;
}
std::string coordinate_role(const std::string& field) {
  return field == kHdf5TimeField ? "time" : field == kHdf5LatitudeField ? "latitude" :
    field == kHdf5LongitudeField ? "longitude" : "pressure";
}
struct Grammar { FieldRef value; std::string path; };
QueryResult<Grammar> grammar(const QueryFragment& f, const QueryLimits& limits) {
  auto bad = [](QueryError code, const char* message) {
    return QueryResult<Grammar>::failure(fail(code, message));
  };
  auto checked = validate_limits(limits);
  if (!checked.ok()) return QueryResult<Grammar>::failure(checked);
  if (f.physical_array)
    return bad(QueryError::InvalidQuery, "HDF5 fields cannot be combined with an Arrow physical array subject");
  if (f.output.mode != QueryFragment::ResultMode::LogicalValues ||
      !f.output.type_overrides.empty() || f.output.interpolation)
    return bad(QueryError::UnsupportedCapability, "HDF5 supports native or CF logical values and coordinate slices only");
  if (f.projection.empty() || f.projection.size() > 8)
    return bad(QueryError::InvalidQuery, "HDF5 projection requires one value and bounded coordinates");
  if (f.slices.size() > limits.max_ranges)
    return bad(QueryError::ResourceExhausted, "HDF5 slice count exceeds request range limit");
  if (f.slices.size() > 4)
    return bad(QueryError::InvalidDomainSlice, "HDF5 query has more than four coordinate slices");
  std::unordered_set<const QueryExpression*> active;
  std::size_t expression_nodes = 0;
  std::function<QueryStatus(const QueryExpressionPtr&, std::size_t)> predicate_shape;
  predicate_shape = [&](const QueryExpressionPtr& expr, std::size_t depth) -> QueryStatus {
    if (!expr || depth > 32 || ++expression_nodes > 128 || !active.insert(expr.get()).second)
      return fail(QueryError::InvalidQuery, "HDF5 attribute expression is cyclic or exceeds bounds");
    auto status = std::visit([&](const auto& node) -> QueryStatus {
      using T = std::decay_t<decltype(node)>;
      if constexpr (std::is_same_v<T, LogicalExpression>) {
        for (const auto& operand : node.operands) {
          auto valid = predicate_shape(operand, depth + 1);
          if (!valid.ok()) return valid;
        }
      } else if (!starts(node.field.field_id, "hdf5.attr:"))
        return fail(QueryError::UnsupportedCapability, "HDF5 predicates require dataset attribute fields");
      return QueryStatus::success();
    }, expr->node);
    active.erase(expr.get()); return status;
  };
  if (f.predicate) {
    auto valid = predicate_shape(f.predicate, 0);
    if (!valid.ok()) return QueryResult<Grammar>::failure(valid);
  }
  Grammar g;
  std::unordered_set<std::string> seen;
  for (const auto& field : f.projection) {
    if (!seen.insert(field.field_id).second)
      return bad(QueryError::InvalidQuery, "duplicate HDF5 projected field");
    if (starts(field.field_id, kHdf5ValueFieldPrefix) || is_hdf5_science_value_field(field)) {
      if (!g.path.empty()) return bad(QueryError::InvalidQuery, "HDF5 query must bind exactly one dataset");
      auto path = parse_hdf5_value_field(field);
      if (!path.ok()) return QueryResult<Grammar>::failure(path.status());
      g.value = field; g.path = std::move(path.value());
    } else if (cf_coordinate(field.field_id)) {}
    else if (field.field_id != kHdf5GpsSecondsField) {
      auto axis = parse_axis(field);
      if (!axis.ok()) return QueryResult<Grammar>::failure(axis.status());
    }
  }
  if (g.path.empty()) return bad(QueryError::InvalidQuery, "HDF5 query requires a dataset value field in its projection");
  seen.clear();
  for (const auto& slice : f.slices) {
    if (!seen.insert(slice.dimension.field_id).second)
      return bad(QueryError::InvalidDomainSlice, "duplicate HDF5 coordinate slice");
    if (!slice.lower && !slice.upper)
      return bad(QueryError::InvalidDomainSlice, "HDF5 axis slice requires at least one bound");
    const bool cf = cf_coordinate(slice.dimension.field_id);
    const bool real = cf || slice.dimension.field_id == kHdf5GpsSecondsField;
    if (!real) {
      auto axis = parse_axis(slice.dimension);
      if (!axis.ok()) return QueryResult<Grammar>::failure(axis.status());
    }
    auto bound = [real, &slice](const std::optional<DomainBound>& b, BoundKind kind) {
      if (!b) return QueryStatus::success();
      QueryErrorDetail detail;
      detail.field_id = slice.dimension.field_id;
      detail.operation = "hdf5.validate_slice";
      if (b->value.type() != (real ? LogicalTypeId::Float64 : LogicalTypeId::UInt64) || b->value.is_null())
        return fail(QueryError::TypeMismatch,
          "HDF5 slice endpoints must match their coordinate type", detail);
      if (real && !std::isfinite(std::get<double>(b->value.value())))
        return fail(QueryError::InvalidDomainSlice,
          "HDF5 coordinate slice endpoints must be finite", detail);
      if (b->kind != kind)
        return fail(QueryError::InvalidDomainSlice,
          "HDF5 slices require inclusive lower and exclusive upper bounds", detail);
      return QueryStatus::success();
    };
    auto lower = bound(slice.lower, BoundKind::Inclusive);
    if (!lower.ok()) return QueryResult<Grammar>::failure(lower);
    auto upper = bound(slice.upper, BoundKind::Exclusive);
    if (!upper.ok()) return QueryResult<Grammar>::failure(upper);
    if (slice.lower && slice.upper &&
        (real ? std::get<double>(slice.lower->value.value()) > std::get<double>(slice.upper->value.value())
              : std::get<std::uint64_t>(slice.lower->value.value()) > std::get<std::uint64_t>(slice.upper->value.value())))
      return bad(QueryError::InvalidDomainSlice, "HDF5 slice lower bound exceeds upper bound; cyclic longitude ranges are not implicit");
  }
  // Validated coordinates can select raw values without CF mask/scale.
  return QueryResult<Grammar>::success(std::move(g));
}
} // namespace

QueryResult<FieldRef> make_hdf5_value_field(const std::string& path) {
  auto status = validate_path(path);
  if (!status.ok()) return QueryResult<FieldRef>::failure(status);
  const char* hex = "0123456789ABCDEF";
  std::string encoded = kHdf5ValueFieldPrefix;
  for (unsigned char c : path) {
    if (unreserved(c)) encoded.push_back(c);
    else { encoded.push_back('%'); encoded.push_back(hex[c >> 4]); encoded.push_back(hex[c & 15]); }
  }
  return QueryResult<FieldRef>::success({std::move(encoded)});
}
QueryResult<FieldRef> make_hdf5_attribute_field(const std::string& name) {
  if (name.empty() || name.size() > kHdf5MaxDatasetPathBytes || name.find('\0') != std::string::npos || !valid_utf8(name))
    return QueryResult<FieldRef>::failure(fail(QueryError::InvalidQuery, "HDF5 attribute name must be bounded nonempty UTF-8"));
  constexpr char hex[] = "0123456789ABCDEF";
  std::string encoded = "hdf5.attr:";
  for (unsigned char c : name) {
    if (unreserved(c)) encoded.push_back(c);
    else { encoded.push_back('%'); encoded.push_back(hex[c >> 4]); encoded.push_back(hex[c & 15]); }
  }
  return QueryResult<FieldRef>::success({std::move(encoded)});
}
QueryResult<FieldRef> make_hdf5_science_value_field(const std::string& path) {
  auto field = make_hdf5_value_field(path);
  if (field.ok()) field.value().field_id.replace(0, std::char_traits<char>::length(kHdf5ValueFieldPrefix), kHdf5ScienceValueFieldPrefix);
  return field;
}
bool is_hdf5_science_value_field(const FieldRef& field) {
  return starts(field.field_id, kHdf5ScienceValueFieldPrefix);
}
QueryResult<std::string> parse_hdf5_value_field(const FieldRef& field) {
  if (!starts(field.field_id, kHdf5ValueFieldPrefix) && !is_hdf5_science_value_field(field))
    return QueryResult<std::string>::failure(fail(QueryError::UnknownField,
      "not an HDF5 dataset value field"));
  const std::size_t prefix = std::char_traits<char>::length(is_hdf5_science_value_field(field) ? kHdf5ScienceValueFieldPrefix : kHdf5ValueFieldPrefix);
  if (field.field_id.size() > prefix + 3 * kHdf5MaxDatasetPathBytes)
    return QueryResult<std::string>::failure(fail(QueryError::ResourceExhausted,
      "encoded HDF5 field exceeds path byte limit"));
  std::string path;
  for (std::size_t i = prefix; i < field.field_id.size(); ++i) {
    auto c = static_cast<unsigned char>(field.field_id[i]);
    if (unreserved(c)) path.push_back(c);
    else if (c == '%' && i + 2 < field.field_id.size() &&
        hex_digit(field.field_id[i + 1]) >= 0 && hex_digit(field.field_id[i + 2]) >= 0) {
      auto byte = static_cast<unsigned char>((hex_digit(field.field_id[i + 1]) << 4) |
        hex_digit(field.field_id[i + 2]));
      if (unreserved(byte))
        return QueryResult<std::string>::failure(fail(QueryError::InvalidQuery,
          "unreserved HDF5 path bytes must not be percent encoded"));
      path.push_back(byte); i += 2;
    } else return QueryResult<std::string>::failure(fail(QueryError::InvalidQuery,
      "HDF5 value field is not canonically percent encoded"));
  }
  auto status = validate_path(path);
  if (!status.ok()) return QueryResult<std::string>::failure(status);
  return QueryResult<std::string>::success(std::move(path));
}
FieldRef hdf5_axis_field(std::size_t axis) {
  if (axis >= 4) throw std::out_of_range("HDF5 ordinal axis must be 0..3");
  return {std::string(kHdf5AxisFieldPrefix) + std::to_string(axis)};
}

bool has_hdf5_query_fields(const QueryFragment& f) {
  auto is_hdf5 = [](const FieldRef& field) { return starts(field.field_id, "hdf5."); };
  for (const auto& field : f.projection) if (is_hdf5(field)) return true;
  for (const auto& slice : f.slices) if (is_hdf5(slice.dimension)) return true;
  for (const auto& item : f.output.type_overrides) if (is_hdf5(item.field)) return true;
  if (f.output.interpolation)
    for (const auto& axis : f.output.interpolation->axes) if (is_hdf5(axis.dimension)) return true;
  std::vector<QueryExpressionPtr> pending;
  if (f.predicate) pending.push_back(f.predicate);
  std::unordered_set<const QueryExpression*> visited;
  while (!pending.empty()) {
    auto expression = std::move(pending.back()); pending.pop_back();
    if (!expression || !visited.insert(expression.get()).second) continue;
    if (auto logical = std::get_if<LogicalExpression>(&expression->node)) {
      pending.insert(pending.end(), logical->operands.begin(), logical->operands.end());
    } else {
      bool found = std::visit([&](const auto& node) {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, LogicalExpression>) return false;
        else return is_hdf5(node.field);
      }, expression->node);
      if (found) return true;
    }
  }
  return false;
}
QueryStatus validate_hdf5_query_request(const QueryRequest& request) {
  if (!request.subset || !has_hdf5_query_fields(*request.subset)) return QueryStatus::success();
  if (request.sources.paths.size() != 1 ||
      request.sources.paths[0].kind != PathScopeKind::Exact ||
      request.sources.paths[0].path.empty() ||
      request.sources.versions.kind != VersionSelectionKind::Exact ||
      !request.sources.versions.exact)
    return fail(QueryError::InvalidQuery, "HDF5 subset requires one exact source path and version");
  auto checked = grammar(*request.subset, request.limits);
  return checked.ok() ? QueryStatus::success() : checked.status();
}

Hdf5QueryBinding::Hdf5QueryBinding(Hdf5DatasetDescriptor descriptor,
    FieldRef value_field, LogicalSchema schema, std::vector<FieldRef> projection,
    std::vector<std::uint64_t> origin, std::vector<std::uint64_t> shape,
    std::uint64_t row_count, std::uint64_t projected_row_bytes,
    std::optional<Hdf5GpsTimeAxis> gps_axis, std::optional<Hdf5CfBinding> cf_binding)
  : descriptor_(std::move(descriptor)), value_field_(std::move(value_field)),
    schema_(std::move(schema)), projection_(std::move(projection)),
    origin_(std::move(origin)), shape_(std::move(shape)), row_count_(row_count),
    projected_row_bytes_(projected_row_bytes), gps_axis_(std::move(gps_axis)), cf_binding_(std::move(cf_binding)) {}

QueryResult<Hdf5QueryBinding> bind_hdf5_query(const Hdf5DatasetProvider& provider,
    const QueryFragment& fragment, const QueryLimits& limits,
    std::optional<Hdf5GpsTimeAxis> gps_axis, std::optional<Hdf5CfBinding> cf_binding,
    std::vector<LogicalField> attribute_fields, bool predicate_matches) {
  auto g = grammar(fragment, limits);
  if (!g.ok()) return QueryResult<Hdf5QueryBinding>::failure(g.status());
  auto described = provider.describe(g.value().path);
  if (!described.ok()) return QueryResult<Hdf5QueryBinding>::failure(described.status());
  auto descriptor = std::move(described.value());
  if (descriptor.path != g.value().path)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::SchemaMismatch,
      "HDF5 provider returned a different dataset path"));
  if (!descriptor.supported)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnsupportedCapability,
      descriptor.unsupported_reason.empty() ? "HDF5 dataset profile is unsupported" : descriptor.unsupported_reason));
  if (descriptor.shape.empty() || descriptor.shape.size() > 4 ||
      (descriptor.value_type != LogicalTypeId::Float32 && descriptor.value_type != LogicalTypeId::Float64 && descriptor.value_type != LogicalTypeId::Int32))
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnsupportedCapability,
      "HDF5 requires rank 1..4 native Float32/Float64 or Int16"));
  const bool science = is_hdf5_science_value_field(g.value().value);
  if (science && !cf_binding)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnsupportedCapability, "scientific HDF5 values require a validated CF binding"));
  if (science && gps_axis)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::InvalidQuery, "CF and GPS binding cannot be mixed"));
  const bool requests_gps =
    std::any_of(fragment.projection.begin(), fragment.projection.end(), [](const FieldRef& f) { return f.field_id == kHdf5GpsSecondsField; }) ||
    std::any_of(fragment.slices.begin(), fragment.slices.end(), [](const DomainSlice& s) { return s.dimension.field_id == kHdf5GpsSecondsField; });
  if (requests_gps && !gps_axis)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnsupportedCapability,
      "GPS coordinates require an explicit validated source binding"));
  if (gps_axis && (descriptor.shape.size() != 1 || gps_axis->count() != descriptor.shape[0]))
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::SchemaMismatch,
      "GPS coordinate length differs from rank-one dataset"));
  auto all_rows = product(descriptor.shape);
  if (!all_rows.ok()) return QueryResult<Hdf5QueryBinding>::failure(all_rows.status());
  const std::uint64_t value_bytes = science ? 9 : descriptor.value_type == LogicalTypeId::Float64 ? 8 : 4;
  auto full_bytes = byte_count(all_rows.value(), value_bytes);
  if (!full_bytes.ok()) return QueryResult<Hdf5QueryBinding>::failure(full_bytes);
  std::vector<LogicalField> fields{{g.value().value.field_id, descriptor.path,
    {science ? LogicalTypeId::Float64 : descriptor.value_type, science, std::nullopt}, FieldRole::Measure, science && !cf_binding->units.empty() ? std::optional<std::string>(cf_binding->units) : std::nullopt}};
  for (std::size_t i = 0; i < descriptor.shape.size(); ++i)
    fields.push_back({hdf5_axis_field(i).field_id, "Axis " + std::to_string(i),
      {LogicalTypeId::UInt64, false, std::nullopt}, FieldRole::Dimension, std::nullopt});
  if (gps_axis) fields.push_back({kHdf5GpsSecondsField, "GPS time",
    {LogicalTypeId::Float64, false, std::nullopt}, FieldRole::Dimension, "second"});
  if (cf_binding) {
    if (!std::isfinite(cf_binding->scale_factor) || !std::isfinite(cf_binding->add_offset) ||
        (cf_binding->valid_min && !std::isfinite(*cf_binding->valid_min)) ||
        (cf_binding->valid_max && !std::isfinite(*cf_binding->valid_max)) ||
        (cf_binding->valid_min && cf_binding->valid_max && *cf_binding->valid_min > *cf_binding->valid_max))
      return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::SchemaMismatch, "invalid CF numeric transform"));
    std::set<std::uint32_t> dimensions;
    std::set<std::string> roles;
    for (const auto& axis : cf_binding->axes) {
      if (axis.dimension >= descriptor.shape.size() || axis.values.size() != descriptor.shape[axis.dimension] ||
          !dimensions.insert(axis.dimension).second || !roles.insert(axis.role).second ||
          (axis.role != "time" && axis.role != "latitude" &&
           axis.role != "longitude" && axis.role != "pressure"))
        return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::SchemaMismatch, "invalid CF coordinate binding"));
      for (std::size_t i = 0; i < axis.values.size(); ++i)
        if (!std::isfinite(axis.values[i]) || (i && (axis.ascending ? axis.values[i] <= axis.values[i-1] : axis.values[i] >= axis.values[i-1])))
          return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::SchemaMismatch, "CF coordinate is not strictly monotonic and finite"));
      const auto name = axis.role == "time" ? kHdf5TimeField : axis.role == "latitude" ? kHdf5LatitudeField :
        axis.role == "longitude" ? kHdf5LongitudeField : kHdf5PressureField;
      fields.push_back({name, axis.role, {LogicalTypeId::Float64, false, std::nullopt}, FieldRole::Dimension, axis.units});
    }
  }
  fields.insert(fields.end(), attribute_fields.begin(), attribute_fields.end());
  auto schema = LogicalSchema::create(std::move(fields));
  if (!schema.ok()) return QueryResult<Hdf5QueryBinding>::failure(schema.status());
  if (fragment.predicate) {
    auto predicate_valid = validate_expression(fragment.predicate, schema.value());
    if (!predicate_valid.ok()) return QueryResult<Hdf5QueryBinding>::failure(predicate_valid);
  }
  std::uint64_t row_bytes = 0;
  for (const auto& field : fragment.projection) {
    if (!schema.value().find(field.field_id))
      return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnknownField,
        "HDF5 projected axis exceeds dataset rank"));
    row_bytes += field.field_id == g.value().value.field_id ? value_bytes : 8;
  }
  std::vector<std::uint64_t> origin(descriptor.shape.size(), 0), shape = descriptor.shape;
  std::set<std::size_t> selected_axes;
  for (const auto& slice : fragment.slices) {
    if (cf_coordinate(slice.dimension.field_id)) {
      if (!cf_binding) return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnsupportedCapability, "CF coordinate is not bound"));
      const auto found = std::find_if(cf_binding->axes.begin(), cf_binding->axes.end(), [&](const auto& axis) { return axis.role == coordinate_role(slice.dimension.field_id); });
      if (found == cf_binding->axes.end()) return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnknownField, "requested CF coordinate is not bound"));
      if (!selected_axes.insert(found->dimension).second) return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::InvalidDomainSlice, "multiple slices bind the same HDF5 axis"));
      const auto& v = found->values;
      auto begin = v.begin(), end = v.end();
      if (found->ascending) {
        if (slice.lower) begin = std::lower_bound(v.begin(), v.end(), std::get<double>(slice.lower->value.value()));
        if (slice.upper) end = std::lower_bound(v.begin(), v.end(), std::get<double>(slice.upper->value.value()));
      } else {
        if (slice.upper) begin = std::upper_bound(v.begin(), v.end(), std::get<double>(slice.upper->value.value()), std::greater<double>());
        if (slice.lower) end = std::upper_bound(v.begin(), v.end(), std::get<double>(slice.lower->value.value()), std::greater<double>());
      }
      origin[found->dimension] = std::distance(v.begin(), begin);
      shape[found->dimension] = std::distance(begin, end);
      continue;
    }
    if (slice.dimension.field_id == kHdf5GpsSecondsField) {
      if (!selected_axes.insert(0).second) return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::InvalidDomainSlice, "multiple slices bind the same HDF5 axis"));
      const auto lower = slice.lower ? gps_axis->lower_bound(std::get<double>(slice.lower->value.value())) : 0;
      const auto upper = slice.upper ? gps_axis->lower_bound(std::get<double>(slice.upper->value.value())) : gps_axis->count();
      origin[0] = lower; shape[0] = upper - lower;
      continue;
    }
    auto axis = parse_axis(slice.dimension).value();
    if (!selected_axes.insert(axis).second) return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::InvalidDomainSlice, "multiple slices bind the same HDF5 axis"));
    if (axis >= shape.size())
      return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::UnknownField,
        "HDF5 sliced axis exceeds dataset rank"));
    const auto lower = slice.lower ? std::get<std::uint64_t>(slice.lower->value.value()) : 0;
    const auto upper = slice.upper ? std::get<std::uint64_t>(slice.upper->value.value()) : descriptor.shape[axis];
    if (lower > upper || upper > descriptor.shape[axis])
      return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::InvalidDomainSlice,
        "HDF5 slice exceeds dataset extent"));
    origin[axis] = lower; shape[axis] = upper - lower;
  }
  if (!predicate_matches) shape.front() = 0;
  auto rows = product(shape);
  if (!rows.ok()) return QueryResult<Hdf5QueryBinding>::failure(rows.status());
  if (rows.value() > limits.max_result_rows)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::ResourceExhausted,
      "HDF5 selection exceeds result row limit"));
  auto bytes = byte_count(rows.value(), row_bytes);
  if (!bytes.ok()) return QueryResult<Hdf5QueryBinding>::failure(bytes);
  if (rows.value() && row_bytes > limits.target_batch_bytes)
    return QueryResult<Hdf5QueryBinding>::failure(fail(QueryError::ResourceExhausted,
      "HDF5 batch byte target cannot hold one projected row"));
  return QueryResult<Hdf5QueryBinding>::success(Hdf5QueryBinding(std::move(descriptor),
    std::move(g.value().value), std::move(schema.value()), fragment.projection,
    std::move(origin), std::move(shape), rows.value(), row_bytes, std::move(gps_axis), std::move(cf_binding)));
}
QueryResult<QueryCapabilities> make_hdf5_query_capabilities(const Hdf5QueryBinding& binding) {
  std::vector<FieldQueryCapability> fields;
  for (const auto& field : binding.schema().fields()) {
    const bool axis = field.role == FieldRole::Dimension;
    const bool attr = field.role == FieldRole::Attribute;
    const std::vector<ComparisonOperator> comparisons = attr ? std::vector<ComparisonOperator>{ComparisonOperator::Equal, ComparisonOperator::NotEqual, ComparisonOperator::Less, ComparisonOperator::LessEqual, ComparisonOperator::Greater, ComparisonOperator::GreaterEqual} : std::vector<ComparisonOperator>{};
    fields.push_back({{field.field_id}, !attr, comparisons, attr ? PushdownLevel::Exact : PushdownLevel::None, axis,
      axis ? PushdownLevel::Exact : PushdownLevel::None, {}, false, attr,
      {QueryFragment::ResultMode::LogicalValues}});
  }
  QueryCapabilities result(1, "hdf5", "typed-query-1", binding.schema(),
    std::move(fields), {DomainKind::DenseGrid}, false, false,
    OrderingGuarantee::Domain);
  auto status = validate_query_capabilities(result);
  if (!status.ok()) return QueryResult<QueryCapabilities>::failure(status);
  return QueryResult<QueryCapabilities>::success(std::move(result));
}

Hdf5SlabIterator::Hdf5SlabIterator(const Hdf5QueryBinding& binding, std::uint64_t capacity,
    const std::vector<std::uint64_t>& alignment)
  : origin_(binding.origin()), shape_(binding.shape()), position_(shape_.size(), 0),
    capacity_(capacity), done_(binding.row_count() == 0) {
  pivot_ = shape_.size() - 1;
  if (done_) return;
  while (pivot_ > 0 && shape_[pivot_] <= capacity_ / suffix_rows_) {
    suffix_rows_ *= shape_[pivot_]; --pivot_;
  }
  if (!alignment.empty()) alignment_ = alignment[pivot_];
}
QueryResult<Hdf5SlabIterator> Hdf5SlabIterator::create(
    const Hdf5QueryBinding& binding, const QueryLimits& limits,
    const std::vector<std::uint64_t>& alignment) {
  auto status = validate_limits(limits);
  if (!status.ok()) return QueryResult<Hdf5SlabIterator>::failure(status);
  if (binding.row_count() > limits.max_result_rows)
    return QueryResult<Hdf5SlabIterator>::failure(fail(QueryError::ResourceExhausted,
      "HDF5 selection exceeds iterator row limit"));
  auto capacity = std::min<std::uint64_t>(limits.target_batch_rows,
    limits.target_batch_bytes / binding.projected_row_bytes());
  if (!capacity && binding.row_count())
    return QueryResult<Hdf5SlabIterator>::failure(fail(QueryError::ResourceExhausted,
      "HDF5 batch byte target cannot hold one projected row"));
  if (!alignment.empty() && (alignment.size() != binding.shape().size() ||
      std::any_of(alignment.begin(), alignment.end(), [](auto size) { return !size; })))
    return QueryResult<Hdf5SlabIterator>::failure(fail(QueryError::CorruptLayout,
      "HDF5 slab alignment is invalid"));
  return QueryResult<Hdf5SlabIterator>::success(Hdf5SlabIterator(binding, capacity, alignment));
}
QueryResult<std::optional<Hdf5Slab>> Hdf5SlabIterator::next() {
  if (done_) return QueryResult<std::optional<Hdf5Slab>>::success(std::nullopt);
  Hdf5Slab slab;
  slab.domain.kind = DomainKind::DenseGrid;
  slab.domain.origin.resize(shape_.size());
  slab.domain.shape.assign(shape_.size(), 1);
  for (std::size_t i = 0; i < shape_.size(); ++i) {
    slab.domain.dimensions.push_back({hdf5_axis_field(i),
      {LogicalTypeId::UInt64, false, std::nullopt}, std::nullopt});
    slab.domain.origin[i] = origin_[i] + position_[i];
    if (i > pivot_) slab.domain.shape[i] = shape_[i];
  }
  slab.domain.shape[pivot_] = std::min(shape_[pivot_] - position_[pivot_], capacity_ / suffix_rows_);
  // Native H5Dread may otherwise decode a boundary chunk again in the next
  // slab when its raw chunk cache is disabled. End at the last chunk boundary
  // within the original cap, including a short head for an unaligned ROI.
  // If even the first boundary does not fit, retain the bounded original split.
  if (alignment_ > 1 && slab.domain.shape[pivot_] < shape_[pivot_] - position_[pivot_]) {
    const auto start = slab.domain.origin[pivot_];
    const auto end = start + slab.domain.shape[pivot_];
    const auto boundary = (end / alignment_) * alignment_;
    if (boundary > start) slab.domain.shape[pivot_] = boundary - start;
  }
  slab.row_count = slab.domain.shape[pivot_] * suffix_rows_;
  position_[pivot_] += slab.domain.shape[pivot_];
  for (std::size_t i = pivot_ + 1; i-- > 0;) {
    if (position_[i] < shape_[i]) break;
    position_[i] = 0;
    if (i == 0) { done_ = true; break; }
    ++position_[i - 1];
  }
  return QueryResult<std::optional<Hdf5Slab>>::success(std::move(slab));
}
} // namespace iotdbfs
