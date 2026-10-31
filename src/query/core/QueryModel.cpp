#include "iotdbfs/query/core/QueryModel.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>
#include <type_traits>
#include <unordered_set>

namespace iotdbfs {
namespace {

QueryStatus validation_failure(QueryError code, std::string message,
    std::optional<std::string> field_id = std::nullopt) {
  QueryErrorDetail detail;
  detail.field_id = std::move(field_id);
  return QueryStatus::failure(code, QueryPhase::Validation,
    std::move(message), std::move(detail));
}

QueryStatus validate_type(const LogicalType& type,
    const std::optional<std::string>& field_id = std::nullopt) {
  if (type.id != LogicalTypeId::TimestampNs && type.timezone)
    return validation_failure(QueryError::SchemaMismatch,
      "timezone is valid only for timestamp fields", field_id);
  if (type.timezone && type.timezone->empty())
    return validation_failure(QueryError::SchemaMismatch,
      "timestamp timezone cannot be empty", field_id);
  return QueryStatus::success();
}

bool is_finite_literal(const LogicalValue& value) {
  if (value.is_null()) return true;
  if (value.type() == LogicalTypeId::Float32)
    return std::isfinite(std::get<float>(value.value()));
  if (value.type() == LogicalTypeId::Float64)
    return std::isfinite(std::get<double>(value.value()));
  return true;
}

bool supports_order(LogicalTypeId type) {
  return type != LogicalTypeId::Boolean && type != LogicalTypeId::Binary;
}

bool is_order_comparison(ComparisonOperator operation) {
  return operation != ComparisonOperator::Equal &&
    operation != ComparisonOperator::NotEqual;
}

QueryStatus validate_field_value(const FieldRef& reference,
    const LogicalValue& value, const LogicalSchema& schema,
    bool allow_null) {
  if (reference.field_id.empty())
    return validation_failure(QueryError::InvalidQuery,
      "field reference cannot be empty");
  const auto* field = schema.find(reference.field_id);
  if (!field)
    return validation_failure(QueryError::UnknownField,
      "unknown query field: " + reference.field_id, reference.field_id);
  if (value.is_null() && !allow_null)
    return validation_failure(QueryError::InvalidQuery,
      "null literals require an IS NULL expression", reference.field_id);
  if (field->type.id != value.type())
    return validation_failure(QueryError::TypeMismatch,
      "query literal type does not match field type", reference.field_id);
  if (!is_finite_literal(value))
    return validation_failure(QueryError::InvalidQuery,
      "query literals must be finite", reference.field_id);
  return QueryStatus::success();
}

struct ExpressionValidationState {
  std::unordered_set<const QueryExpression*> active;
  std::size_t nodes{ 0 };
};

QueryStatus validate_expression_node(const QueryExpressionPtr& expression,
    const LogicalSchema& schema, ExpressionValidationState& state,
    std::size_t depth) {
  if (!expression)
    return validation_failure(QueryError::InvalidQuery,
      "query expression contains a null node");
  if (depth > 64)
    return validation_failure(QueryError::InvalidQuery,
      "query expression exceeds depth limit");
  if (++state.nodes > 1024)
    return validation_failure(QueryError::InvalidQuery,
      "query expression exceeds node limit");
  if (!state.active.insert(expression.get()).second)
    return validation_failure(QueryError::InvalidQuery,
      "query expression contains a cycle");

  QueryStatus status = std::visit([&](const auto& node) -> QueryStatus {
    using Node = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<Node, ComparisonExpression>) {
      auto checked = validate_field_value(node.field, node.value, schema, false);
      if (!checked.ok()) return checked;
      const auto* field = schema.find(node.field.field_id);
      if (is_order_comparison(node.operation) &&
          !supports_order(field->type.id))
        return validation_failure(QueryError::UnsupportedCapability,
          "field type does not support ordered comparison",
          node.field.field_id);
      return QueryStatus::success();
    } else if constexpr (std::is_same_v<Node, NullExpression>) {
      if (node.field.field_id.empty())
        return validation_failure(QueryError::InvalidQuery,
          "field reference cannot be empty");
      if (!schema.find(node.field.field_id))
        return validation_failure(QueryError::UnknownField,
          "unknown query field: " + node.field.field_id,
          node.field.field_id);
      return QueryStatus::success();
    } else if constexpr (std::is_same_v<Node, InExpression>) {
      if (node.values.empty())
        return validation_failure(QueryError::InvalidQuery,
          "IN expression requires at least one literal",
          node.field.field_id);
      for (const auto& value : node.values) {
        auto checked = validate_field_value(node.field, value, schema, false);
        if (!checked.ok()) return checked;
      }
      return QueryStatus::success();
    } else {
      const std::size_t required = node.operation == LogicalOperator::Not ? 1 : 2;
      if ((node.operation == LogicalOperator::Not &&
           node.operands.size() != required) ||
          (node.operation != LogicalOperator::Not &&
           node.operands.size() < required))
        return validation_failure(QueryError::InvalidQuery,
          node.operation == LogicalOperator::Not
            ? "NOT expression requires exactly one operand"
            : "AND/OR expression requires at least two operands");
      for (const auto& operand : node.operands) {
        auto checked = validate_expression_node(operand, schema, state,
          depth + 1);
        if (!checked.ok()) return checked;
      }
      return QueryStatus::success();
    }
  }, expression->node);

  state.active.erase(expression.get());
  return status;
}

template<typename T>
int compare_values(const T& left, const T& right) {
  if (left < right) return -1;
  if (right < left) return 1;
  return 0;
}

int compare_logical_values(const LogicalValue& left,
    const LogicalValue& right) {
  switch (left.type()) {
  case LogicalTypeId::Int32:
    return compare_values(std::get<std::int32_t>(left.value()),
      std::get<std::int32_t>(right.value()));
  case LogicalTypeId::Int64:
    return compare_values(std::get<std::int64_t>(left.value()),
      std::get<std::int64_t>(right.value()));
  case LogicalTypeId::UInt32:
    return compare_values(std::get<std::uint32_t>(left.value()),
      std::get<std::uint32_t>(right.value()));
  case LogicalTypeId::UInt64:
    return compare_values(std::get<std::uint64_t>(left.value()),
      std::get<std::uint64_t>(right.value()));
  case LogicalTypeId::Float32:
    return compare_values(std::get<float>(left.value()),
      std::get<float>(right.value()));
  case LogicalTypeId::Float64:
    return compare_values(std::get<double>(left.value()),
      std::get<double>(right.value()));
  case LogicalTypeId::Utf8:
    return compare_values(std::get<std::string>(left.value()),
      std::get<std::string>(right.value()));
  case LogicalTypeId::TimestampNs:
    return compare_values(std::get<TimestampNs>(left.value()).value,
      std::get<TimestampNs>(right.value()).value);
  case LogicalTypeId::Boolean:
  case LogicalTypeId::Binary:
    break;
  }
  return 0;
}

LogicalTypeId array_type_id(const LogicalArrayData& values) {
  switch (values.index()) {
  case 0: return LogicalTypeId::Boolean;
  case 1: return LogicalTypeId::Int32;
  case 2: return LogicalTypeId::Int64;
  case 3: return LogicalTypeId::UInt32;
  case 4: return LogicalTypeId::UInt64;
  case 5: return LogicalTypeId::Float32;
  case 6: return LogicalTypeId::Float64;
  case 7: return LogicalTypeId::Utf8;
  case 8: return LogicalTypeId::Binary;
  case 9: return LogicalTypeId::TimestampNs;
  default: return LogicalTypeId::Binary;
  }
}

bool validity_bit(const std::vector<std::uint8_t>& validity,
    std::size_t index) {
  return (validity[index / 8] &
    static_cast<std::uint8_t>(1u << (index % 8))) != 0;
}

QueryStatus validate_domain_descriptor(const DomainDescriptor& domain,
    std::uint64_t row_count, const LogicalSchema& schema) {
  if (domain.dimensions.empty())
    return validation_failure(QueryError::SchemaMismatch,
      "batch domain requires at least one dimension");
  if (domain.shape.size() != domain.dimensions.size())
    return validation_failure(QueryError::SchemaMismatch,
      "batch domain shape must match its dimensions");
  if (!domain.origin.empty() &&
      domain.origin.size() != domain.dimensions.size())
    return validation_failure(QueryError::SchemaMismatch,
      "batch domain origin must match its dimensions");

  std::set<std::string> ids;
  // Any zero extent makes the rectangle empty even when earlier nonzero
  // extents would overflow their intermediate product. Still validate every
  // dimension below; emptiness does not excuse malformed axis metadata.
  std::uint64_t cells = std::find(domain.shape.begin(), domain.shape.end(), 0) !=
    domain.shape.end() ? 0 : 1;
  for (std::size_t i = 0; i < domain.dimensions.size(); ++i) {
    const auto& dimension = domain.dimensions[i];
    if (dimension.field.field_id.empty() ||
        !ids.insert(dimension.field.field_id).second)
      return validation_failure(QueryError::SchemaMismatch,
        "batch domain dimension ids must be non-empty and unique",
        dimension.field.field_id);
    auto type_status = validate_type(dimension.type,
      dimension.field.field_id);
    if (!type_status.ok()) return type_status;
    if (dimension.type.nullable)
      return validation_failure(QueryError::SchemaMismatch,
        "batch domain dimensions cannot be nullable",
        dimension.field.field_id);
    const auto* projected = schema.find(dimension.field.field_id);
    if (projected && projected->type.id != dimension.type.id)
      return validation_failure(QueryError::SchemaMismatch,
        "batch domain type conflicts with projected field",
        dimension.field.field_id);
    if (domain.shape[i] != 0 &&
        cells > std::numeric_limits<std::uint64_t>::max() / domain.shape[i])
      return validation_failure(QueryError::SchemaMismatch,
        "batch domain shape overflows row count");
    cells *= domain.shape[i];
  }
  if (cells != row_count)
    return validation_failure(QueryError::SchemaMismatch,
      "batch domain shape does not match row count");
  return QueryStatus::success();
}

} // namespace

QueryStatus::QueryStatus(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail)
  : code_(code), phase_(phase), message_(std::move(message)),
    detail_(std::move(detail)) {}

QueryStatus QueryStatus::failure(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail) {
  if (code == QueryError::None)
    return QueryStatus(QueryError::Internal, phase,
      "failure status requires a non-success error", std::move(detail));
  return QueryStatus(code, phase, std::move(message), std::move(detail));
}

LogicalValue LogicalValue::null(LogicalTypeId type) {
  return LogicalValue(type, std::monostate{});
}
LogicalValue LogicalValue::boolean(bool value) {
  return LogicalValue(LogicalTypeId::Boolean, value);
}
LogicalValue LogicalValue::int32(std::int32_t value) {
  return LogicalValue(LogicalTypeId::Int32, value);
}
LogicalValue LogicalValue::int64(std::int64_t value) {
  return LogicalValue(LogicalTypeId::Int64, value);
}
LogicalValue LogicalValue::uint32(std::uint32_t value) {
  return LogicalValue(LogicalTypeId::UInt32, value);
}
LogicalValue LogicalValue::uint64(std::uint64_t value) {
  return LogicalValue(LogicalTypeId::UInt64, value);
}
LogicalValue LogicalValue::float32(float value) {
  return LogicalValue(LogicalTypeId::Float32, value);
}
LogicalValue LogicalValue::float64(double value) {
  return LogicalValue(LogicalTypeId::Float64, value);
}
LogicalValue LogicalValue::utf8(std::string value) {
  return LogicalValue(LogicalTypeId::Utf8, std::move(value));
}
LogicalValue LogicalValue::binary(BinaryValue value) {
  return LogicalValue(LogicalTypeId::Binary, std::move(value));
}
LogicalValue LogicalValue::timestamp_ns(std::int64_t value) {
  return LogicalValue(LogicalTypeId::TimestampNs, TimestampNs{ value });
}

QueryResult<LogicalSchema> LogicalSchema::create(
    std::vector<LogicalField> fields) {
  std::set<std::string> ids;
  for (const auto& field : fields) {
    if (field.field_id.empty())
      return QueryResult<LogicalSchema>::failure(validation_failure(
        QueryError::SchemaMismatch, "schema field id cannot be empty"));
    if (!ids.insert(field.field_id).second)
      return QueryResult<LogicalSchema>::failure(validation_failure(
        QueryError::SchemaMismatch, "duplicate schema field: " +
        field.field_id, field.field_id));
    auto type_status = validate_type(field.type, field.field_id);
    if (!type_status.ok())
      return QueryResult<LogicalSchema>::failure(std::move(type_status));
    if (field.unit && field.unit->empty())
      return QueryResult<LogicalSchema>::failure(validation_failure(
        QueryError::SchemaMismatch, "field unit cannot be empty",
        field.field_id));
  }
  return QueryResult<LogicalSchema>::success(
    LogicalSchema(std::move(fields)));
}

const LogicalField* LogicalSchema::find(const std::string& field_id) const {
  const auto index = index_of(field_id);
  return index ? &fields_[*index] : nullptr;
}

std::optional<std::size_t> LogicalSchema::index_of(
    const std::string& field_id) const {
  for (std::size_t i = 0; i < fields_.size(); ++i)
    if (fields_[i].field_id == field_id) return i;
  return std::nullopt;
}

QueryExpressionPtr make_comparison(FieldRef field,
    ComparisonOperator operation, LogicalValue value) {
  return std::make_shared<const QueryExpression>(
    ComparisonExpression{ std::move(field), operation, std::move(value) });
}

QueryExpressionPtr make_is_null(FieldRef field, bool negated) {
  return std::make_shared<const QueryExpression>(
    NullExpression{ std::move(field), negated });
}

QueryExpressionPtr make_in(FieldRef field,
    std::vector<LogicalValue> values, bool negated) {
  return std::make_shared<const QueryExpression>(
    InExpression{ std::move(field), std::move(values), negated });
}

QueryExpressionPtr make_logical(LogicalOperator operation,
    std::vector<QueryExpressionPtr> operands) {
  return std::make_shared<const QueryExpression>(
    LogicalExpression{ operation, std::move(operands) });
}

QueryStatus validate_expression(const QueryExpressionPtr& expression,
    const LogicalSchema& schema) {
  ExpressionValidationState state;
  return validate_expression_node(expression, schema, state, 0);
}

QueryStatus validate_domain_slice(const DomainSlice& slice,
    const LogicalSchema& schema) {
  if (slice.dimension.field_id.empty())
    return validation_failure(QueryError::InvalidDomainSlice,
      "domain dimension cannot be empty");
  const auto* dimension = schema.find(slice.dimension.field_id);
  if (!dimension)
    return validation_failure(QueryError::UnknownField,
      "unknown domain dimension: " + slice.dimension.field_id,
      slice.dimension.field_id);
  if (dimension->role != FieldRole::Dimension)
    return validation_failure(QueryError::InvalidDomainSlice,
      "domain slice field is not a dimension", slice.dimension.field_id);
  if (!supports_order(dimension->type.id))
    return validation_failure(QueryError::InvalidDomainSlice,
      "domain dimension type is not ordered", slice.dimension.field_id);
  if (!slice.lower && !slice.upper)
    return validation_failure(QueryError::InvalidDomainSlice,
      "domain slice requires a lower or upper bound",
      slice.dimension.field_id);

  for (const auto* bound : { &slice.lower, &slice.upper }) {
    if (!*bound) continue;
    const auto checked = validate_field_value(slice.dimension,
      (*bound)->value, schema, false);
    if (!checked.ok())
      return validation_failure(checked.code(), checked.message(),
        slice.dimension.field_id);
  }
  if (slice.lower && slice.upper &&
      compare_logical_values(slice.lower->value, slice.upper->value) > 0)
    return validation_failure(QueryError::InvalidDomainSlice,
      "domain lower bound exceeds upper bound", slice.dimension.field_id);
  return QueryStatus::success();
}

std::size_t logical_array_length(const LogicalArray& array) {
  return std::visit([](const auto& values) { return values.size(); },
    array.values);
}

QueryStatus validate_logical_array(const LogicalArray& array) {
  auto type_status = validate_type(array.type);
  if (!type_status.ok()) return type_status;
  if (array.type.id != array_type_id(array.values))
    return validation_failure(QueryError::TypeMismatch,
      "logical array storage does not match its declared type");

  const auto length = logical_array_length(array);
  if (!array.validity.empty()) {
    const auto expected = (length + 7) / 8;
    if (array.validity.size() != expected)
      return validation_failure(QueryError::SchemaMismatch,
        "logical array validity bitmap has the wrong size");
    if (!array.type.nullable) {
      for (std::size_t i = 0; i < length; ++i) {
        if (!validity_bit(array.validity, i))
          return validation_failure(QueryError::SchemaMismatch,
            "non-nullable logical array contains a null value");
      }
    }
  }
  return QueryStatus::success();
}

QueryResult<QueryBatch> QueryBatch::create(LogicalSchema schema,
    std::uint64_t row_count, std::vector<LogicalArray> columns,
    std::optional<DomainDescriptor> domain,
    std::vector<QuerySourceReference> sources) {
  if (schema.fields().size() != columns.size())
    return QueryResult<QueryBatch>::failure(validation_failure(
      QueryError::SchemaMismatch,
      "batch column count does not match schema"));

  for (std::size_t i = 0; i < columns.size(); ++i) {
    auto status = validate_logical_array(columns[i]);
    if (!status.ok()) {
      QueryErrorDetail detail = status.detail();
      detail.field_id = schema.fields()[i].field_id;
      return QueryResult<QueryBatch>::failure(QueryStatus::failure(
        status.code(), status.phase(), status.message(), std::move(detail)));
    }
    if (columns[i].type != schema.fields()[i].type)
      return QueryResult<QueryBatch>::failure(validation_failure(
        QueryError::TypeMismatch,
        "batch column type does not match schema field",
        schema.fields()[i].field_id));
    if (logical_array_length(columns[i]) != row_count)
      return QueryResult<QueryBatch>::failure(validation_failure(
        QueryError::SchemaMismatch,
        "batch column length does not match row count",
        schema.fields()[i].field_id));
  }

  if (domain) {
    auto status = validate_domain_descriptor(*domain, row_count, schema);
    if (!status.ok())
      return QueryResult<QueryBatch>::failure(std::move(status));
  }
  for (const auto& source : sources) {
    if (source.path.empty())
      return QueryResult<QueryBatch>::failure(validation_failure(
        QueryError::SchemaMismatch,
        "batch provenance source path cannot be empty"));
  }

  return QueryResult<QueryBatch>::success(QueryBatch(std::move(schema),
    row_count, std::move(columns), std::move(domain), std::move(sources)));
}

} // namespace iotdbfs
