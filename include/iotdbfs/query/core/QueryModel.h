#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace iotdbfs {

// Query failures are classified independently from storage and format-library
// failures so every frontend observes the same semantics.
enum class QueryError {
  None,
  InvalidQuery,
  PermissionDenied,
  UnknownField,
  TypeMismatch,
  SchemaMismatch,
  InvalidDomainSlice,
  SourceNotFound,
  SourceNotPublished,
  AdapterUnavailable,
  UnsupportedCapability,
  LayoutUnavailable,
  UnsupportedLayoutVersion,
  CorruptLayout,
  CorruptPayload,
  ReadFailure,
  ResourceExhausted,
  Cancelled,
  StalePlan,
  DeadlineExceeded,
  Internal,
};

enum class QueryPhase {
  None,
  Validation,
  Catalog,
  Planning,
  Storage,
  Execution,
  Encoding,
};

struct QuerySourceReference {
  std::string path;
  std::int64_t version{ 0 };
  std::string format_id;
};

struct QueryErrorDetail {
  std::optional<QuerySourceReference> source;
  std::optional<std::string> field_id;
  std::optional<std::string> operation;
  bool retriable{ false };
};

class QueryStatus {
public:
  QueryStatus() = default;

  static QueryStatus success() { return {}; }
  static QueryStatus failure(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail = {});

  bool ok() const { return code_ == QueryError::None; }
  QueryError code() const { return code_; }
  QueryPhase phase() const { return phase_; }
  const std::string& message() const { return message_; }
  const QueryErrorDetail& detail() const { return detail_; }

private:
  QueryStatus(QueryError code, QueryPhase phase, std::string message,
    QueryErrorDetail detail);

  QueryError code_{ QueryError::None };
  QueryPhase phase_{ QueryPhase::None };
  std::string message_;
  QueryErrorDetail detail_;
};

template<typename T>
class QueryResult {
public:
  static QueryResult success(T value) {
    QueryResult result;
    result.value_ = std::move(value);
    return result;
  }

  static QueryResult failure(QueryStatus status) {
    QueryResult result;
    result.status_ = status.ok()
      ? QueryStatus::failure(QueryError::Internal, QueryPhase::None,
          "failure result requires a failure status")
      : std::move(status);
    return result;
  }

  bool ok() const { return status_.ok(); }
  const QueryStatus& status() const { return status_; }
  const T& value() const { return *value_; }
  T& value() { return *value_; }

private:
  QueryStatus status_;
  std::optional<T> value_;
};

enum class LogicalTypeId {
  Boolean,
  Int32,
  Int64,
  UInt32,
  UInt64,
  Float32,
  Float64,
  Utf8,
  Binary,
  TimestampNs,
};

struct LogicalType {
  LogicalTypeId id{ LogicalTypeId::Binary };
  bool nullable{ false };
  std::optional<std::string> timezone;

  friend bool operator==(const LogicalType& left, const LogicalType& right) {
    return left.id == right.id && left.nullable == right.nullable &&
      left.timezone == right.timezone;
  }
  friend bool operator!=(const LogicalType& left, const LogicalType& right) {
    return !(left == right);
  }
};

struct TimestampNs {
  std::int64_t value{ 0 };

  friend bool operator==(TimestampNs left, TimestampNs right) {
    return left.value == right.value;
  }
};

using BinaryValue = std::vector<std::uint8_t>;
// Immutable result descriptors use format-owned keys and opaque UTF-8 values.
// Transport layers scope these per prepared source and enforce encoding limits.
using QueryResultMetadata = std::vector<std::pair<std::string, std::string>>;
using LogicalValueStorage = std::variant<std::monostate, bool, std::int32_t,
  std::int64_t, std::uint32_t, std::uint64_t, float, double, std::string,
  BinaryValue, TimestampNs>;

class LogicalValue {
public:
  static LogicalValue null(LogicalTypeId type);
  static LogicalValue boolean(bool value);
  static LogicalValue int32(std::int32_t value);
  static LogicalValue int64(std::int64_t value);
  static LogicalValue uint32(std::uint32_t value);
  static LogicalValue uint64(std::uint64_t value);
  static LogicalValue float32(float value);
  static LogicalValue float64(double value);
  static LogicalValue utf8(std::string value);
  static LogicalValue binary(BinaryValue value);
  static LogicalValue timestamp_ns(std::int64_t value);

  LogicalTypeId type() const { return type_; }
  bool is_null() const {
    return std::holds_alternative<std::monostate>(value_);
  }
  const LogicalValueStorage& value() const { return value_; }

private:
  LogicalValue(LogicalTypeId type, LogicalValueStorage value)
    : type_(type), value_(std::move(value)) {}

  LogicalTypeId type_;
  LogicalValueStorage value_;
};

enum class FieldRole { Attribute, Dimension, Measure, SourceIdentity };

struct FieldRef {
  std::string field_id;
};

struct LogicalField {
  std::string field_id;
  std::string display_name;
  LogicalType type;
  FieldRole role{ FieldRole::Attribute };
  std::optional<std::string> unit;
};

class LogicalSchema {
public:
  static QueryResult<LogicalSchema> create(std::vector<LogicalField> fields);

  const std::vector<LogicalField>& fields() const { return fields_; }
  const LogicalField* find(const std::string& field_id) const;
  std::optional<std::size_t> index_of(const std::string& field_id) const;

private:
  explicit LogicalSchema(std::vector<LogicalField> fields)
    : fields_(std::move(fields)) {}

  std::vector<LogicalField> fields_;
};

enum class ComparisonOperator { Equal, NotEqual, Less, LessEqual, Greater,
  GreaterEqual };
enum class LogicalOperator { And, Or, Not };

struct ComparisonExpression {
  FieldRef field;
  ComparisonOperator operation{ ComparisonOperator::Equal };
  LogicalValue value;
};

struct NullExpression {
  FieldRef field;
  bool negated{ false };
};

struct InExpression {
  FieldRef field;
  std::vector<LogicalValue> values;
  bool negated{ false };
};

struct QueryExpression;
using QueryExpressionPtr = std::shared_ptr<const QueryExpression>;

struct LogicalExpression {
  LogicalOperator operation{ LogicalOperator::And };
  std::vector<QueryExpressionPtr> operands;
};

struct QueryExpression {
  using Node = std::variant<ComparisonExpression, NullExpression,
    InExpression, LogicalExpression>;

  explicit QueryExpression(Node value) : node(std::move(value)) {}
  Node node;
};

QueryExpressionPtr make_comparison(FieldRef field,
  ComparisonOperator operation, LogicalValue value);
QueryExpressionPtr make_is_null(FieldRef field, bool negated = false);
QueryExpressionPtr make_in(FieldRef field, std::vector<LogicalValue> values,
  bool negated = false);
QueryExpressionPtr make_logical(LogicalOperator operation,
  std::vector<QueryExpressionPtr> operands);

// Expressions use SQL-style null semantics. Comparisons with a null literal
// are rejected; callers must use make_is_null(). No implicit type conversion
// is performed by validation.
QueryStatus validate_expression(const QueryExpressionPtr& expression,
  const LogicalSchema& schema);

enum class BoundKind { Inclusive, Exclusive };

struct DomainBound {
  LogicalValue value;
  BoundKind kind{ BoundKind::Inclusive };
};

struct DomainSlice {
  FieldRef dimension;
  std::optional<DomainBound> lower;
  std::optional<DomainBound> upper;
};

// A slice is an interval on one named dimension. It does not imply a physical
// row layout or a universal two-dimensional data model.
QueryStatus validate_domain_slice(const DomainSlice& slice,
  const LogicalSchema& schema);

enum class DomainKind { Rows, DenseGrid, TimeSeries };

struct DomainDimension {
  FieldRef field;
  LogicalType type;
  std::optional<std::string> unit;
};

struct DomainDescriptor {
  DomainKind kind{ DomainKind::Rows };
  std::vector<DomainDimension> dimensions;
  std::vector<std::uint64_t> shape;
  std::vector<std::uint64_t> origin;
};

using LogicalArrayData = std::variant<std::vector<bool>,
  std::vector<std::int32_t>, std::vector<std::int64_t>,
  std::vector<std::uint32_t>, std::vector<std::uint64_t>,
  std::vector<float>, std::vector<double>, std::vector<std::string>,
  std::vector<BinaryValue>, std::vector<TimestampNs>>;

struct LogicalArray {
  LogicalType type;
  LogicalArrayData values;

  // Empty means every value is valid. Otherwise this is a bit-packed bitmap
  // where bit i == 1 means row i is valid.
  std::vector<std::uint8_t> validity;
};

std::size_t logical_array_length(const LogicalArray& array);
QueryStatus validate_logical_array(const LogicalArray& array);

class QueryBatch {
public:
  static QueryResult<QueryBatch> create(LogicalSchema schema,
    std::uint64_t row_count, std::vector<LogicalArray> columns,
    std::optional<DomainDescriptor> domain = std::nullopt,
    std::vector<QuerySourceReference> sources = {});

  const LogicalSchema& schema() const { return schema_; }
  std::uint64_t row_count() const { return row_count_; }
  const std::vector<LogicalArray>& columns() const { return columns_; }
  const std::optional<DomainDescriptor>& domain() const { return domain_; }
  const std::vector<QuerySourceReference>& sources() const { return sources_; }

private:
  QueryBatch(LogicalSchema schema, std::uint64_t row_count,
    std::vector<LogicalArray> columns,
    std::optional<DomainDescriptor> domain,
    std::vector<QuerySourceReference> sources)
    : schema_(std::move(schema)), row_count_(row_count),
      columns_(std::move(columns)), domain_(std::move(domain)),
      sources_(std::move(sources)) {}

  LogicalSchema schema_;
  std::uint64_t row_count_{ 0 };
  std::vector<LogicalArray> columns_;
  std::optional<DomainDescriptor> domain_;
  std::vector<QuerySourceReference> sources_;
};

} // namespace iotdbfs
