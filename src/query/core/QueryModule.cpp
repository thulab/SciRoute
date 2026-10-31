#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryBatchBudget.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <type_traits>

namespace iotdbfs {
namespace {

struct IndexedCandidateDomain final : QueryDomainState {
  std::string adapter_id;
  std::weak_ptr<const FormatQueryAdapter> adapter;
  std::vector<StructuredBlobManifest> manifests;
  std::shared_ptr<const AdapterDomainIndex> index;
  std::shared_ptr<const PreparedQueryMetadataTrace> fees;
  std::uint64_t bytes{0};
  std::uint64_t retained_bytes() const override { return bytes; }
};

// These observations distinguish an index hit from the work needed to charge
// its immutable evidence. They do not change replay, collector or error order.
void replay_adapter_domain_fees(
    const std::shared_ptr<const PreparedQueryMetadataTrace>& fees) {
  if (!active_query_profile) { replay_query_metadata_trace(fees); return; }
  const auto budget = current_query_metadata_budget();
  QueryMetadataStageSummaryStats before;
  try { if (budget) before = budget->stage_summary_stats(); }
  catch (...) { replay_query_metadata_trace(fees); return; }
  query_profile_count("hot_query.adapter_fee_logical_entries", fees->fees().size());
  if (active_query_metadata_trace)
    query_profile_count("hot_query.adapter_fee_collector_untracked");
  const auto report = [&]() noexcept {
    if (!budget) return;
    try {
      const auto after = budget->stage_summary_stats();
      query_profile_count("hot_query.adapter_fee_entries_visited",
        after.full_fee_entries_visited - before.full_fee_entries_visited);
      query_profile_count("hot_query.adapter_fee_entries_skipped",
        after.borrowed_fee_entries_skipped - before.borrowed_fee_entries_skipped);
      query_profile_count("hot_query.adapter_fee_summary_stages",
        after.successful_stages - before.successful_stages);
    } catch (...) {}
  };
  QueryProfileTimer timer("adapter_domain_fee_replay");
  try { replay_query_metadata_trace(fees); }
  catch (...) { report(); throw; }
  report();
}

std::uint64_t retained_manifest_bytes(const StructuredBlobManifest& manifest) {
  std::uint64_t bytes = sizeof(manifest) + 128 + manifest.key.path.capacity();
  if(manifest.format_id) bytes += manifest.format_id->capacity();
  for(const auto& item : manifest.metadata) {
    bytes += sizeof(item) + 64 + item.first.capacity();
    bytes += std::visit([](const auto& value) -> std::uint64_t {
      using T = std::decay_t<decltype(value)>;
      if constexpr(std::is_same_v<T,std::string>) return value.capacity();
      else if constexpr(std::is_arithmetic_v<T>) return 0;
      else {
        std::uint64_t size = 0;
        for(const auto& element : value) {
          size += sizeof(element) + 64;
          if constexpr(std::is_same_v<typename T::value_type,std::string>) size += element.capacity();
        }
        return size;
      }
    }, item.second.value());
  }
  return bytes;
}

QueryStatus module_failure(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail = {}) {
  return QueryStatus::failure(code, phase, std::move(message),
    std::move(detail));
}

template<typename T>
QueryResult<T> module_failure_result(QueryError code, QueryPhase phase,
    std::string message, QueryErrorDetail detail = {}) {
  return QueryResult<T>::failure(module_failure(code, phase,
    std::move(message), std::move(detail)));
}

bool same_field(const LogicalField& left, const LogicalField& right) {
  return left.field_id == right.field_id &&
    left.display_name == right.display_name && left.type == right.type &&
    left.role == right.role && left.unit == right.unit;
}

bool same_schema(const LogicalSchema& left, const LogicalSchema& right) {
  if (left.fields().size() != right.fields().size()) return false;
  for (std::size_t i = 0; i < left.fields().size(); ++i)
    if (!same_field(left.fields()[i], right.fields()[i])) return false;
  return true;
}

QueryResult<LogicalSchema> file_result_schema() {
  return LogicalSchema::create({
    {"source.path", "Source path",
      {LogicalTypeId::Utf8, false, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"source.version", "Source version",
      {LogicalTypeId::Int64, false, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"source.format", "Source format",
      {LogicalTypeId::Utf8, true, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"source.payload_size", "Payload bytes",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, "byte"},
    {"source.layout_size", "Layout bytes",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, "byte"},
    {"source.revision", "Manifest revision",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, std::nullopt},
  });
}

LogicalType metadata_type(MetadataValueType type) {
  switch (type) {
  case MetadataValueType::String:
    return {LogicalTypeId::Utf8, false, std::nullopt};
  case MetadataValueType::Int64:
    return {LogicalTypeId::Int64, false, std::nullopt};
  case MetadataValueType::Double:
    return {LogicalTypeId::Float64, false, std::nullopt};
  case MetadataValueType::Bool:
    return {LogicalTypeId::Boolean, false, std::nullopt};
  case MetadataValueType::StringSet:
  case MetadataValueType::Int64Set:
  case MetadataValueType::DoubleSet:
  case MetadataValueType::BoolSet:
    break;
  }
  return {LogicalTypeId::Binary, false, std::nullopt};
}

bool scalar_metadata(MetadataValueType type) {
  return type == MetadataValueType::String ||
    type == MetadataValueType::Int64 ||
    type == MetadataValueType::Double ||
    type == MetadataValueType::Bool;
}

LogicalValue metadata_value(const MetadataValue& value) {
  switch (value.type()) {
  case MetadataValueType::String:
    return LogicalValue::utf8(std::get<std::string>(value.value()));
  case MetadataValueType::Int64:
    return LogicalValue::int64(std::get<std::int64_t>(value.value()));
  case MetadataValueType::Double:
    return LogicalValue::float64(std::get<double>(value.value()));
  case MetadataValueType::Bool:
    return LogicalValue::boolean(std::get<bool>(value.value()));
  case MetadataValueType::StringSet:
  case MetadataValueType::Int64Set:
  case MetadataValueType::DoubleSet:
  case MetadataValueType::BoolSet:
    break;
  }
  return LogicalValue::binary({});
}

QueryResult<LogicalSchema> manifest_predicate_schema(
    const StructuredBlobManifest& manifest) {
  std::vector<LogicalField> fields{
    {"manifest.path", "Manifest path",
      {LogicalTypeId::Utf8, false, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"manifest.version", "Manifest version",
      {LogicalTypeId::Int64, false, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"manifest.format_id", "Format identity",
      {LogicalTypeId::Utf8, true, std::nullopt},
      FieldRole::SourceIdentity, std::nullopt},
    {"manifest.payload_size", "Payload bytes",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, "byte"},
    {"manifest.layout_size", "Layout bytes",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, "byte"},
    {"manifest.revision", "Manifest revision",
      {LogicalTypeId::UInt64, false, std::nullopt},
      FieldRole::Attribute, std::nullopt},
  };
  for (const auto& entry : manifest.metadata) {
    // Collection summaries are indexed by the storage module for exact
    // membership pruning. They are intentionally not encoded as Binary in the
    // scalar logical-expression contract.
    if (!scalar_metadata(entry.second.type())) continue;
    fields.push_back({"sigma." + entry.first, entry.first,
      metadata_type(entry.second.type()), FieldRole::Attribute,
      std::nullopt});
  }
  return LogicalSchema::create(std::move(fields));
}

std::map<std::string, LogicalValue> manifest_values(
    const StructuredBlobManifest& manifest) {
  std::map<std::string, LogicalValue> values;
  values.emplace("manifest.path", LogicalValue::utf8(manifest.key.path));
  values.emplace("manifest.version",
    LogicalValue::int64(manifest.key.version));
  values.emplace("manifest.format_id", manifest.format_id
    ? LogicalValue::utf8(*manifest.format_id)
    : LogicalValue::null(LogicalTypeId::Utf8));
  values.emplace("manifest.payload_size",
    LogicalValue::uint64(manifest.payload_size));
  values.emplace("manifest.layout_size",
    LogicalValue::uint64(manifest.layout_size));
  values.emplace("manifest.revision",
    LogicalValue::uint64(manifest.revision));
  for (const auto& entry : manifest.metadata) {
    if (scalar_metadata(entry.second.type()))
      values.emplace("sigma." + entry.first, metadata_value(entry.second));
  }
  return values;
}

enum class QueryTruth { False, True, Unknown };

QueryTruth logical_not(QueryTruth value) {
  if (value == QueryTruth::True) return QueryTruth::False;
  if (value == QueryTruth::False) return QueryTruth::True;
  return QueryTruth::Unknown;
}

QueryTruth logical_and(QueryTruth left, QueryTruth right) {
  if (left == QueryTruth::False || right == QueryTruth::False)
    return QueryTruth::False;
  if (left == QueryTruth::Unknown || right == QueryTruth::Unknown)
    return QueryTruth::Unknown;
  return QueryTruth::True;
}

QueryTruth logical_or(QueryTruth left, QueryTruth right) {
  if (left == QueryTruth::True || right == QueryTruth::True)
    return QueryTruth::True;
  if (left == QueryTruth::Unknown || right == QueryTruth::Unknown)
    return QueryTruth::Unknown;
  return QueryTruth::False;
}

template<typename T>
std::optional<int> compare_scalar(const T& left, const T& right) {
  if (left < right) return -1;
  if (right < left) return 1;
  return 0;
}

std::optional<int> compare_values(const LogicalValue& left,
    const LogicalValue& right) {
  if (left.is_null() || right.is_null() || left.type() != right.type())
    return std::nullopt;
  switch (left.type()) {
  case LogicalTypeId::Boolean:
    return compare_scalar(std::get<bool>(left.value()),
      std::get<bool>(right.value()));
  case LogicalTypeId::Int32:
    return compare_scalar(std::get<std::int32_t>(left.value()),
      std::get<std::int32_t>(right.value()));
  case LogicalTypeId::Int64:
    return compare_scalar(std::get<std::int64_t>(left.value()),
      std::get<std::int64_t>(right.value()));
  case LogicalTypeId::UInt32:
    return compare_scalar(std::get<std::uint32_t>(left.value()),
      std::get<std::uint32_t>(right.value()));
  case LogicalTypeId::UInt64:
    return compare_scalar(std::get<std::uint64_t>(left.value()),
      std::get<std::uint64_t>(right.value()));
  case LogicalTypeId::Float32: {
    const auto lhs = std::get<float>(left.value());
    const auto rhs = std::get<float>(right.value());
    if (!std::isfinite(lhs) || !std::isfinite(rhs)) return std::nullopt;
    return compare_scalar(lhs, rhs);
  }
  case LogicalTypeId::Float64: {
    const auto lhs = std::get<double>(left.value());
    const auto rhs = std::get<double>(right.value());
    if (!std::isfinite(lhs) || !std::isfinite(rhs)) return std::nullopt;
    return compare_scalar(lhs, rhs);
  }
  case LogicalTypeId::Utf8:
    return compare_scalar(std::get<std::string>(left.value()),
      std::get<std::string>(right.value()));
  case LogicalTypeId::Binary:
    return compare_scalar(std::get<BinaryValue>(left.value()),
      std::get<BinaryValue>(right.value()));
  case LogicalTypeId::TimestampNs:
    return compare_scalar(std::get<TimestampNs>(left.value()).value,
      std::get<TimestampNs>(right.value()).value);
  }
  return std::nullopt;
}

QueryTruth compare_result(int comparison, ComparisonOperator operation) {
  switch (operation) {
  case ComparisonOperator::Equal:
    return comparison == 0 ? QueryTruth::True : QueryTruth::False;
  case ComparisonOperator::NotEqual:
    return comparison != 0 ? QueryTruth::True : QueryTruth::False;
  case ComparisonOperator::Less:
    return comparison < 0 ? QueryTruth::True : QueryTruth::False;
  case ComparisonOperator::LessEqual:
    return comparison <= 0 ? QueryTruth::True : QueryTruth::False;
  case ComparisonOperator::Greater:
    return comparison > 0 ? QueryTruth::True : QueryTruth::False;
  case ComparisonOperator::GreaterEqual:
    return comparison >= 0 ? QueryTruth::True : QueryTruth::False;
  }
  return QueryTruth::Unknown;
}

using ValueLookup = std::function<QueryResult<LogicalValue>(
  const std::string&)>;

QueryResult<QueryTruth> evaluate_expression(
    const QueryExpressionPtr& expression, const ValueLookup& lookup) {
  if (!expression)
    return QueryResult<QueryTruth>::success(QueryTruth::True);
  return std::visit([&](const auto& node) -> QueryResult<QueryTruth> {
    using Node = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<Node, ComparisonExpression>) {
      auto field = lookup(node.field.field_id);
      if (!field.ok())
        return QueryResult<QueryTruth>::failure(field.status());
      auto comparison = compare_values(field.value(), node.value);
      return QueryResult<QueryTruth>::success(comparison
        ? compare_result(*comparison, node.operation)
        : QueryTruth::Unknown);
    } else if constexpr (std::is_same_v<Node, NullExpression>) {
      auto field = lookup(node.field.field_id);
      if (!field.ok())
        return QueryResult<QueryTruth>::failure(field.status());
      const bool is_null = field.value().is_null();
      return QueryResult<QueryTruth>::success(
        (node.negated ? !is_null : is_null)
          ? QueryTruth::True : QueryTruth::False);
    } else if constexpr (std::is_same_v<Node, InExpression>) {
      auto field = lookup(node.field.field_id);
      if (!field.ok())
        return QueryResult<QueryTruth>::failure(field.status());
      if (field.value().is_null())
        return QueryResult<QueryTruth>::success(QueryTruth::Unknown);
      bool match = false;
      for (const auto& candidate : node.values) {
        const auto comparison = compare_values(field.value(), candidate);
        if (comparison && *comparison == 0) {
          match = true;
          break;
        }
      }
      if (node.negated) match = !match;
      return QueryResult<QueryTruth>::success(
        match ? QueryTruth::True : QueryTruth::False);
    } else {
      if (node.operation == LogicalOperator::Not) {
        auto operand = evaluate_expression(node.operands.front(), lookup);
        if (!operand.ok()) return operand;
        return QueryResult<QueryTruth>::success(
          logical_not(operand.value()));
      }
      QueryTruth accumulated = node.operation == LogicalOperator::And
        ? QueryTruth::True : QueryTruth::False;
      for (const auto& operand_expression : node.operands) {
        auto operand = evaluate_expression(operand_expression, lookup);
        if (!operand.ok()) return operand;
        accumulated = node.operation == LogicalOperator::And
          ? logical_and(accumulated, operand.value())
          : logical_or(accumulated, operand.value());
      }
      return QueryResult<QueryTruth>::success(accumulated);
    }
  }, expression->node);
}

bool validity_bit(const LogicalArray& array, std::size_t row) {
  if (array.validity.empty()) return true;
  return (array.validity[row / 8] &
    static_cast<std::uint8_t>(1u << (row % 8))) != 0;
}

void set_validity_bit(std::vector<std::uint8_t>& validity,
    std::size_t row) {
  validity[row / 8] |= static_cast<std::uint8_t>(1u << (row % 8));
}

QueryResult<LogicalValue> batch_value(const QueryBatch& batch,
    const std::string& field_id, std::size_t row) {
  const auto index = batch.schema().index_of(field_id);
  if (!index)
    return module_failure_result<LogicalValue>(QueryError::UnknownField,
      QueryPhase::Execution, "batch does not contain residual field: " +
      field_id);
  const auto& array = batch.columns()[*index];
  if (!validity_bit(array, row))
    return QueryResult<LogicalValue>::success(
      LogicalValue::null(array.type.id));
  switch (array.type.id) {
  case LogicalTypeId::Boolean:
    return QueryResult<LogicalValue>::success(LogicalValue::boolean(
      std::get<std::vector<bool>>(array.values)[row]));
  case LogicalTypeId::Int32:
    return QueryResult<LogicalValue>::success(LogicalValue::int32(
      std::get<std::vector<std::int32_t>>(array.values)[row]));
  case LogicalTypeId::Int64:
    return QueryResult<LogicalValue>::success(LogicalValue::int64(
      std::get<std::vector<std::int64_t>>(array.values)[row]));
  case LogicalTypeId::UInt32:
    return QueryResult<LogicalValue>::success(LogicalValue::uint32(
      std::get<std::vector<std::uint32_t>>(array.values)[row]));
  case LogicalTypeId::UInt64:
    return QueryResult<LogicalValue>::success(LogicalValue::uint64(
      std::get<std::vector<std::uint64_t>>(array.values)[row]));
  case LogicalTypeId::Float32:
    return QueryResult<LogicalValue>::success(LogicalValue::float32(
      std::get<std::vector<float>>(array.values)[row]));
  case LogicalTypeId::Float64:
    return QueryResult<LogicalValue>::success(LogicalValue::float64(
      std::get<std::vector<double>>(array.values)[row]));
  case LogicalTypeId::Utf8:
    return QueryResult<LogicalValue>::success(LogicalValue::utf8(
      std::get<std::vector<std::string>>(array.values)[row]));
  case LogicalTypeId::Binary:
    return QueryResult<LogicalValue>::success(LogicalValue::binary(
      std::get<std::vector<BinaryValue>>(array.values)[row]));
  case LogicalTypeId::TimestampNs:
    return QueryResult<LogicalValue>::success(LogicalValue::timestamp_ns(
      std::get<std::vector<TimestampNs>>(array.values)[row].value));
  }
  return module_failure_result<LogicalValue>(QueryError::Internal,
    QueryPhase::Execution, "batch contains an unknown logical type");
}

QueryResult<bool> matches_slice(const QueryBatch& batch,
    const DomainSlice& slice, std::size_t row) {
  auto value = batch_value(batch, slice.dimension.field_id, row);
  if (!value.ok()) return QueryResult<bool>::failure(value.status());
  if (value.value().is_null()) return QueryResult<bool>::success(false);
  if (slice.lower) {
    auto comparison = compare_values(value.value(), slice.lower->value);
    if (!comparison) return QueryResult<bool>::success(false);
    if (*comparison < 0 ||
        (*comparison == 0 && slice.lower->kind == BoundKind::Exclusive))
      return QueryResult<bool>::success(false);
  }
  if (slice.upper) {
    auto comparison = compare_values(value.value(), slice.upper->value);
    if (!comparison) return QueryResult<bool>::success(false);
    if (*comparison > 0 ||
        (*comparison == 0 && slice.upper->kind == BoundKind::Exclusive))
      return QueryResult<bool>::success(false);
  }
  return QueryResult<bool>::success(true);
}

LogicalArray select_rows(const LogicalArray& array,
    const std::vector<std::size_t>& rows) {
  LogicalArray selected;
  selected.type = array.type;
  selected.values = std::visit([&](const auto& values) -> LogicalArrayData {
    using Values = std::decay_t<decltype(values)>;
    Values output;
    output.reserve(rows.size());
    for (const auto row : rows) output.push_back(values[row]);
    return LogicalArrayData(std::move(output));
  }, array.values);
  if (!array.validity.empty()) {
    selected.validity.assign((rows.size() + 7) / 8, 0);
    for (std::size_t i = 0; i < rows.size(); ++i)
      if (validity_bit(array, rows[i])) set_validity_bit(selected.validity, i);
  }
  return selected;
}

QueryResult<std::optional<QueryBatch>> filter_and_project(
    QueryBatch batch, const AdapterPlan& plan,
    const LogicalSchema& expected_schema,
    const QuerySourceReference& source) {
  if (!same_schema(batch.schema(), plan.adapter_output_schema()))
    return module_failure_result<std::optional<QueryBatch>>(
      QueryError::SchemaMismatch, QueryPhase::Execution,
      "adapter batch schema differs from its prepared output schema");

  std::vector<std::size_t> rows;
  const bool has_filter = plan.residual_predicate() ||
    !plan.residual_slices().empty();
  if (!has_filter && same_schema(batch.schema(), expected_schema) &&
      !batch.sources().empty())
    return QueryResult<std::optional<QueryBatch>>::success(
      std::move(batch));

  rows.reserve(static_cast<std::size_t>(batch.row_count()));
  for (std::size_t row = 0; row < batch.row_count(); ++row) {
    bool selected = true;
    for (const auto& slice : plan.residual_slices()) {
      auto match = matches_slice(batch, slice, row);
      if (!match.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(
          match.status());
      if (!match.value()) {
        selected = false;
        break;
      }
    }
    if (selected && plan.residual_predicate()) {
      auto result = evaluate_expression(plan.residual_predicate(),
        [&](const std::string& field_id) {
          return batch_value(batch, field_id, row);
        });
      if (!result.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(
          result.status());
      selected = result.value() == QueryTruth::True;
    }
    if (selected) rows.push_back(row);
  }
  if (rows.empty())
    return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);

  std::vector<LogicalField> projected_fields;
  std::vector<LogicalArray> projected_columns;
  projected_fields.reserve(plan.final_projection().size());
  projected_columns.reserve(plan.final_projection().size());
  for (const auto& reference : plan.final_projection()) {
    const auto index = batch.schema().index_of(reference.field_id);
    if (!index)
      return module_failure_result<std::optional<QueryBatch>>(
        QueryError::SchemaMismatch, QueryPhase::Execution,
        "adapter batch omits a final projection field");
    projected_fields.push_back(batch.schema().fields()[*index]);
    projected_columns.push_back(select_rows(batch.columns()[*index], rows));
  }
  auto projected_schema = LogicalSchema::create(std::move(projected_fields));
  if (!projected_schema.ok())
    return QueryResult<std::optional<QueryBatch>>::failure(
      projected_schema.status());
  if (!same_schema(projected_schema.value(), expected_schema))
    return module_failure_result<std::optional<QueryBatch>>(
      QueryError::SchemaMismatch, QueryPhase::Execution,
      "projected adapter batch differs from the prepared result schema");

  std::optional<DomainDescriptor> domain;
  if (!has_filter || rows.size() == batch.row_count()) domain = batch.domain();
  auto provenance = batch.sources();
  if (provenance.empty()) provenance.push_back(source);
  auto projected = QueryBatch::create(std::move(projected_schema.value()),
    rows.size(), std::move(projected_columns), std::move(domain),
    std::move(provenance));
  if (!projected.ok())
    return QueryResult<std::optional<QueryBatch>>::failure(
      projected.status());
  return QueryResult<std::optional<QueryBatch>>::success(
    std::move(projected.value()));
}

struct QueryReadBudget {
  explicit QueryReadBudget(const QueryLimits& limits,
      QueryPhase phase = QueryPhase::Execution)
    : limits(limits), phase(phase), metadata_budget(current_query_metadata_budget()),
      batch_budget(active_query_batch_budget) {}

  std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope() const {
    return metadata_budget ? std::make_unique<ScopedQueryMetadataBudget>(metadata_budget) : nullptr;
  }

  QueryStatus reserve(bool layout, std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex);
    if (metadata_budget) {
      const auto status = metadata_budget->status(phase);
      if (!status.ok()) return status;
    }
    if (ranges == limits.max_ranges)
      return module_failure(QueryError::ResourceExhausted,
        phase, "query range budget is exhausted");
    auto& consumed = layout ? layout_bytes : payload_bytes;
    const auto maximum = layout ? limits.max_layout_bytes
                                : limits.max_payload_bytes;
    if (bytes > maximum - consumed)
      return module_failure(QueryError::ResourceExhausted,
        phase, layout
          ? "query layout byte budget is exhausted"
          : "query payload byte budget is exhausted");
    if (batch_budget) {
      const auto status = batch_budget->reserve_read(layout, bytes);
      if (!status.ok()) return status;
    }
    if (metadata_budget) {
      if (layout) {
        try { metadata_budget->observe_unique(bytes); }
        catch (const QueryMetadataBudgetExceeded&) { return metadata_budget->status(phase); }
      }
      const auto status = metadata_budget->status(phase);
      if (!status.ok()) return status;
    }
    ++ranges;
    consumed += bytes;
    return QueryStatus::success();
  }

  QueryLimits limits;
  QueryPhase phase;
  std::shared_ptr<QueryMetadataBudget> metadata_budget;
  std::shared_ptr<QueryBatchBudget> batch_budget;
  std::mutex mutex;
  std::size_t ranges{ 0 };
  std::uint64_t layout_bytes{ 0 };
  std::uint64_t payload_bytes{ 0 };
};

class BudgetedQuerySource final : public QuerySource {
public:
  BudgetedQuerySource(std::shared_ptr<const QuerySource> source,
      std::shared_ptr<QueryReadBudget> budget)
    : source_(std::move(source)), budget_(std::move(budget)) {}

  std::uint64_t storage_namespace() const override {
    return source_->storage_namespace();
  }

  const StructuredBlobManifest& manifest() const override {
    return source_->manifest();
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    auto scope = budget_->metadata_scope();
    return checked(source_->open_native_file());
  }
  QueryResult<std::optional<std::string>> native_file_cache_identity() const override {
    auto scope = budget_->metadata_scope();
    return checked(source_->native_file_cache_identity());
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>& handle) const override {
    auto scope = budget_->metadata_scope();
    return checked(source_->reuse_native_file(handle));
  }
  QueryResult<std::optional<std::string>> verified_layout_identity() const override {
    auto scope = budget_->metadata_scope();
    return checked(source_->verified_layout_identity());
  }

  QueryStatus reserve_logical_payload(std::uint64_t length) const override {
    return budget_->reserve(false, length);
  }

  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    auto scope = budget_->metadata_scope();
    auto reserved = budget_->reserve(false, length);
    if (!reserved.ok())
      return QueryResult<StructuredBlobBytes>::failure(std::move(reserved));
    return checked(source_->read_payload(offset, length));
  }

  PayloadReadCapabilities payload_read_capabilities() const override {
    return source_->payload_read_capabilities();
  }

  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    auto scope = budget_->metadata_scope();
    for (const auto& range : ranges) {
      auto reserved = budget_->reserve(false, range.length);
      if (!reserved.ok())
        return QueryResult<StructuredBlobPayloadBatch>::failure(std::move(reserved));
    }
    return checked(source_->read_payload_ranges(ranges));
  }

  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset,
      std::uint64_t length) const override {
    auto scope = budget_->metadata_scope();
    auto reserved = budget_->reserve(true, length);
    if (!reserved.ok())
      return QueryResult<StructuredBlobBytes>::failure(std::move(reserved));
    return checked(source_->read_layout(offset, length));
  }

private:
  template<class Result> Result checked(Result result) const {
    if (budget_->metadata_budget) {
      const auto status = budget_->metadata_budget->status(budget_->phase);
      if (!status.ok()) return Result::failure(status);
    }
    return result;
  }
  std::shared_ptr<const QuerySource> source_;
  std::shared_ptr<QueryReadBudget> budget_;
};

class EmptyQueryCursor final : public QueryBatchCursor {
public:
  explicit EmptyQueryCursor(LogicalSchema schema)
    : schema_(std::move(schema)) {}

  const LogicalSchema& schema() const override { return schema_; }
  QueryResult<std::optional<QueryBatch>> next() override {
    return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
  }
  void cancel() override {}

private:
  LogicalSchema schema_;
};

class FileSelectionCursor final : public QueryBatchCursor {
public:
  FileSelectionCursor(LogicalSchema schema,
      std::vector<PreparedSourceQuery> sources, std::size_t batch_rows)
    : schema_(std::move(schema)), sources_(std::move(sources)),
      batch_rows_(batch_rows) {}

  const LogicalSchema& schema() const override { return schema_; }

  QueryResult<std::optional<QueryBatch>> next() override {
    if (cancelled_)
      return module_failure_result<std::optional<QueryBatch>>(
        QueryError::Cancelled, QueryPhase::Execution,
        "file selection cursor was cancelled");
    if (offset_ == sources_.size())
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    const auto count = std::min(batch_rows_, sources_.size() - offset_);

    std::vector<std::string> paths;
    std::vector<std::int64_t> versions;
    std::vector<std::string> formats;
    std::vector<std::uint64_t> payload_sizes;
    std::vector<std::uint64_t> layout_sizes;
    std::vector<std::uint64_t> revisions;
    std::vector<std::uint8_t> format_validity((count + 7) / 8, 0);
    std::vector<QuerySourceReference> provenance;
    paths.reserve(count);
    versions.reserve(count);
    formats.reserve(count);
    payload_sizes.reserve(count);
    layout_sizes.reserve(count);
    revisions.reserve(count);
    provenance.reserve(count);
    bool every_format_present = true;
    for (std::size_t i = 0; i < count; ++i) {
      const auto& manifest = sources_[offset_ + i].source->manifest();
      paths.push_back(manifest.key.path);
      versions.push_back(manifest.key.version);
      formats.push_back(manifest.format_id.value_or(""));
      payload_sizes.push_back(manifest.payload_size);
      layout_sizes.push_back(manifest.layout_size);
      revisions.push_back(manifest.revision);
      if (manifest.format_id) set_validity_bit(format_validity, i);
      else every_format_present = false;
      provenance.push_back({manifest.key.path, manifest.key.version,
        manifest.format_id.value_or("")});
    }
    if (every_format_present) format_validity.clear();

    std::vector<LogicalArray> columns{
      {{LogicalTypeId::Utf8, false, std::nullopt}, std::move(paths), {}},
      {{LogicalTypeId::Int64, false, std::nullopt}, std::move(versions), {}},
      {{LogicalTypeId::Utf8, true, std::nullopt}, std::move(formats),
        std::move(format_validity)},
      {{LogicalTypeId::UInt64, false, std::nullopt},
        std::move(payload_sizes), {}},
      {{LogicalTypeId::UInt64, false, std::nullopt},
        std::move(layout_sizes), {}},
      {{LogicalTypeId::UInt64, false, std::nullopt},
        std::move(revisions), {}},
    };
    offset_ += count;
    auto batch = QueryBatch::create(schema_, count, std::move(columns),
      std::nullopt, std::move(provenance));
    if (!batch.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(batch.status());
    return QueryResult<std::optional<QueryBatch>>::success(
      std::move(batch.value()));
  }

  void cancel() override { cancelled_ = true; }

private:
  LogicalSchema schema_;
  std::vector<PreparedSourceQuery> sources_;
  std::size_t batch_rows_{ 1 };
  std::size_t offset_{ 0 };
  bool cancelled_{ false };
};

struct AdapterExecution {
  std::shared_ptr<const QuerySource> source;
  AdapterPlan plan;
  std::unique_ptr<QueryBatchCursor> cursor;
};

class CombinedQueryCursor final : public QueryBatchCursor {
public:
  CombinedQueryCursor(LogicalSchema schema,
      std::vector<AdapterExecution> executions,
      std::uint64_t max_result_rows,
      std::shared_ptr<QueryReadBudget> budget)
    : schema_(std::move(schema)), executions_(std::move(executions)),
      max_result_rows_(max_result_rows), budget_(std::move(budget)) {}

  const LogicalSchema& schema() const override { return schema_; }

  QueryResult<std::optional<QueryBatch>> next() override {
    if (cancelled_.load())
      return module_failure_result<std::optional<QueryBatch>>(
        QueryError::Cancelled, QueryPhase::Execution,
        "query cursor was cancelled");
    while (current_ < executions_.size()) {
      auto& execution = executions_[current_];
      auto next_batch = execution.cursor->next();
      if (cancelled_.load())
        return module_failure_result<std::optional<QueryBatch>>(
          QueryError::Cancelled, QueryPhase::Execution, "query cursor was cancelled");
      if (!next_batch.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(
          next_batch.status());
      if (!next_batch.value()) {
        ++current_;
        continue;
      }
      const auto& manifest = execution.source->manifest();
      auto transformed = filter_and_project(std::move(*next_batch.value()),
        execution.plan, schema_, {manifest.key.path, manifest.key.version,
          manifest.format_id.value_or("")});
      if (cancelled_.load())
        return module_failure_result<std::optional<QueryBatch>>(
          QueryError::Cancelled, QueryPhase::Execution, "query cursor was cancelled");
      if (!transformed.ok()) return transformed;
      if (!transformed.value()) continue;
      const auto rows = transformed.value()->row_count();
      if (rows > max_result_rows_ - emitted_rows_)
        return module_failure_result<std::optional<QueryBatch>>(
          QueryError::ResourceExhausted, QueryPhase::Execution,
          "query result row budget is exhausted");
      emitted_rows_ += rows;
      return transformed;
    }
    return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
  }

  void cancel() override {
    if (cancelled_.exchange(true)) return;
    for (auto& execution : executions_) execution.cursor->cancel();
  }

private:
  LogicalSchema schema_;
  std::vector<AdapterExecution> executions_;
  std::uint64_t max_result_rows_{ 0 };
  std::shared_ptr<QueryReadBudget> budget_;
  std::size_t current_{ 0 };
  std::uint64_t emitted_rows_{ 0 };
  std::atomic<bool> cancelled_{ false };
};

QueryResult<LogicalSchema> projection_schema(const AdapterPlan& plan) {
  std::vector<LogicalField> fields;
  fields.reserve(plan.final_projection().size());
  for (const auto& reference : plan.final_projection()) {
    const auto* field = plan.adapter_output_schema().find(reference.field_id);
    if (!field)
      return module_failure_result<LogicalSchema>(QueryError::SchemaMismatch,
        QueryPhase::Planning,
        "adapter plan final projection is absent from its output schema");
    fields.push_back(*field);
  }
  return LogicalSchema::create(std::move(fields));
}

QueryStatus unsupported_field_operation(const std::string& field_id,
    const char* operation) {
  return module_failure(QueryError::UnsupportedCapability,
    QueryPhase::Planning,
    "query operation is not supported by the format adapter",
    QueryErrorDetail{std::nullopt, field_id, std::string(operation), false});
}

QueryStatus validate_predicate_capabilities(const QueryExpressionPtr& expression,
    const std::map<std::string, const FieldQueryCapability*>& fields) {
  if (!expression) return QueryStatus::success();
  return std::visit([&](const auto& node) -> QueryStatus {
    using Node = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<Node, ComparisonExpression>) {
      const auto found = fields.find(node.field.field_id);
      if (found == fields.end() ||
          std::find(found->second->predicate_operators.begin(),
            found->second->predicate_operators.end(), node.operation) ==
            found->second->predicate_operators.end())
        return unsupported_field_operation(node.field.field_id, "predicate");
      return QueryStatus::success();
    } else if constexpr (std::is_same_v<Node, InExpression>) {
      const auto found = fields.find(node.field.field_id);
      if (found == fields.end() ||
          std::find(found->second->predicate_operators.begin(),
            found->second->predicate_operators.end(),
            ComparisonOperator::Equal) ==
            found->second->predicate_operators.end())
        return unsupported_field_operation(node.field.field_id, "in");
      return QueryStatus::success();
    } else if constexpr (std::is_same_v<Node, NullExpression>) {
      const auto found = fields.find(node.field.field_id);
      if (found == fields.end() || !found->second->null_predicate)
        return unsupported_field_operation(node.field.field_id, "null");
      return QueryStatus::success();
    } else {
      for (const auto& operand : node.operands) {
        auto status = validate_predicate_capabilities(operand, fields);
        if (!status.ok()) return status;
      }
      return QueryStatus::success();
    }
  }, expression->node);
}

QueryStatus validate_fragment(const QueryFragment& fragment,
    const QueryCapabilities& capabilities) {
  std::map<std::string, const FieldQueryCapability*> field_capabilities;
  for (const auto& capability : capabilities.fields())
    field_capabilities.emplace(capability.field.field_id, &capability);
  for (const auto& projection : fragment.projection) {
    const auto found = field_capabilities.find(projection.field_id);
    if (found == field_capabilities.end() || !found->second->projectable ||
        (!found->second->result_modes.empty() &&
         std::find(found->second->result_modes.begin(),
           found->second->result_modes.end(), fragment.output.mode) ==
           found->second->result_modes.end()))
      return module_failure(QueryError::UnsupportedCapability,
        QueryPhase::Planning,
        "query projection is not supported by the format adapter",
        QueryErrorDetail{std::nullopt, projection.field_id,
          std::string("projection"), false});
  }
  if (fragment.predicate) {
    auto status = validate_expression(fragment.predicate,
      capabilities.schema());
    if (!status.ok()) return status;
    status = validate_predicate_capabilities(fragment.predicate,
      field_capabilities);
    if (!status.ok()) return status;
  }
  for (const auto& slice : fragment.slices) {
    auto status = validate_domain_slice(slice, capabilities.schema());
    if (!status.ok()) return status;
    const auto found = field_capabilities.find(slice.dimension.field_id);
    if (found == field_capabilities.end() || !found->second->sliceable)
      return unsupported_field_operation(slice.dimension.field_id, "slice");
  }
  if (std::find(capabilities.result_modes().begin(),
      capabilities.result_modes().end(), fragment.output.mode) ==
      capabilities.result_modes().end())
    return module_failure(QueryError::UnsupportedCapability,
      QueryPhase::Planning,
      "query result mode is not supported by the format adapter");
  if (fragment.output.mode == QueryFragment::ResultMode::BinaryView) {
    if (fragment.projection.size() != 1)
      return module_failure(QueryError::UnsupportedCapability,
        QueryPhase::Planning,
        "binary-view result mode requires one Binary projection");
    const auto* field = capabilities.schema().find(
      fragment.projection.front().field_id);
    if (!field || field->type.id != LogicalTypeId::Binary)
      return unsupported_field_operation(
        fragment.projection.front().field_id, "binary-view");
  }
  for (const auto& type_override : fragment.output.type_overrides) {
    const auto found = field_capabilities.find(
      type_override.field.field_id);
    const auto projected = std::find_if(fragment.projection.begin(),
      fragment.projection.end(), [&](const auto& field) {
        return field.field_id == type_override.field.field_id;
      });
    if (found == field_capabilities.end() ||
        projected == fragment.projection.end() ||
        std::find(found->second->output_types.begin(),
          found->second->output_types.end(), type_override.type) ==
          found->second->output_types.end())
      return unsupported_field_operation(type_override.field.field_id,
        "output-type");
  }
  if (fragment.output.interpolation) {
    if (std::find(capabilities.interpolation_methods().begin(),
        capabilities.interpolation_methods().end(),
        fragment.output.interpolation->method) ==
        capabilities.interpolation_methods().end())
      return module_failure(QueryError::UnsupportedCapability,
        QueryPhase::Planning,
        "query interpolation method is not supported by the format adapter");
    for (const auto& axis : fragment.output.interpolation->axes) {
      const auto found = field_capabilities.find(axis.dimension.field_id);
      if (found == field_capabilities.end() ||
          !found->second->interpolatable)
        return unsupported_field_operation(axis.dimension.field_id,
          "interpolation");
    }
  }
  return QueryStatus::success();
}

bool same_projection(const std::vector<FieldRef>& left,
    const std::vector<FieldRef>& right) {
  if (left.size() != right.size()) return false;
  for (std::size_t i = 0; i < left.size(); ++i)
    if (left[i].field_id != right[i].field_id) return false;
  return true;
}

} // namespace

class QueryModule::Implementation {
public:
  Implementation(std::shared_ptr<const PublishedVersionCatalog> catalog,
      std::shared_ptr<const StructuredBlobStorage> storage,
      std::map<std::string, std::shared_ptr<const FormatQueryAdapter>> adapters)
    : catalog(std::move(catalog)), storage(std::move(storage)),
      adapters(std::move(adapters)) {}

  std::shared_ptr<const PublishedVersionCatalog> catalog;
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::map<std::string, std::shared_ptr<const FormatQueryAdapter>> adapters;
};

QueryResult<std::shared_ptr<const QueryModule>> QueryModule::create(
    std::shared_ptr<const PublishedVersionCatalog> catalog,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::vector<std::shared_ptr<const FormatQueryAdapter>> adapters) {
  if (!catalog || !storage)
    return module_failure_result<std::shared_ptr<const QueryModule>>(
      QueryError::InvalidQuery, QueryPhase::Validation,
      "Query Module requires a catalog and Structured Blob Storage");
  std::map<std::string, std::shared_ptr<const FormatQueryAdapter>> registry;
  for (auto& adapter : adapters) {
    if (!adapter || adapter->id().empty())
      return module_failure_result<std::shared_ptr<const QueryModule>>(
        QueryError::InvalidQuery, QueryPhase::Validation,
        "Query Module adapter identity cannot be empty");
    if (!registry.emplace(adapter->id(), std::move(adapter)).second)
      return module_failure_result<std::shared_ptr<const QueryModule>>(
        QueryError::InvalidQuery, QueryPhase::Validation,
        "Query Module adapter identities must be unique");
  }
  auto implementation = std::make_shared<const Implementation>(
    std::move(catalog), std::move(storage), std::move(registry));
  std::shared_ptr<const QueryModule> module(
    new QueryModule(std::move(implementation)));
  return QueryResult<std::shared_ptr<const QueryModule>>::success(
    std::move(module));
}

QueryResult<CatalogSelection> QueryModule::select(const QueryRequest& request) const {
  auto valid = validate_query_request(request);
  if (!valid.ok()) return QueryResult<CatalogSelection>::failure(valid);
  QueryProfileTimer timer("typed_catalog_scan");
  query_profile_count("catalog_scan_calls");
  return implementation_->catalog->scan(request.sources, request.limits.max_sources);
}

QueryStatus QueryModule::validate_format_catalog_request(const QueryRequest& request) const {
  auto valid = validate_query_request(request);
  if (!valid.ok()) return valid;
  if (!request.subset || request.sources.formats.adapters != AdapterSelectionKind::Listed ||
      request.sources.formats.adapter_ids.size() != 1 ||
      request.sources.formats.include_byte_passthrough)
    return module_failure(QueryError::UnsupportedCapability, QueryPhase::Planning,
      "format discovery requires one explicit metadata adapter");
  const auto found = implementation_->adapters.find(request.sources.formats.adapter_ids.front());
  if (found == implementation_->adapters.end() ||
      !found->second->supports_unit_catalog(*request.subset))
    return module_failure(QueryError::UnsupportedCapability, QueryPhase::Planning,
      "adapter does not support this unit discovery request");
  return QueryStatus::success();
}

QueryResult<PreparedQuery> QueryModule::prepare(
    const QueryRequest& request, const CatalogSelection* selected_snapshot) const {
  auto valid = validate_query_request(request);
  if (!valid.ok())
    return QueryResult<PreparedQuery>::failure(std::move(valid));
  CatalogSelection owned_selection;
  if (selected_snapshot) {
    query_profile_count("catalog_selection_reuse");
  } else {
    auto selected = select(request);
    if (!selected.ok())
      return QueryResult<PreparedQuery>::failure(selected.status());
    owned_selection = std::move(selected.value());
  }
  const auto& selected = selected_snapshot ? *selected_snapshot : owned_selection;

  std::shared_ptr<QueryDomainCache::Claim> domain_claim;
  std::shared_ptr<const IndexedCandidateDomain> indexed_domain;
  std::shared_ptr<const FormatQueryAdapter> domain_adapter;
  std::unique_ptr<ScopedQueryMetadataTrace> domain_capture;
  std::optional<AdapterDomainSelection> matching_sources;
  if(selected.hot_state && request.subset &&
      !selected.residual_file_predicate && !request.sources.file_predicate &&
      request.sources.formats.adapters == AdapterSelectionKind::Listed &&
      request.sources.formats.adapter_ids.size() == 1 &&
      !request.sources.formats.include_byte_passthrough) {
    const auto found = implementation_->adapters.find(request.sources.formats.adapter_ids.front());
    if(found != implementation_->adapters.end() && found->second->supports_domain_index(*request.subset)) {
      domain_adapter = found->second;
      try {
        if(const auto budget = current_query_metadata_budget(); budget && !budget->status().ok())
          return QueryResult<PreparedQuery>::failure(budget->status());
        domain_claim = selected.hot_state->claim();
        if(domain_claim->value) {
          auto cached = std::dynamic_pointer_cast<const IndexedCandidateDomain>(domain_claim->value);
          if(cached && cached->adapter_id == domain_adapter->id() &&
              cached->adapter.lock() == domain_adapter) {
            auto matches = [&] {
              QueryProfileTimer timer("adapter_domain_lookup");
              return cached->index->select(*request.subset, request.limits);
            }();
            if(!matches.ok()) return QueryResult<PreparedQuery>::failure(matches.status());
            if(matches.value()) {
              if(selected.read_context) selected.read_context->arm_domain_metadata_summary();
              replay_adapter_domain_fees(cached->fees);
              matching_sources = std::move(*matches.value());
              indexed_domain = std::move(cached);
              query_profile_count("hot_query.adapter_domain_hits");
            }
          }
        } else if(domain_claim->building()) {
          domain_capture = std::make_unique<ScopedQueryMetadataTrace>("",
            [claim = domain_claim](std::uint64_t bytes) {
              if(!claim->grow(bytes)) throw std::bad_alloc();
            }, QueryMetadataTracePolicy::CacheStable);
        }
      } catch(const QueryMetadataBudgetExceeded&) {
        const auto budget = current_query_metadata_budget();
        return QueryResult<PreparedQuery>::failure(budget ? budget->status() :
          QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Planning,
            "request metadata byte budget exhausted"));
      } catch(const std::bad_alloc&) {
        domain_capture.reset(); domain_claim.reset();
      }
    }
  }

  std::vector<StructuredBlobManifest> manifests;
  if(!indexed_domain) manifests.reserve(selected.versions().size());
  if(!indexed_domain) for (const auto& observed : selected.versions()) {
    auto manifest = observed;
    if (selected.read_context) {
      auto fresh = selected.read_context->manifest(manifest.key);
      if (!fresh.ok()) return QueryResult<PreparedQuery>::failure(fresh.status());
      manifest = std::move(fresh.value());
    }
    if (!manifest.metadata_materialized) {
      // Frontends authorize the selected identity/permission skeletons before
      // prepare(). Materialize only those selected exact sources here.
      auto reader = implementation_->storage->open_catalog_version(manifest.key,
        manifest, selected.storage_namespace);
      if (!reader.ok())
        return module_failure_result<PreparedQuery>(QueryError::ReadFailure,
          QueryPhase::Catalog, reader.status().message(),
          QueryErrorDetail{QuerySourceReference{manifest.key.path,
            manifest.key.version, manifest.format_id.value_or("")}});
      manifest = reader.value().manifest();
    }
    if (!manifest.published ||
        manifest.phase != StructuredBlobPhase::Published)
      return module_failure_result<PreparedQuery>(
        QueryError::SourceNotPublished, QueryPhase::Catalog,
        "Published Version Catalog returned an unpublished version");
    if (selected.residual_file_predicate) {
      auto schema = manifest_predicate_schema(manifest);
      if (!schema.ok())
        return QueryResult<PreparedQuery>::failure(schema.status());
      auto predicate = validate_expression(
        selected.residual_file_predicate, schema.value());
      if (!predicate.ok())
        return QueryResult<PreparedQuery>::failure(std::move(predicate));
      auto values = manifest_values(manifest);
      auto matched = evaluate_expression(
        selected.residual_file_predicate,
        [&](const std::string& field_id) -> QueryResult<LogicalValue> {
          const auto found = values.find(field_id);
          if (found == values.end())
            return module_failure_result<LogicalValue>(
              QueryError::UnknownField, QueryPhase::Planning,
              "manifest predicate field is missing: " + field_id);
          return QueryResult<LogicalValue>::success(found->second);
        });
      if (!matched.ok())
        return QueryResult<PreparedQuery>::failure(matched.status());
      if (matched.value() != QueryTruth::True) continue;
    }
    manifests.push_back(manifest);
  }
  const auto& active_manifests = indexed_domain ? indexed_domain->manifests : manifests;
  if (request.subset && active_manifests.empty() && selected.versions().empty() &&
      query_fragment_requires_exact_source(*request.subset)) {
    // A catalog may push down the file predicate (or exclude the format), so
    // an empty selection does not prove that the exact version is absent.
    const auto& path = request.sources.paths.front().path;
    const auto version = *request.sources.versions.exact;
    auto source = [&] {
      QueryProfileTimer timer("typed_source_open");
      query_profile_count("source_open_calls");
      query_profile_count("source_manifest_load_calls");
      return open_structured_blob_query_source(implementation_->storage, {path, version});
    }();
    if (!source.ok())
      return QueryResult<PreparedQuery>::failure(source.status());
  }

  std::vector<PreparedSourceQuery> prepared_sources;
  prepared_sources.reserve(matching_sources ? matching_sources->size() : active_manifests.size());
  std::optional<LogicalSchema> result_schema;
  // Planning and execution each receive a fresh budget. Charge actual reads,
  // including repeated capability/prepare reads, rather than manifest sizes.
  auto planning_budget = std::make_shared<QueryReadBudget>(request.limits,
    QueryPhase::Planning);
  if (!request.subset) {
    auto schema = file_result_schema();
    if (!schema.ok())
      return QueryResult<PreparedQuery>::failure(schema.status());
    result_schema = std::move(schema.value());
  }

  // An empty semantic match still validates the request and derives its schema
  // through one exact-source adapter. Its plan is discarded before execution.
  const bool schema_probe = matching_sources && matching_sources->empty() && !active_manifests.empty();
  const auto source_count = matching_sources ? (schema_probe ? 1 : matching_sources->size()) : active_manifests.size();
  for (std::size_t position = 0; position < source_count; ++position) {
    const auto ordinal = matching_sources ?
      (schema_probe ? 0 : matching_sources->at(position).source_ordinal) : position;
    if(ordinal >= active_manifests.size())
      return module_failure_result<PreparedQuery>(QueryError::Internal, QueryPhase::Planning,
        "adapter domain index returned an invalid source ordinal");
    const auto& manifest = active_manifests[ordinal];
    auto source = [&] {
      QueryProfileTimer timer("typed_source_open");
      query_profile_count("source_open_calls");
      if (selected.storage_namespace) {
        query_profile_count("catalog_manifest_reuse");
        return open_catalog_query_source(implementation_->storage, manifest,
          selected.storage_namespace);
      }
      query_profile_count("source_manifest_load_calls");
      return open_structured_blob_query_source(implementation_->storage, manifest.key);
    }();
    if (!source.ok())
      return QueryResult<PreparedQuery>::failure(source.status());
    if (!request.subset) {
      prepared_sources.push_back({std::move(source.value()), nullptr,
        std::nullopt});
      continue;
    }
    if (!manifest.format_id)
      return module_failure_result<PreparedQuery>(
        QueryError::AdapterUnavailable, QueryPhase::Planning,
        "byte pass-through source does not support a semantic subset");
    const auto adapter_found = implementation_->adapters.find(
      *manifest.format_id);
    if (adapter_found == implementation_->adapters.end())
      return module_failure_result<PreparedQuery>(
        QueryError::AdapterUnavailable, QueryPhase::Planning,
        "no query adapter is registered for Format Identity " +
        *manifest.format_id);
    const auto& adapter = adapter_found->second;
    // This wrapper is scoped to planning. Prepared sources retain the original
    // reader so execution does not inherit an exhausted planning budget.
    const BudgetedQuerySource planning_source(source.value(), planning_budget);
    const QueryPlanningContext planning_context(planning_source, request.limits);
    if (matching_sources && !schema_probe)
      planning_context.set_state(matching_sources->at(position).planning_state);
    auto capabilities = adapter->capabilities(planning_context, *request.subset);
    if (!capabilities.ok())
      return QueryResult<PreparedQuery>::failure(capabilities.status());
    if (adapter->id() != capabilities.value().adapter_id())
      return module_failure_result<PreparedQuery>(QueryError::Internal,
        QueryPhase::Planning,
        "query adapter capability identity does not match its registry id");
    auto capability_status = validate_query_capabilities(
      capabilities.value());
    if (!capability_status.ok())
      return QueryResult<PreparedQuery>::failure(
        std::move(capability_status));
    auto fragment_status = validate_fragment(*request.subset,
      capabilities.value());
    if (!fragment_status.ok())
      return QueryResult<PreparedQuery>::failure(std::move(fragment_status));

    auto plan = adapter->prepare(planning_context, *request.subset);
    if (!plan.ok())
      return QueryResult<PreparedQuery>::failure(plan.status());
    if (!plan.value().implementation())
      return module_failure_result<PreparedQuery>(QueryError::Internal,
        QueryPhase::Planning,
        "query adapter returned an empty physical plan");
    if (!same_projection(plan.value().final_projection(),
        request.subset->projection))
      return module_failure_result<PreparedQuery>(
        QueryError::SchemaMismatch, QueryPhase::Planning,
        "query adapter changed the requested final projection");
    auto projected = projection_schema(plan.value());
    if (!projected.ok())
      return QueryResult<PreparedQuery>::failure(projected.status());
    if (!result_schema) result_schema = projected.value();
    else if (!same_schema(*result_schema, projected.value()))
      return module_failure_result<PreparedQuery>(
        QueryError::SchemaMismatch, QueryPhase::Planning,
        "selected formats do not produce the same result schema");
    prepared_sources.push_back({std::move(source.value()), adapter,
      std::move(plan.value())});
  }

  if(schema_probe) prepared_sources.clear();
  auto result = PreparedQuery::create(request, std::move(result_schema),
    std::move(prepared_sources));
  if(!result.ok()) return result;
  result.value().retain_read_context(selected.read_context);
  // Cache only a successful ordinary full-domain preparation. Capturing errors
  // or retaining a query-specific plan would weaken later request validation.
  if(domain_capture && domain_claim && domain_adapter && !domain_capture->failed()) {
    auto fees = domain_capture->release();
    domain_capture.reset();
    try {
      std::uint64_t wrapper_bytes = sizeof(IndexedCandidateDomain) + 512 +
        domain_adapter->id().capacity() + PreparedQueryMetadataTrace::management_bytes +
        result.value().sources().size() * sizeof(std::shared_ptr<const QuerySource>);
      for(const auto& manifest : manifests) wrapper_bytes += retained_manifest_bytes(manifest);
      if(domain_claim->grow(wrapper_bytes)) {
        const auto index_budget = domain_claim->available();
        if(index_budget && domain_claim->grow(index_budget)) {
          std::vector<std::shared_ptr<const QuerySource>> sources;
          sources.reserve(result.value().sources().size());
          for(const auto& prepared : result.value().sources()) sources.push_back(prepared.source);
          auto index = domain_adapter->build_domain_index(sources, *request.subset, request.limits, index_budget);
          if(index.ok() && index.value() && index.value()->retained_bytes() <= index_budget) {
            domain_claim->shrink(index_budget - index.value()->retained_bytes());
            auto domain = std::make_shared<IndexedCandidateDomain>();
            domain->adapter_id = domain_adapter->id();
            domain->adapter = domain_adapter;
            domain->manifests = std::move(manifests);
            domain->index = std::move(index.value());
            domain->fees = std::make_shared<const PreparedQueryMetadataTrace>(std::move(fees), domain_claim->residency_pin());
            domain->bytes = wrapper_bytes + domain->index->retained_bytes();
            const auto published_fees=domain->fees;
            if(domain_claim->publish(std::move(domain))) {
              if(selected.read_context) selected.read_context->bind_domain_metadata_fees(published_fees);
              query_profile_count("hot_query.adapter_domain_builds");
            }
          }
        }
      }
    } catch(const std::bad_alloc&) { query_profile_count("hot_query.adapter_domain_opt_outs"); }
  }
  domain_capture.reset();

  return result;
}

QueryResult<FormatCatalogResult> QueryModule::format_catalog(
    const PreparedQuery& query, std::uint64_t max_response_bytes) const {
  using Result = QueryResult<FormatCatalogResult>;
  QueryProfileTimer timer("format_catalog_locations");
  auto valid = validate_format_catalog_request(query.request());
  if (!valid.ok()) return Result::failure(valid);
  if (!query.request().subset || max_response_bytes < 64)
    return module_failure_result<FormatCatalogResult>(QueryError::InvalidQuery,
      QueryPhase::Planning, "format discovery requires a semantic request and response budget");
  FormatCatalogResult result;
  result.read_context = query.read_context();
  // JSON escaping uses at most six bytes per input byte. Reserve fixed
  // envelope/number/punctuation overhead before retaining any output string.
  std::uint64_t remaining = max_response_bytes - 64;
  for (const auto& selected : query.sources()) {
    if (!selected.source || !selected.plan ||
        !selected.plan->implementation())
      return module_failure_result<FormatCatalogResult>(QueryError::UnsupportedCapability,
        QueryPhase::Planning, "format discovery requires adapter metadata plans");
    if (selected.plan->residual_predicate() || !selected.plan->residual_slices().empty())
      return module_failure_result<FormatCatalogResult>(QueryError::UnsupportedCapability,
        QueryPhase::Planning, "format discovery cannot discard residual query semantics");
    const auto& manifest = selected.source->manifest();
    std::set<std::string> identities;
    const auto status = selected.plan->implementation()->visit_unit_locators(
      [&](const FormatUnitLocator& unit) -> QueryStatus {
        if (unit.unit_id.empty() || unit.unit_id.find('\0') != std::string::npos ||
            !unit.length || unit.offset > manifest.payload_size ||
            unit.length > manifest.payload_size - unit.offset)
          return module_failure(QueryError::CorruptLayout, QueryPhase::Planning,
            "format discovery returned an invalid unit location",
            QueryErrorDetail{QuerySourceReference{manifest.key.path, manifest.key.version,
              manifest.format_id.value_or("")}});
        if (result.units.size() >= query.request().limits.max_result_rows)
          return module_failure(QueryError::ResourceExhausted, QueryPhase::Planning,
            "format discovery result row budget exhausted");
        std::uint64_t bytes = 256;
        for (const auto size : {manifest.key.path.size(),
             manifest.format_id ? manifest.format_id->size() : std::size_t{0}, unit.unit_id.size()}) {
          if (size > (remaining >= bytes ? remaining - bytes : 0) / 6)
            return module_failure(QueryError::ResourceExhausted, QueryPhase::Encoding,
              "format discovery response exceeds control frame budget");
          bytes += size * 6;
        }
        if (bytes > remaining)
          return module_failure(QueryError::ResourceExhausted, QueryPhase::Encoding,
            "format discovery response exceeds control frame budget");
        if (!identities.insert(unit.unit_id).second)
          return module_failure(QueryError::CorruptLayout, QueryPhase::Planning,
            "format discovery returned a duplicate unit identity");
        remaining -= bytes;
        // This is the new metadata response, not an exemption from source or
        // cached-evidence charging. Ordinary typed execution is unaffected.
        require_query_metadata_unique(sizeof(FormatCatalogMatch) +
          manifest.key.path.size() + manifest.format_id.value_or("").size() +
          unit.unit_id.size() + 128);
        result.units.push_back({{manifest.key.path, manifest.key.version,
          manifest.format_id.value_or("")}, manifest.revision, unit});
        query_profile_count("format_catalog.units");
        return QueryStatus::success();
      });
    if (!status.ok()) {
      auto detail = status.detail();
      if (!detail.source) detail.source = QuerySourceReference{manifest.key.path,
        manifest.key.version, manifest.format_id.value_or("")};
      return Result::failure(QueryStatus::failure(status.code(), status.phase(),
        status.message(), std::move(detail)));
    }
  }
  return Result::success(std::move(result));
}

QueryResult<std::unique_ptr<QueryBatchCursor>> QueryModule::execute(
    const PreparedQuery& query) const {
  if (!query.request().subset) {
    if (query.sources().size() > query.request().limits.max_result_rows)
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::ResourceExhausted, QueryPhase::Execution,
        "file selection exceeds the query result row budget");
    auto schema = file_result_schema();
    if (!schema.ok())
      return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(
        schema.status());
    if (query.result_schema() &&
        !same_schema(*query.result_schema(), schema.value()))
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::SchemaMismatch, QueryPhase::Execution,
        "prepared file selection has an invalid result schema");
    std::unique_ptr<QueryBatchCursor> cursor =
      std::make_unique<FileSelectionCursor>(std::move(schema.value()),
        query.sources(), query.request().limits.target_batch_rows);
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
      std::move(cursor));
  }

  LogicalSchema schema = query.result_schema()
    ? *query.result_schema()
    : LogicalSchema::create({}).value();
  if (query.sources().empty()) {
    std::unique_ptr<QueryBatchCursor> cursor =
      std::make_unique<EmptyQueryCursor>(std::move(schema));
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
      std::move(cursor));
  }
  auto budget = std::make_shared<QueryReadBudget>(query.request().limits);
  QueryExecutionOptions options;
  options.target_batch_rows = query.request().limits.target_batch_rows;
  options.target_batch_bytes = query.request().limits.target_batch_bytes;
  options.max_ranges = query.request().limits.max_ranges;
  options.max_layout_bytes = query.request().limits.max_layout_bytes;
  options.max_payload_bytes = query.request().limits.max_payload_bytes;
  options.max_result_rows = query.request().limits.max_result_rows;

  std::vector<AdapterExecution> executions;
  executions.reserve(query.sources().size());
  for (const auto& prepared : query.sources()) {
    if (!prepared.source || !prepared.adapter || !prepared.plan ||
        !prepared.plan->implementation())
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::Internal, QueryPhase::Execution,
        "prepared semantic query is incomplete");
    const auto registered = implementation_->adapters.find(
      prepared.adapter->id());
    if (registered == implementation_->adapters.end() ||
        registered->second.get() != prepared.adapter.get())
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::StalePlan, QueryPhase::Execution,
        "prepared query adapter does not belong to this Query Module");
    std::shared_ptr<const QuerySource> budgeted =
      std::make_shared<const BudgetedQuerySource>(prepared.source, budget);
    auto cursor = prepared.adapter->execute(
      QueryExecutionContext(budgeted, prepared.source),
      prepared.plan->implementation(), options);
    if (!cursor.ok())
      return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(
        cursor.status());
    if (!cursor.value())
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::Internal, QueryPhase::Execution,
        "query adapter returned a null cursor");
    if (!same_schema(cursor.value()->schema(),
        prepared.plan->adapter_output_schema()))
      return module_failure_result<std::unique_ptr<QueryBatchCursor>>(
        QueryError::SchemaMismatch, QueryPhase::Execution,
        "query adapter cursor schema differs from its prepared plan");
    executions.push_back({std::move(budgeted), *prepared.plan,
      std::move(cursor.value())});
  }
  std::unique_ptr<QueryBatchCursor> cursor =
    std::make_unique<CombinedQueryCursor>(std::move(schema),
      std::move(executions), query.request().limits.max_result_rows,
      std::move(budget));
  return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
    std::move(cursor));
}

} // namespace iotdbfs
