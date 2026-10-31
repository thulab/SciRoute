#include "iotdbfs/formats/arrow/ArrowQueryModel.h"

#include <set>
#include <utility>

namespace iotdbfs {
namespace {

QueryStatus invalid_array(std::string message,
    std::optional<std::string> field_id = std::nullopt) {
  QueryErrorDetail detail;
  detail.field_id = std::move(field_id);
  return QueryStatus::failure(QueryError::InvalidQuery,
    QueryPhase::Validation, std::move(message), std::move(detail));
}

template<typename T>
QueryResult<T> array_failure(QueryError code, std::string message) {
  return QueryResult<T>::failure(QueryStatus::failure(code,
    QueryPhase::Planning, std::move(message)));
}

} // namespace

std::string arrow_query_value_field_id(const ArrayValuePath& path) {
  return path.child_indices.empty() ? "value" :
    "value." + std::to_string(path.child_indices.front());
}

QueryResult<LogicalSchema> make_arrow_logical_schema(const ArrowPhysicalBinding& binding) {
  std::vector<LogicalField> fields{{"index", "index",
    {LogicalTypeId::UInt64, false, std::nullopt}, FieldRole::Dimension, std::nullopt}};
  for (const auto& path : binding.subject.values) {
    auto name = arrow_query_value_field_id(path);
    fields.push_back({name, name, {LogicalTypeId::Float32, true, std::nullopt},
      FieldRole::Measure, std::nullopt});
  }
  return LogicalSchema::create(std::move(fields));
}

QueryResult<QueryCapabilities> make_arrow_query_capabilities(
    const ArrowPhysicalBinding& binding) {
  auto schema = make_arrow_logical_schema(binding);
  if (!schema.ok()) return QueryResult<QueryCapabilities>::failure(schema.status());
  std::vector<FieldQueryCapability> fields;
  for (const auto& field : schema.value().fields()) {
    FieldQueryCapability capability;
    capability.field = {field.field_id};
    capability.projectable = true;
    capability.predicate_operators = {ComparisonOperator::Equal, ComparisonOperator::NotEqual,
      ComparisonOperator::Less, ComparisonOperator::LessEqual, ComparisonOperator::Greater,
      ComparisonOperator::GreaterEqual};
    capability.predicate_pushdown = PushdownLevel::None;
    capability.null_predicate = field.type.nullable;
    capability.sliceable = field.field_id == "index";
    capability.slice_pushdown = capability.sliceable ? PushdownLevel::Exact : PushdownLevel::None;
    fields.push_back(std::move(capability));
  }
  return QueryResult<QueryCapabilities>::success(QueryCapabilities(1, "arrow", "1",
    std::move(schema.value()), std::move(fields), {DomainKind::Rows}, false, false,
    OrderingGuarantee::Domain));
}

QueryStatus validate_physical_array_subject(
    const PhysicalArraySubject& subject) {
  if (subject.field_id.empty())
    return invalid_array("physical array field cannot be empty");
  if (subject.values.empty())
    return invalid_array("physical array subject requires at least one value",
      subject.field_id);
  std::set<std::vector<std::uint64_t>> paths;
  const auto path_depth = subject.values.front().child_indices.size();
  for (const auto& value : subject.values) {
    if (value.child_indices.size() > 1)
      return invalid_array("physical array path exceeds two List levels",
        subject.field_id);
    if (value.child_indices.size() != path_depth)
      return invalid_array("physical array paths must have the same depth",
        subject.field_id);
    if (!paths.insert(value.child_indices).second)
      return invalid_array("physical array paths must be unique",
        subject.field_id);
  }
  return QueryStatus::success();
}

QueryStatus validate_physical_array_shape(const PhysicalArrayShape& shape) {
  if (shape.list_depth != 1 && shape.list_depth != 2)
    return QueryStatus::failure(QueryError::UnsupportedCapability,
      QueryPhase::Planning, "physical array requires one or two List levels");
  if (shape.leaf_type != PhysicalArrayLeafType::Float32)
    return QueryStatus::failure(QueryError::UnsupportedCapability,
      QueryPhase::Planning, "physical array leaf must be float32");
  return QueryStatus::success();
}

QueryResult<ResolvedArrayValue> resolve_array_value(
    const PhysicalArrayShape& shape, const ArrayListIndex& index,
    std::uint64_t row_index, const ArrayValuePath& path,
    const std::optional<ArrayElementRange>& range) {
  auto valid = validate_physical_array_shape(shape);
  if (!valid.ok())
    return QueryResult<ResolvedArrayValue>::failure(std::move(valid));
  if (path.child_indices.size() + 1 != shape.list_depth)
    return array_failure<ResolvedArrayValue>(QueryError::InvalidQuery,
      "physical array path does not match List depth");
  if (index.list_depth() != shape.list_depth)
    return array_failure<ResolvedArrayValue>(QueryError::CorruptLayout,
      "physical array offset index does not match List depth");
  if (range && range->begin > range->end)
    return array_failure<ResolvedArrayValue>(QueryError::InvalidDomainSlice,
      "physical array element range is reversed");

  auto current = row_index;
  for (std::uint32_t level = 0; level < shape.list_depth; ++level) {
    if (current >= index.list_count(level))
      return array_failure<ResolvedArrayValue>(QueryError::InvalidQuery,
        "physical array List index is outside the stored level");
    auto span = index.list_span(level, current);
    if (!span.ok())
      return QueryResult<ResolvedArrayValue>::failure(span.status());
    const auto child_count = level + 1 == shape.list_depth
      ? index.leaf_count() : index.list_count(level + 1);
    if (span.value().begin > span.value().end ||
        span.value().end > child_count)
      return array_failure<ResolvedArrayValue>(QueryError::CorruptLayout,
        "physical array List offsets are outside the child level");
    if (!span.value().valid)
      return QueryResult<ResolvedArrayValue>::success({false, 0, 0, level});

    if (level + 1 < shape.list_depth) {
      const auto ordinal = path.child_indices[level];
      if (ordinal >= span.value().end - span.value().begin)
        return array_failure<ResolvedArrayValue>(QueryError::InvalidQuery,
          "physical array child index is outside the selected List");
      current = span.value().begin + ordinal;
      continue;
    }

    const auto size = span.value().end - span.value().begin;
    if (range && range->end > size)
      return array_failure<ResolvedArrayValue>(QueryError::InvalidDomainSlice,
        "physical array element range exceeds the selected List");
    return QueryResult<ResolvedArrayValue>::success({true,
      span.value().begin + (range ? range->begin : 0),
      span.value().begin + (range ? range->end : size)});
  }
  return array_failure<ResolvedArrayValue>(QueryError::Internal,
    "physical array resolution did not reach the leaf level");
}

} // namespace iotdbfs
