#include "iotdbfs/formats/arrow/ArrowQueryModel.h"

#include <gtest/gtest.h>

#include <utility>

using namespace iotdbfs;

namespace {

class OffsetIndex final : public ArrayListIndex {
public:
  explicit OffsetIndex(std::vector<std::vector<ArrayListSpan>> levels,
      std::uint64_t leaves)
    : levels_(std::move(levels)), leaves_(leaves) {}

  std::uint32_t list_depth() const override { return levels_.size(); }

  std::uint64_t list_count(std::uint32_t level) const override {
    return levels_.at(level).size();
  }

  std::uint64_t leaf_count() const override { return leaves_; }

  QueryResult<ArrayListSpan> list_span(std::uint32_t level,
      std::uint64_t index) const override {
    return QueryResult<ArrayListSpan>::success(levels_.at(level).at(index));
  }

private:
  std::vector<std::vector<ArrayListSpan>> levels_;
  std::uint64_t leaves_;
};

} // namespace

TEST(ArrowQueryModel, PhysicalBindingPublishesOrderedPositionalContract) {
  ArrowPhysicalBinding binding;
  binding.subject = {"target", 2, 7, {{{2}}, {{0}}}};
  auto capabilities = make_arrow_query_capabilities(binding);
  ASSERT_TRUE(capabilities.ok()) << capabilities.status().message();
  const auto& contract = capabilities.value();
  EXPECT_TRUE(validate_query_capabilities(contract).ok());
  EXPECT_EQ(contract.adapter_id(), "arrow");
  EXPECT_EQ(contract.ordering(), OrderingGuarantee::Domain);
  EXPECT_EQ(contract.domains(), std::vector<DomainKind>{DomainKind::Rows});
  const auto& fields = contract.schema().fields();
  ASSERT_EQ(fields.size(), 3u);
  EXPECT_EQ(fields[0].field_id, "index");
  EXPECT_EQ(fields[0].type.id, LogicalTypeId::UInt64);
  EXPECT_FALSE(fields[0].type.nullable);
  EXPECT_EQ(fields[1].field_id, "value.2");
  EXPECT_EQ(fields[2].field_id, "value.0");
  EXPECT_EQ(fields[1].type.id, LogicalTypeId::Float32);
  EXPECT_TRUE(fields[1].type.nullable);
  // Index slicing is exact; value predicates remain residual operations.
  const auto& queries = contract.fields();
  ASSERT_EQ(queries.size(), 3u);
  EXPECT_TRUE(queries[0].sliceable);
  EXPECT_EQ(queries[0].slice_pushdown, PushdownLevel::Exact);
  EXPECT_FALSE(queries[1].sliceable);
  EXPECT_EQ(queries[1].predicate_pushdown, PushdownLevel::None);
  EXPECT_TRUE(queries[1].null_predicate);
  EXPECT_EQ(queries[1].predicate_operators.size(), 6u);

  binding.subject.values = {{{}}};
  auto single = make_arrow_logical_schema(binding);
  ASSERT_TRUE(single.ok());
  ASSERT_EQ(single.value().fields().size(), 2u);
  EXPECT_EQ(single.value().fields()[1].field_id, "value");
}

TEST(ArrowQueryModel, OneListLevelUsesAnEmptyChildPath) {
  OffsetIndex index({{{0, 5, true}, {5, 5, true}, {5, 8, false}}}, 8);
  auto first = resolve_array_value({1, PhysicalArrayLeafType::Float32}, index,
    0, {{}}, ArrayElementRange{1, 4});
  ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_TRUE(first.value().valid);
  EXPECT_FALSE(first.value().null_list_level);
  EXPECT_EQ(first.value().begin, 1u);
  EXPECT_EQ(first.value().end, 4u);

  auto empty = resolve_array_value({1, PhysicalArrayLeafType::Float32}, index,
    1, {{}});
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().valid);
  EXPECT_FALSE(empty.value().null_list_level);
  EXPECT_EQ(empty.value().begin, empty.value().end);

  auto null_list = resolve_array_value({1, PhysicalArrayLeafType::Float32}, index,
    2, {{}});
  ASSERT_TRUE(null_list.ok());
  EXPECT_FALSE(null_list.value().valid);
  EXPECT_EQ(null_list.value().null_list_level, 0u);
}

TEST(ArrowQueryModel, TwoListLevelsResolveChildOrdinalsWithoutMeaning) {
  OffsetIndex index({
    {{0, 3, true}, {3, 5, true}},
    {{0, 4, true}, {4, 9, true}, {9, 14, true},
      {14, 17, true}, {17, 21, false}},
  }, 21);

  auto second = resolve_array_value({2, PhysicalArrayLeafType::Float32}, index,
    0, {{1}}, ArrayElementRange{2, 5});
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_TRUE(second.value().valid);
  EXPECT_FALSE(second.value().null_list_level);
  EXPECT_EQ(second.value().begin, 6u);
  EXPECT_EQ(second.value().end, 9u);

  auto last = resolve_array_value({2, PhysicalArrayLeafType::Float32}, index,
    1, {{1}});
  ASSERT_TRUE(last.ok());
  EXPECT_FALSE(last.value().valid);
  EXPECT_EQ(last.value().null_list_level, 1u);

  EXPECT_EQ(resolve_array_value({2, PhysicalArrayLeafType::Float32}, index,
    1, {{2}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(resolve_array_value({2, PhysicalArrayLeafType::Float32}, index,
    0, {{}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(resolve_array_value({1, PhysicalArrayLeafType::Float32}, index,
    0, {{1}}).status().code(), QueryError::InvalidQuery);
}

TEST(ArrowQueryModel, NullParentShortCircuitsUnobservableChildOrdinal) {
  OffsetIndex index({
    {{0, 2, false}, {2, 3, true}},
    {{0, 1, true}, {1, 2, true}, {2, 3, false}},
  }, 3);

  auto null_parent = resolve_array_value(
    {2, PhysicalArrayLeafType::Float32}, index, 0, {{7}});
  ASSERT_TRUE(null_parent.ok()) << null_parent.status().message();
  EXPECT_FALSE(null_parent.value().valid);
  EXPECT_EQ(null_parent.value().null_list_level, 0u);

  auto null_child = resolve_array_value(
    {2, PhysicalArrayLeafType::Float32}, index, 1, {{0}});
  ASSERT_TRUE(null_child.ok()) << null_child.status().message();
  EXPECT_FALSE(null_child.value().valid);
  EXPECT_EQ(null_child.value().null_list_level, 1u);

  EXPECT_EQ(resolve_array_value(
    {2, PhysicalArrayLeafType::Float32}, index, 1, {{7}})
    .status().code(), QueryError::InvalidQuery);
}

TEST(ArrowQueryModel, RejectsInvalidShapeAndCorruptOffsets) {
  OffsetIndex corrupt({{{0, 12, true}}}, 8);
  EXPECT_EQ(resolve_array_value({1, PhysicalArrayLeafType::Float32}, corrupt,
    0, {{}}).status().code(), QueryError::CorruptLayout);
  EXPECT_EQ(resolve_array_value({1, PhysicalArrayLeafType::Float32}, corrupt,
    1, {{}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(resolve_array_value({1, PhysicalArrayLeafType::Float32}, corrupt,
    0, {{}}, ArrayElementRange{4, 2}).status().code(),
    QueryError::InvalidDomainSlice);
  EXPECT_EQ(validate_physical_array_shape({3, PhysicalArrayLeafType::Float32}).code(),
    QueryError::UnsupportedCapability);
  EXPECT_EQ(validate_physical_array_shape({1, PhysicalArrayLeafType::Float64}).code(),
    QueryError::UnsupportedCapability);
  EXPECT_EQ(resolve_array_value({2, PhysicalArrayLeafType::Float32}, corrupt,
    0, {{0}}).status().code(), QueryError::CorruptLayout);

  OffsetIndex valid({{{0, 5, true}}}, 5);
  EXPECT_EQ(resolve_array_value({1, PhysicalArrayLeafType::Float32}, valid,
    0, {{}}, ArrayElementRange{2, 6}).status().code(),
    QueryError::InvalidDomainSlice);
}

TEST(ArrowQueryModel, ValidatesPhysicalSelectionWithoutSemanticMetadata) {
  PhysicalArraySubject one{"target", 2, 7, {{{}}}};
  EXPECT_TRUE(validate_physical_array_subject(one).ok());
  PhysicalArraySubject two{"target", 2, 7, {{{2}}, {{0}}}};
  EXPECT_TRUE(validate_physical_array_subject(two).ok());

  two.values.push_back({{2}});
  EXPECT_EQ(validate_physical_array_subject(two).code(),
    QueryError::InvalidQuery);
  two.values = {{{}}, {{1}}};
  EXPECT_EQ(validate_physical_array_subject(two).code(),
    QueryError::InvalidQuery);
  one.values.clear();
  EXPECT_EQ(validate_physical_array_subject(one).code(),
    QueryError::InvalidQuery);
}
