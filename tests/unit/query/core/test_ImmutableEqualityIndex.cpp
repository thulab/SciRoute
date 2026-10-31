#include "iotdbfs/query/core/ImmutableEqualityIndex.h"
#include <gtest/gtest.h>
#include <future>
#include <limits>
#include <numeric>

using namespace iotdbfs;
namespace {
using Key = EqualityCompositeKey<1, 1>;
using Index = OrderedEqualityIndex<Key, unsigned>;
constexpr std::array<std::string_view, 2> fields{"record.name", "record.level"};
QueryExpressionPtr predicate(std::string text = "temperature", std::int64_t number = 850) {
  return make_logical(LogicalOperator::And, {
    make_comparison({"record.name"}, ComparisonOperator::Equal, LogicalValue::utf8(std::move(text))),
    make_comparison({"record.level"}, ComparisonOperator::Equal, LogicalValue::int64(number))});
}
std::vector<unsigned> lookup(const Index& index, const Key& key) {
  std::vector<unsigned> result;
  const auto range = index.equal_range(key);
  for (auto it = range.first; it != range.second; ++it) result.push_back(it->locator);
  return result;
}
}

TEST(ImmutableEqualityIndex, AdapterFieldNamesAndNestedOrderCompileWithoutOwningLiteralCopies) {
  const auto expression = predicate(std::string("a\0b", 3), std::numeric_limits<std::int64_t>::min());
  auto leaves = std::get<LogicalExpression>(expression->node).operands;
  const auto nested = make_logical(LogicalOperator::And, {leaves[1],
    make_logical(LogicalOperator::And, {leaves[0]})});
  auto key = compile_complete_equality_key<1, 1>(nested, fields);
  ASSERT_TRUE(key);
  EXPECT_EQ(key->text[0], std::string_view("a\0b", 3));
  EXPECT_EQ(key->number[0], std::numeric_limits<std::int64_t>::min());
  const auto& literal = std::get<std::string>(std::get<ComparisonExpression>(leaves[0]->node).value.value());
  EXPECT_EQ(key->text[0].data(), literal.data());
  const std::array<std::string_view, 2> other_fields{"other.name", "other.level"};
  EXPECT_FALSE((compile_complete_equality_key<1, 1>(nested, other_fields)));
}

TEST(ImmutableEqualityIndex, UnsupportedSyntaxAndIncompleteOrMismatchedTypesDecline) {
  const auto valid = predicate();
  const auto leaves = std::get<LogicalExpression>(valid->node).operands;
  const std::vector<QueryExpressionPtr> invalid{
    nullptr, make_logical(LogicalOperator::And, {}),
    make_logical(LogicalOperator::And, {leaves[0]}),
    make_logical(LogicalOperator::And, {leaves[0], leaves[1], leaves[0]}),
    make_logical(LogicalOperator::Or, leaves),
    make_logical(LogicalOperator::Not, {valid}),
    make_logical(LogicalOperator::And, {leaves[0], make_is_null({"record.level"})}),
    make_logical(LogicalOperator::And, {leaves[0], make_in({"record.level"}, {LogicalValue::int64(850)})}),
    make_logical(LogicalOperator::And, {leaves[0], make_comparison({"record.level"}, ComparisonOperator::Less, LogicalValue::int64(850))}),
    make_logical(LogicalOperator::And, {leaves[0], make_comparison({"record.level"}, ComparisonOperator::Equal, LogicalValue::uint64(850))}),
    make_logical(LogicalOperator::And, {leaves[0], make_comparison({"record.level"}, ComparisonOperator::Equal, LogicalValue::null(LogicalTypeId::Int64))}),
    make_logical(LogicalOperator::And, {leaves[0], make_comparison({"unknown"}, ComparisonOperator::Equal, LogicalValue::int64(850))})};
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    EXPECT_FALSE((compile_complete_equality_key<1, 1>(invalid[i], fields)));
  }
}

TEST(ImmutableEqualityIndex, AstDepthBoundaryIsPreserved) {
  auto at_limit = predicate(); // root at depth0, leaves at depth1
  for (unsigned i = 0; i < 31; ++i)
    at_limit = make_logical(LogicalOperator::And, {at_limit});
  EXPECT_TRUE((compile_complete_equality_key<1, 1>(at_limit, fields)));
  auto over = make_logical(LogicalOperator::And, {at_limit});
  EXPECT_FALSE((compile_complete_equality_key<1, 1>(over, fields)));
}

TEST(ImmutableEqualityIndex, AstNodeBoundaryIsIndependentOfAdapterFieldCount) {
  std::array<std::string, 64> names;
  std::array<std::string_view, 64> declared;
  std::vector<QueryExpressionPtr> leaves;
  for (std::size_t i = 0; i < names.size(); ++i) {
    names[i] = "slot." + std::to_string(i); declared[i] = names[i];
    auto leaf = make_comparison({names[i]}, ComparisonOperator::Equal, LogicalValue::int64(i));
    // root+64 leaves+63 unary ANDs =128 nodes; all depths remain <=2.
    leaves.push_back(i == 63 ? leaf : make_logical(LogicalOperator::And, {leaf}));
  }
  const auto at_limit = make_logical(LogicalOperator::And, leaves);
  EXPECT_TRUE((compile_complete_equality_key<0, 64>(at_limit, declared)));
  leaves[63] = make_logical(LogicalOperator::And, {leaves[63]});
  EXPECT_FALSE((compile_complete_equality_key<0, 64>(make_logical(LogicalOperator::And, leaves), declared)));
}

TEST(ImmutableEqualityIndex, EqualKeysUseAdapterLocatorOrderAndKeepAllDuplicateRows) {
  std::vector<Index::Entry> entries;
  entries.reserve(32);
  const Key hit{{"temperature"}, {850}}, miss{{"temperature"}, {500}};
  entries.push_back({hit, 1}); entries.push_back({miss, 8});
  entries.push_back({hit, 4}); entries.push_back({hit, 2});
  const auto capacity = entries.capacity();
  const Index index(std::move(entries), std::greater<unsigned>{});
  EXPECT_EQ(index.size(), 4U);
  EXPECT_EQ(index.capacity(), capacity);
  EXPECT_EQ(lookup(index, hit), (std::vector<unsigned>{4,2,1}));
  EXPECT_EQ(lookup(index, miss), (std::vector<unsigned>{8}));
  EXPECT_TRUE(lookup(index, Key{{"absent"}, {850}}).empty());
  EXPECT_EQ(index.entries().size(), index.size());
}

TEST(ImmutableEqualityIndex, EmptyDomainAndIntegerOnlyKeysAreSupported) {
  const Index empty;
  EXPECT_TRUE(empty.empty());
  const auto range = empty.equal_range(Key{{"any"}, {0}});
  EXPECT_EQ(range.first, range.second);
  using Numbers = EqualityCompositeKey<0, 2>;
  using NumberIndex = OrderedEqualityIndex<Numbers, unsigned>;
  const NumberIndex numbers({{{{}, {1, -2}}, 7}, {{{}, {-1, 2}}, 9}}, std::less<unsigned>{});
  auto found = numbers.equal_range(Numbers{{}, {1, -2}});
  ASSERT_EQ(std::distance(found.first, found.second), 1);
  EXPECT_EQ(found.first->locator, 7U);
}

TEST(ImmutableEqualityIndex, LookupsAgreeWithIndependentUnorderedCandidateScan) {
  std::vector<Index::Entry> entries;
  std::vector<std::pair<std::int64_t, unsigned>> candidates;
  for (unsigned i = 0; i < 257; ++i) {
    const auto number = static_cast<std::int64_t>((i * 37) % 11);
    entries.push_back({Key{{"shared-prefix"}, {number}}, i});
    candidates.emplace_back(number, i);
  }
  const Index index(std::move(entries), std::less<unsigned>{});
  for (std::int64_t number = -1; number <= 11; ++number) {
    std::vector<unsigned> expected;
    for (const auto& candidate : candidates)
      if (candidate.first == number) expected.push_back(candidate.second);
    EXPECT_EQ(lookup(index, Key{{"shared-prefix"}, {number}}), expected);
  }
}

TEST(ImmutableEqualityIndex, ImmutableStorageSupportsConcurrentReadOnlyLookup) {
  const auto index = std::make_shared<const Index>(std::vector<Index::Entry>{
      {Key{{"same"}, {3}}, 4}, {Key{{"other"}, {3}}, 0}, {Key{{"same"}, {3}}, 2}},
      std::less<unsigned>{});
  std::vector<std::future<bool>> calls;
  for (unsigned i = 0; i < 8; ++i) calls.push_back(std::async(std::launch::async, [index] {
    for (unsigned j = 0; j < 100; ++j)
      if (lookup(*index, Key{{"same"}, {3}}) != std::vector<unsigned>{2,4}) return false;
    return true;
  }));
  for (auto& call : calls) EXPECT_TRUE(call.get());
}
