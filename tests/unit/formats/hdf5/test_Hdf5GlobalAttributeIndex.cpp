#include "iotdbfs/formats/hdf5/Hdf5GlobalAttributeIndex.h"
#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include <gtest/gtest.h>
#include <atomic>
#include <future>
#include <limits>
using namespace iotdbfs;
namespace {
DatasetCatalogRecord index_record(std::string id, std::int64_t level = 850) {
  DatasetCatalogRecord row{id, "/" + id, true, {}};
  const auto add = [&](const std::string& name, DatasetAttributeValue value) {
    row.attributes.push_back({name, DatasetAttributeState::Complete, std::move(value), {}, {}});
  };
  add("shortName", std::string("t")); add("typeOfLevel", std::string("isobaricInhPa"));
  add("level", level); add("step", std::int64_t(0));
  add("validityDate", std::int64_t(20260101)); add("validityTime", std::int64_t(0));
  return row;
}
std::shared_ptr<const DatasetCatalogArtifact> index_catalog(std::vector<DatasetCatalogRecord> rows) {
  auto result = std::make_shared<DatasetCatalogArtifact>(); result->format_id = "hdf5";
  result->records = std::move(rows); return result;
}
QueryExpressionPtr index_predicate(std::int64_t level = 850) {
  std::vector<QueryExpressionPtr> leaves;
  const auto add = [&](const char* name, LogicalValue value) {
    leaves.push_back(make_comparison({std::string("hdf5.attr:") + name}, ComparisonOperator::Equal, std::move(value)));
  };
  add("shortName", LogicalValue::utf8("t")); add("typeOfLevel", LogicalValue::utf8("isobaricInhPa"));
  add("level", LogicalValue::int64(level)); add("step", LogicalValue::int64(0));
  add("validityDate", LogicalValue::int64(20260101)); add("validityTime", LogicalValue::int64(0));
  return make_logical(LogicalOperator::And, std::move(leaves));
}
}
TEST(Hdf5GlobalAttributeIndex, CrossFileLookupPreservesSourceAndObjectOrder) {
  auto first = index_catalog({index_record("z"), index_record("a"), index_record("miss", 500)});
  auto second = index_catalog({index_record("b")});
  auto index = Hdf5GlobalAttributeIndex::build({first, second}, 64ULL << 20);
  ASSERT_TRUE(index);
  auto matches = index->lookup(index_predicate()); ASSERT_TRUE(matches);
  const std::vector<Hdf5GlobalAttributeIndex::Match> expected{{0,1},{0,0},{1,0}};
  EXPECT_EQ(*matches, expected);
  EXPECT_EQ(index->catalogs()[0].get(), first.get());
  EXPECT_EQ(index->candidate_counts(), (std::vector<std::size_t>{3,1}));
}
TEST(Hdf5GlobalAttributeIndex, CompleteMissingAttributeDeclinesAndGenericPreservesNullSemantics) {
  auto row = index_record("missing"); row.attributes.erase(row.attributes.begin());
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({row})}, 64ULL << 20));
  auto predicate = DatasetAttributePredicate::compile(index_predicate()); ASSERT_TRUE(predicate.ok());
  auto generic = predicate.value().evaluate(row); ASSERT_TRUE(generic.ok()); EXPECT_FALSE(generic.value());
  auto is_null = DatasetAttributePredicate::compile(make_is_null({"hdf5.attr:shortName"}));
  ASSERT_TRUE(is_null.ok()); auto missing = is_null.value().evaluate(row);
  ASSERT_TRUE(missing.ok()); EXPECT_TRUE(missing.value());
}
TEST(Hdf5GlobalAttributeIndex, PredicateMissCannotHideUnsupportedAttributeOrTypeError) {
  auto bad = index_record("miss", 500);
  bad.attributes[3].state = DatasetAttributeState::Unsupported;
  bad.attributes[3].value.reset();
  auto predicate = DatasetAttributePredicate::compile(index_predicate()); ASSERT_TRUE(predicate.ok());
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({index_record("hit"), bad})}, 64ULL << 20));
  auto generic = predicate.value().evaluate(bad);
  ASSERT_FALSE(generic.ok()); EXPECT_EQ(generic.status().code(), QueryError::UnsupportedCapability);
  bad = index_record("wrong-type-miss", 500); bad.attributes[3].value = std::uint64_t(0);
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({index_record("hit"), bad})}, 64ULL << 20));
  generic = predicate.value().evaluate(bad);
  ASSERT_FALSE(generic.ok()); EXPECT_EQ(generic.status().code(), QueryError::TypeMismatch);
}
TEST(Hdf5GlobalAttributeIndex, IncompleteMissingDeclinesAndGenericStillErrors) {
  auto row = index_record("incomplete"); row.attributes_complete = false; row.attributes.pop_back();
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({row})}, 64ULL << 20));
  auto predicate = DatasetAttributePredicate::compile(index_predicate()); ASSERT_TRUE(predicate.ok());
  auto generic = predicate.value().evaluate(row);
  ASSERT_FALSE(generic.ok()); EXPECT_EQ(generic.status().code(), QueryError::LayoutUnavailable);
}
TEST(Hdf5GlobalAttributeIndex, UnsupportedPredicatesDeclineRatherThanReturnWrongEmpty) {
  auto index = Hdf5GlobalAttributeIndex::build({index_catalog({index_record("hit")})}, 64ULL << 20);
  ASSERT_TRUE(index);
  std::vector<QueryExpressionPtr> unsupported{nullptr, make_is_null({"hdf5.attr:shortName"}),
    make_logical(LogicalOperator::Or, {index_predicate()}),
    make_logical(LogicalOperator::Not, {index_predicate()}),
    make_in({"hdf5.attr:level"}, {LogicalValue::int64(850)}),
    make_comparison({"hdf5.attr:level"}, ComparisonOperator::Equal, LogicalValue::uint64(850)),
    make_logical(LogicalOperator::And, {})};
  auto leaves = std::get<LogicalExpression>(index_predicate()->node).operands;
  leaves[2] = make_comparison({"hdf5.attr:level"}, ComparisonOperator::Equal, LogicalValue::float64(850));
  unsupported.push_back(make_logical(LogicalOperator::And, leaves));
  leaves = std::get<LogicalExpression>(index_predicate()->node).operands;
  leaves.push_back(leaves.front()); unsupported.push_back(make_logical(LogicalOperator::And, leaves));
  leaves.pop_back(); leaves.pop_back(); unsupported.push_back(make_logical(LogicalOperator::And, leaves));
  for (const auto& predicate : unsupported) {
    EXPECT_FALSE(Hdf5GlobalAttributeIndex::supports(predicate));
    EXPECT_FALSE(index->lookup(predicate).has_value());
  }
  auto empty = index->lookup(index_predicate(1000)); ASSERT_TRUE(empty); EXPECT_TRUE(empty->empty());
}
TEST(Hdf5GlobalAttributeIndex, NestedAndReorderedLeavesAndArbitraryValuesRemainEquivalent) {
  auto leaves = std::get<LogicalExpression>(index_predicate(500)->node).operands;
  auto nested = make_logical(LogicalOperator::And, {leaves[5], leaves[2],
    make_logical(LogicalOperator::And, {leaves[4], leaves[0], leaves[3], leaves[1]})});
  auto index = Hdf5GlobalAttributeIndex::build({index_catalog({index_record("a", 500), index_record("b", 850)})}, 64ULL << 20);
  ASSERT_TRUE(index); EXPECT_TRUE(Hdf5GlobalAttributeIndex::supports(nested));
  auto matches = index->lookup(nested); ASSERT_TRUE(matches);
  EXPECT_EQ(*matches, (std::vector<Hdf5GlobalAttributeIndex::Match>{{0,0}}));
  EXPECT_EQ(matches, index->lookup(index_predicate(500)));
}
TEST(Hdf5GlobalAttributeIndex, ExactDatasetDomainDoesNotValidateUnselectedRecords) {
  auto bad = index_record("outside"); bad.attributes.clear(); bad.attributes_complete = false;
  auto good = index_record("selected");
  auto index = Hdf5GlobalAttributeIndex::build({index_catalog({}), index_catalog({bad, good}), index_catalog({bad})}, 64ULL << 20, "/selected");
  ASSERT_TRUE(index); EXPECT_EQ(index->candidate_counts(), (std::vector<std::size_t>{0,1,0}));
  auto matches = index->lookup(index_predicate()); ASSERT_TRUE(matches);
  EXPECT_EQ(*matches, (std::vector<Hdf5GlobalAttributeIndex::Match>{{1,1}}));
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({bad, good})}, 64ULL << 20));
}
TEST(Hdf5GlobalAttributeIndex, DoesNotReselectHistoricalMatchOutsideLatestFrozenInput) {
  auto historical = index_catalog({index_record("dataset", 850)});
  auto latest = index_catalog({index_record("dataset", 500)});
  auto index = Hdf5GlobalAttributeIndex::build({latest}, 64ULL << 20);
  ASSERT_TRUE(index); auto match = index->lookup(index_predicate()); ASSERT_TRUE(match); EXPECT_TRUE(match->empty());
  auto all = Hdf5GlobalAttributeIndex::build({historical, latest}, 64ULL << 20);
  ASSERT_TRUE(all); auto all_matches = all->lookup(index_predicate()); ASSERT_TRUE(all_matches);
  EXPECT_EQ(*all_matches, (std::vector<Hdf5GlobalAttributeIndex::Match>{{0,0}}));
}
TEST(Hdf5GlobalAttributeIndex, RetainedCatalogAndIndexCapacityAreChargedAndBounded) {
  auto catalog = index_catalog({index_record("a"), index_record("b")});
  auto index = Hdf5GlobalAttributeIndex::build({catalog}, 64ULL << 20); ASSERT_TRUE(index);
  auto charge = index->estimated_bytes(); EXPECT_GT(charge, sizeof(DatasetCatalogArtifact));
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({catalog}, charge - 1));
  auto exact = Hdf5GlobalAttributeIndex::build({catalog}, charge); ASSERT_TRUE(exact);
  EXPECT_EQ(exact->estimated_bytes(), charge);
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({catalog}, 0));
  std::weak_ptr<const DatasetCatalogArtifact> weak = catalog; catalog.reset();
  EXPECT_FALSE(weak.expired()); index.reset(); exact.reset(); EXPECT_TRUE(weak.expired());
}
TEST(Hdf5GlobalAttributeIndex, NullCatalogAndNonstandardSixFieldDomainDecline) {
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({nullptr}, 64ULL << 20));
  auto row = index_record("a"); row.attributes.push_back(row.attributes.front());
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({row})}, 64ULL << 20));
  row = index_record("a"); row.attributes[0].shape = {2};
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({index_catalog({row})}, 64ULL << 20));
  auto artifact = std::make_shared<DatasetCatalogArtifact>(*index_catalog({index_record("a")}));
  artifact->directory_complete = false;
  EXPECT_FALSE(Hdf5GlobalAttributeIndex::build({artifact}, 64ULL << 20));
}
TEST(Hdf5GlobalAttributeIndex, EmptyCertifiedDomainIsSupportedEmptyNotUnsupported) {
  auto index = Hdf5GlobalAttributeIndex::build({index_catalog({})}, 64ULL << 20); ASSERT_TRUE(index);
  auto matches = index->lookup(index_predicate()); ASSERT_TRUE(matches); EXPECT_TRUE(matches->empty());
  EXPECT_EQ(index->candidate_counts(), (std::vector<std::size_t>{0}));
}
TEST(Hdf5GlobalAttributeIndex, ConstLookupsShareImmutableStateAcrossThreads) {
  auto index = Hdf5GlobalAttributeIndex::build({index_catalog({index_record("a", 500), index_record("b")}), index_catalog({index_record("c")})}, 64ULL << 20);
  ASSERT_TRUE(index); auto predicate = index_predicate();
  std::vector<std::future<bool>> pending;
  for (unsigned thread = 0; thread < 8; ++thread) pending.push_back(std::async(std::launch::async, [index, predicate] {
    for (unsigned attempt = 0; attempt < 500; ++attempt) {
      auto result = index->lookup(predicate);
      if (!result || *result != std::vector<Hdf5GlobalAttributeIndex::Match>{{0,1},{1,0}}) return false;
    }
    return true;
  }));
  for (auto& result : pending) EXPECT_TRUE(result.get());
}
