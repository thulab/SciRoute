#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include <gtest/gtest.h>
#include <limits>
using namespace iotdbfs;
namespace {
DatasetCatalogRecord record(std::string id = "1", std::string name = "sst") {
  return {id,
          "/" + id,
          true,
          {{"standard_name",
            DatasetAttributeState::Complete,
            DatasetAttributeValue(name),
            {},
            {}},
           {"n",
            DatasetAttributeState::Complete,
            DatasetAttributeValue(std::uint64_t(18446744073709551615ULL)),
            {1},
            {}}}};
}
DatasetCatalogArtifact artifact() {
  return {"iotdbfs-dataset-catalog/1", "hdf5", "IOTDBFS-HDF5-LAYOUT/4\n",
          std::string(64, 'a'),        true,   {record()}};
}
StructuredBlobManifest manifest(int version) {
  StructuredBlobManifest m;
  m.key = {"/a.h5", version};
  m.format_id = "hdf5";
  m.published = true;
  m.phase = StructuredBlobPhase::Published;
  m.layout_size = 128;
  return m;
}
QueryExpressionPtr eq(std::string text = "sst") {
  return make_comparison({"hdf5.attr:standard_name"}, ComparisonOperator::Equal,
                         LogicalValue::utf8(text));
}
} // namespace
TEST(DatasetCatalog, BinaryRoundTripPreservesUint64AndShape) {
  auto a = artifact();
  auto bytes = serialize_dataset_catalog(a);
  ASSERT_TRUE(bytes.ok()) << bytes.error();
  auto b = deserialize_dataset_catalog(bytes.value());
  ASSERT_TRUE(b.ok()) << b.error();
  EXPECT_EQ(std::get<std::uint64_t>(*b.value().records[0].attributes[1].value),
            std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(b.value().records[0].attributes[1].shape,
            std::vector<std::uint64_t>{1});
}
TEST(DatasetCatalog, VerifiedUtcCoveragePrunesBeforeLoadingCatalog) {
  DatasetCatalogRequest missing_path;
  missing_path.time_range = DatasetCatalogRequest::UtcTimeRange{1, 2};
  EXPECT_EQ(validate_dataset_catalog_request(missing_path).code(),
            QueryError::InvalidQuery);
  auto m = manifest(7);
  m.metadata = {
      {"__iotdbfs.time_coverage.protocol", MetadataValue("utc-point/1")},
      {"__iotdbfs.time_coverage.dataset_path", MetadataValue("/T")},
      {"__iotdbfs.time_coverage.utc_second", MetadataValue(std::int64_t(1309507200))},
      {"__iotdbfs.time_coverage.layout_digest", MetadataValue(std::string(64, 'a'))},
      {"__iotdbfs.dataset_catalog.layout_digest", MetadataValue(std::string(64, 'a'))}};
  auto source = make_in_memory_published_version_catalog({m});
  DatasetCatalogRequest request;
  request.dataset_path = "/T";
  request.time_range = DatasetCatalogRequest::UtcTimeRange{1309507200, 1309507201};
  unsigned loads = 0;
  auto find = [&](const DatasetCatalogRequest &q) {
    return discover_datasets_reference(*source, q,
      [&](const StructuredBlobManifest &) {
        ++loads;
        return QueryResult<DatasetCatalogArtifact>::success(artifact());
      }, [](const auto &, const CatalogSelection&) { return QueryStatus::success(); });
  };
  auto hit = find(request);
  ASSERT_TRUE(hit.ok()) << hit.status().message();
  EXPECT_EQ(hit.value().selected_sources, 1u);
  EXPECT_EQ(hit.value().time_candidate_sources, 1u);
  EXPECT_EQ(loads, 1u);
  request.time_range = DatasetCatalogRequest::UtcTimeRange{1309507201, 1309507202};
  auto miss = find(request);
  ASSERT_TRUE(miss.ok()) << miss.status().message();
  EXPECT_TRUE(miss.value().datasets.empty());
  EXPECT_EQ(miss.value().time_candidate_sources, 0u);
  EXPECT_EQ(loads, 1u);
  request.time_range = DatasetCatalogRequest::UtcTimeRange{1309507201, 1309507201};
  EXPECT_EQ(validate_dataset_catalog_request(request).code(),
            QueryError::InvalidDomainSlice);
  EXPECT_EQ(matches_verified_utc_time(m, "/U", {1309507200, 1309507201})
                .status().code(), QueryError::LayoutUnavailable);
  m.metadata["__iotdbfs.time_coverage.layout_digest"] =
      MetadataValue(std::string(64, 'b'));
  EXPECT_EQ(matches_verified_utc_time(m, "/T", {1309507200, 1309507201})
                .status().code(), QueryError::LayoutUnavailable);
  m.metadata.erase("__iotdbfs.time_coverage.protocol");
  EXPECT_EQ(matches_verified_utc_time(m, "/T", {1309507200, 1309507201})
                .status().code(), QueryError::LayoutUnavailable);
}
TEST(DatasetCatalog, RejectsTruncationAndTrailingBytes) {
  auto b = serialize_dataset_catalog(artifact()).value();
  b.pop_back();
  EXPECT_FALSE(deserialize_dataset_catalog(b).ok());
  b = serialize_dataset_catalog(artifact()).value();
  b.push_back(0);
  EXPECT_FALSE(deserialize_dataset_catalog(b).ok());
}
TEST(DatasetCatalog, RejectsDuplicateObjectsAndAttributes) {
  auto a = artifact();
  a.records.push_back(a.records[0]);
  EXPECT_FALSE(validate_dataset_catalog(a).ok());
  a = artifact();
  a.records[0].attributes.push_back(a.records[0].attributes[0]);
  EXPECT_FALSE(validate_dataset_catalog(a).ok());
}
TEST(DatasetCatalog, RejectsNonFiniteAndNonScalarCompleteValues) {
  auto a = artifact();
  a.records[0].attributes[1].value = std::numeric_limits<double>::infinity();
  EXPECT_FALSE(validate_dataset_catalog(a).ok());
  a = artifact();
  a.records[0].attributes[1].shape = {2};
  EXPECT_FALSE(validate_dataset_catalog(a).ok());
}
TEST(DatasetCatalog, PredicatesPreserveObjectCorrelation) {
  auto r = record();
  auto conjunction = make_logical(
      LogicalOperator::And,
      {eq(), make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
                             LogicalValue::uint64(4))});
  auto matched = evaluate_dataset_attribute_predicate(r, conjunction);
  ASSERT_TRUE(matched.ok());
  EXPECT_FALSE(matched.value());
}
TEST(DatasetCatalog, Uint64ComparisonIsExact) {
  auto r = record();
  auto x = make_comparison({"hdf5.attr:n"}, ComparisonOperator::Greater,
                           LogicalValue::uint64(18446744073709551614ULL));
  auto m = evaluate_dataset_attribute_predicate(r, x);
  ASSERT_TRUE(m.ok());
  EXPECT_TRUE(m.value());
  auto sql = compile_dataset_predicate_sql(x);
  ASSERT_TRUE(sql.ok());
  EXPECT_NE(sql.value().predicate.find("18446744073709551614"),
            std::string::npos);
  EXPECT_EQ(dataset_attribute_text(DatasetAttributeValue(std::uint64_t(1))),
            "00000000000000000001");
}
TEST(DatasetCatalog, DoesNotCoerceNumericTypes) {
  auto r = record();
  auto m = evaluate_dataset_attribute_predicate(
      r, make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
                         LogicalValue::float64(1)));
  EXPECT_FALSE(m.ok());
  EXPECT_EQ(m.status().code(), QueryError::TypeMismatch);
}
TEST(DatasetCatalog, ProvenMissingUsesSqlNullSemantics) {
  auto r = record();
  EXPECT_TRUE(evaluate_dataset_attribute_predicate(
                  r, make_is_null({"hdf5.attr:absent"}))
                  .value());
  auto x = make_comparison({"hdf5.attr:absent"}, ComparisonOperator::Equal,
                           LogicalValue::utf8("x"));
  EXPECT_FALSE(evaluate_dataset_attribute_predicate(r, x).value());
  EXPECT_FALSE(evaluate_dataset_attribute_predicate(
                   r, make_logical(LogicalOperator::Not, {x}))
                   .value());
}
TEST(DatasetCatalog, UnreadableIsNotNullEvenInOr) {
  auto r = record();
  r.attributes[0].state = DatasetAttributeState::Unsupported;
  r.attributes[0].value.reset();
  EXPECT_FALSE(evaluate_dataset_attribute_predicate(
                   r, make_is_null({"hdf5.attr:standard_name"}))
                   .ok());
  auto x = make_logical(
      LogicalOperator::Or,
      {eq(), make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
                             LogicalValue::uint64(
                                 std::numeric_limits<std::uint64_t>::max()))});
  EXPECT_FALSE(evaluate_dataset_attribute_predicate(r, x).ok());
}
TEST(DatasetCatalog, IncompleteNamesCannotProveAbsence) {
  auto r = record();
  r.attributes_complete = false;
  EXPECT_FALSE(evaluate_dataset_attribute_predicate(
                   r, make_is_null({"hdf5.attr:absent"}))
                   .ok());
  EXPECT_TRUE(evaluate_dataset_attribute_predicate(r, eq()).value());
}
TEST(DatasetCatalog, InRejectsNullLiteralAccordingToCoreContract) {
  auto r = record();
  auto x = make_in(
      {"hdf5.attr:standard_name"},
      {LogicalValue::utf8("other"), LogicalValue::null(LogicalTypeId::Utf8)},
      true);
  auto m = evaluate_dataset_attribute_predicate(r, x);
  EXPECT_FALSE(m.ok());
  EXPECT_EQ(m.status().code(), QueryError::InvalidQuery);
}
TEST(DatasetCatalog, RejectsBadFieldEncodingBeforeDiscovery) {
  EXPECT_TRUE(dataset_attribute_name({"hdf5.attr:long%20name"}).ok());
  for (const auto &s : {"hdf5.attr:%61", "hdf5.attr:%ff", "hdf5.attr:%00",
                        "hdf5.attr:a b", "wrong:x"})
    EXPECT_FALSE(dataset_attribute_name({s}).ok()) << s;
}
TEST(DatasetCatalog, SqlUsesDistinctCorrelatedAliasesAndSafeLiterals) {
  auto x = make_logical(LogicalOperator::And,
                        {eq("o'clock"), make_is_null({"hdf5.attr:absent"})});
  auto sql = compile_dataset_predicate_sql(x);
  ASSERT_TRUE(sql.ok());
  EXPECT_EQ(sql.value().attributes.size(), 2);
  EXPECT_NE(sql.value().predicate.find("o''clock"), std::string::npos);
  EXPECT_NE(sql.value().predicate.find("IS NULL"), std::string::npos);
}
TEST(DatasetCatalog, SqlDoesNotAssumeDatabaseStringCollation) {
  auto x = make_logical(
      LogicalOperator::Not,
      {make_comparison({"hdf5.attr:standard_name"}, ComparisonOperator::Less,
                       LogicalValue::utf8("z"))});
  auto sql = compile_dataset_predicate_sql(x);
  ASSERT_TRUE(sql.ok());
  EXPECT_EQ(sql.value().predicate, "TRUE");
  EXPECT_TRUE(sql.value().attributes.empty());
}
TEST(DatasetCatalog, StringOrderingFallbackHasNoSqlAttributeDependencies) {
  for (const auto op : {ComparisonOperator::Less, ComparisonOperator::LessEqual,
                        ComparisonOperator::Greater, ComparisonOperator::GreaterEqual}) {
    SCOPED_TRACE(static_cast<int>(op));
    auto sql = compile_dataset_predicate_sql(make_comparison(
        {"hdf5.attr:standard_name"}, op, LogicalValue::utf8("z")));
    ASSERT_TRUE(sql.ok()) << sql.status().message();
    EXPECT_EQ(sql.value().predicate, "TRUE");
    EXPECT_TRUE(sql.value().attributes.empty());
  }
}
TEST(DatasetCatalog, WholeExpressionFallbackDropsEverySqlAttributeDependency) {
  const auto ordering = make_comparison({"hdf5.attr:standard_name"},
      ComparisonOperator::Less, LogicalValue::utf8("z"));
  const auto exact = make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
      LogicalValue::uint64(std::numeric_limits<std::uint64_t>::max()));
  const std::vector<QueryExpressionPtr> expressions = {
      make_logical(LogicalOperator::And, {exact, ordering}),
      make_logical(LogicalOperator::And, {ordering, exact}),
      make_logical(LogicalOperator::Or, {exact, ordering}),
      make_logical(LogicalOperator::Or, {ordering, exact}),
      make_logical(LogicalOperator::Not, {ordering}),
      make_logical(LogicalOperator::Not,
          {make_logical(LogicalOperator::And, {exact, ordering})}),
      make_logical(LogicalOperator::Or,
          {make_is_null({"hdf5.attr:absent"}),
           make_logical(LogicalOperator::Not, {ordering})})};
  for (std::size_t i = 0; i < expressions.size(); ++i) {
    SCOPED_TRACE(static_cast<int>(i));
    auto sql = compile_dataset_predicate_sql(expressions[i]);
    ASSERT_TRUE(sql.ok()) << sql.status().message();
    EXPECT_EQ(sql.value().predicate, "TRUE");
    EXPECT_TRUE(sql.value().attributes.empty());
  }
}
TEST(DatasetCatalog, ExactEqualityNullAndInKeepSqlAttributeDependencies) {
  const std::vector<QueryExpressionPtr> expressions = {
      eq(),
      make_comparison({"hdf5.attr:standard_name"}, ComparisonOperator::NotEqual,
          LogicalValue::utf8("sst")),
      make_is_null({"hdf5.attr:standard_name"}),
      make_is_null({"hdf5.attr:standard_name"}, true),
      make_in({"hdf5.attr:standard_name"},
          {LogicalValue::utf8("sst"), LogicalValue::utf8("other")}),
      make_in({"hdf5.attr:standard_name"},
          {LogicalValue::utf8("sst"), LogicalValue::utf8("other")}, true),
      make_logical(LogicalOperator::Not, {eq()})};
  for (std::size_t i = 0; i < expressions.size(); ++i) {
    SCOPED_TRACE(static_cast<int>(i));
    auto sql = compile_dataset_predicate_sql(expressions[i]);
    ASSERT_TRUE(sql.ok()) << sql.status().message();
    EXPECT_EQ(sql.value().attributes, std::vector<std::string>{"standard_name"});
    EXPECT_NE(sql.value().predicate, "TRUE");
    EXPECT_NE(sql.value().predicate.find("a0."), std::string::npos);
    auto batches = compile_dataset_catalog_index_sql_batches(
        {{{"/a.h5", 7}, 1, 2}}, sql.value());
    ASSERT_TRUE(batches.ok()) << batches.status().message();
    ASSERT_EQ(batches.value().size(), 1u);
    EXPECT_NE(batches.value()[0].filter_sql.find("LEFT JOIN blob_dataset_attribute a0"),
              std::string::npos);
  }
}
TEST(DatasetCatalog, FallbackRemovesAttributeJoinsButRetainsExactKeyIntegrityCounts) {
  const auto predicate = make_logical(LogicalOperator::And, {eq(),
      make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
          LogicalValue::uint64(4)),
      make_comparison({"hdf5.attr:standard_name"}, ComparisonOperator::Less,
          LogicalValue::utf8("z"))});
  auto sql = compile_dataset_predicate_sql(predicate);
  ASSERT_TRUE(sql.ok()) << sql.status().message();
  auto batches = compile_dataset_catalog_index_sql_batches(
      {{{"/a.h5", 7}, 1, 2}, {{"/a.h5", 8}, 1, 2}}, sql.value(),
      std::string("/T"));
  ASSERT_TRUE(batches.ok()) << batches.status().message();
  ASSERT_EQ(batches.value().size(), 1u);
  const auto& batch = batches.value()[0];
  EXPECT_TRUE(batch.filter_sql.find("LEFT JOIN") == std::string::npos);
  EXPECT_NE(batch.filter_sql.find("JOIN inode"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("i.published=true"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("d.dataset_path='/T' AND (TRUE)"),
            std::string::npos);
  for (const auto* count : {&batch.entry_count_sql, &batch.attribute_count_sql}) {
    EXPECT_NE(count->find("count(*)"), std::string::npos);
    EXPECT_NE(count->find("path='/a.h5' AND time=7"), std::string::npos);
    EXPECT_NE(count->find("path='/a.h5' AND time=8"), std::string::npos);
    EXPECT_NE(count->find("GROUP BY path,time"), std::string::npos);
    EXPECT_EQ(count->find("dataset_path"), std::string::npos);
  }
}
TEST(DatasetCatalog, FallbackStillUsesResidualTruthAndNullSemantics) {
  const auto ordering = make_comparison({"hdf5.attr:standard_name"},
      ComparisonOperator::Less, LogicalValue::utf8("z"));
  const auto negated = make_logical(LogicalOperator::Not, {ordering});
  auto matched = evaluate_dataset_attribute_predicate(record("1", "sst"), ordering);
  ASSERT_TRUE(matched.ok());
  EXPECT_TRUE(matched.value());
  matched = evaluate_dataset_attribute_predicate(record("2", "zz"), ordering);
  ASSERT_TRUE(matched.ok());
  EXPECT_FALSE(matched.value());
  auto missing = record("3");
  missing.attributes.erase(missing.attributes.begin());
  for (const auto& expression : {ordering, negated}) {
    matched = evaluate_dataset_attribute_predicate(missing, expression);
    ASSERT_TRUE(matched.ok());
    EXPECT_FALSE(matched.value());
  }
  matched = evaluate_dataset_attribute_predicate(missing,
      make_logical(LogicalOperator::Or,
          {ordering, make_is_null({"hdf5.attr:standard_name"})}));
  ASSERT_TRUE(matched.ok());
  EXPECT_TRUE(matched.value());

  auto cat = make_in_memory_published_version_catalog({manifest(7)});
  DatasetCatalogRequest request;
  request.predicate = ordering;
  auto discovered = discover_datasets_reference(*cat, request,
      [&](const auto&) {
        auto a = artifact();
        a.records = {record("1", "sst"), record("2", "zz"), missing};
        return QueryResult<DatasetCatalogArtifact>::success(std::move(a));
      }, [](const auto&, const CatalogSelection&) { return QueryStatus::success(); });
  ASSERT_TRUE(discovered.ok()) << discovered.status().message();
  EXPECT_EQ(discovered.value().candidate_datasets, 3u);
  ASSERT_EQ(discovered.value().datasets.size(), 1u);
  EXPECT_EQ(discovered.value().datasets[0].object_id, "1");
}
TEST(DatasetCatalog, FallbackDoesNotHideUnavailableOrMistypedAttributes) {
  const auto ordering = make_comparison({"hdf5.attr:standard_name"},
      ComparisonOperator::Less, LogicalValue::utf8("z"));
  const auto true_branch = make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
      LogicalValue::uint64(std::numeric_limits<std::uint64_t>::max()));
  const auto false_branch = make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal,
      LogicalValue::uint64(4));
  for (const auto state : {DatasetAttributeState::Unsupported,
                          DatasetAttributeState::TooLarge,
                          DatasetAttributeState::Invalid}) {
    auto unavailable = record();
    unavailable.attributes[0].state = state;
    unavailable.attributes[0].value.reset();
    for (const auto& expression : {
             ordering, make_logical(LogicalOperator::Or, {true_branch, ordering}),
             make_logical(LogicalOperator::And, {false_branch, ordering})}) {
      auto sql = compile_dataset_predicate_sql(expression);
      ASSERT_TRUE(sql.ok());
      EXPECT_TRUE(sql.value().attributes.empty());
      auto matched = evaluate_dataset_attribute_predicate(unavailable, expression);
      ASSERT_FALSE(matched.ok());
      EXPECT_EQ(matched.status().code(), QueryError::UnsupportedCapability);
    }
  }
  auto incomplete = record();
  incomplete.attributes.erase(incomplete.attributes.begin());
  incomplete.attributes_complete = false;
  auto matched = evaluate_dataset_attribute_predicate(incomplete, ordering);
  ASSERT_FALSE(matched.ok());
  EXPECT_EQ(matched.status().code(), QueryError::LayoutUnavailable);
  auto mistyped = record();
  mistyped.attributes[0].value = std::int64_t(42);
  matched = evaluate_dataset_attribute_predicate(mistyped, ordering);
  ASSERT_FALSE(matched.ok());
  EXPECT_EQ(matched.status().code(), QueryError::TypeMismatch);
}
TEST(DatasetCatalog, AuthorizesBeforeLoadingAnyAttribute) {
  auto cat = make_in_memory_published_version_catalog({manifest(1)});
  int loads = 0;
  DatasetCatalogRequest r;
  r.predicate = eq();
  auto out = discover_datasets_reference(
      *cat, r,
      [&](const auto &) {
        ++loads;
        return QueryResult<DatasetCatalogArtifact>::success(artifact());
      },
      [](const auto &, const CatalogSelection&) {
        return QueryStatus::failure(QueryError::PermissionDenied,
                                    QueryPhase::Catalog, "no");
      });
  EXPECT_FALSE(out.ok());
  EXPECT_EQ(loads, 0);
}
TEST(DatasetCatalog, SourceCatalogFailurePrecedesAuthorizationAndArtifactErrors) {
  auto cat = make_refreshing_published_version_catalog([](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::failure(QueryStatus::failure(
      QueryError::ReadFailure, QueryPhase::Catalog, "source catalog unavailable"));
  });
  unsigned authorizations = 0;
  unsigned artifact_reads = 0;
  auto result = discover_datasets_reference(*cat, {},
    [&](const auto&) {
      ++artifact_reads;
      return QueryResult<DatasetCatalogArtifact>::failure(QueryStatus::failure(
        QueryError::CorruptLayout, QueryPhase::Catalog, "bad catalog artifact"));
    }, [&](const auto&, const CatalogSelection&) {
      ++authorizations;
      return QueryStatus::failure(QueryError::PermissionDenied,
        QueryPhase::Catalog, "denied");
    });
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ReadFailure);
  EXPECT_EQ(result.status().message(), "source catalog unavailable");
  EXPECT_EQ(authorizations, 0u);
  EXPECT_EQ(artifact_reads, 0u);
}
TEST(DatasetCatalog, DiscoveryPassesFilteredPermissionEvidenceBeforeTimePruning) {
  auto first = manifest(7);
  first.key.path = "/data/a.h5";
  auto second = first;
  second.key.path = "/data/b.h5";
  const auto set_time = [](StructuredBlobManifest& m, std::int64_t point) {
    m.metadata = {
      {"__iotdbfs.time_coverage.protocol", MetadataValue("utc-point/1")},
      {"__iotdbfs.time_coverage.dataset_path", MetadataValue("/T")},
      {"__iotdbfs.time_coverage.utc_second", MetadataValue(point)},
      {"__iotdbfs.time_coverage.layout_digest", MetadataValue(std::string(64, 'a'))},
      {"__iotdbfs.dataset_catalog.layout_digest", MetadataValue(std::string(64, 'a'))}};
  };
  set_time(first, 10);
  set_time(second, 20);
  auto older = first;
  older.key.version = 6;
  auto unrelated = first;
  unrelated.key.path = "/other/c.h5";
  auto hints = std::make_shared<SourcePermissionSnapshot>();
  hints->storage_namespace = 99;
  hints->metadata_generation = 17;
  hints->observed_at = std::chrono::steady_clock::now();
  for (const auto& m : {first, second, older, unrelated})
    hints->sources.emplace(m.key, CatalogSourcePermission{
      "hdf5", m.revision, m.payload_size, m.layout_size, 0640, 1001, 1002, false});
  hints->parents.emplace("/", CatalogParentPermission{3, 0755, 0, 0, true});
  hints->parents.emplace("/data", CatalogParentPermission{4, 0750, 1001, 1002, true});
  hints->parents.emplace("/other", CatalogParentPermission{5, 0700, 0, 0, true});
  auto cat = make_refreshing_published_version_catalog(
    [&](const SourceSelector& selector) {
      EXPECT_EQ(selector.formats.adapter_ids, std::vector<std::string>{"hdf5"});
      return QueryResult<PublishedVersionSnapshot>::success(
        {{first, second, older, unrelated}, hints});
    }, 99);
  DatasetCatalogRequest request;
  request.sources.paths = {{PathScopeKind::Exact, first.key.path},
                           {PathScopeKind::Exact, second.key.path}};
  request.dataset_path = "/T";
  request.time_range = DatasetCatalogRequest::UtcTimeRange{10, 11};
  bool authorized = false;
  std::vector<std::string> loaded;
  auto result = discover_datasets_reference(*cat, request,
    [&](const StructuredBlobManifest& m) {
      EXPECT_TRUE(authorized);
      loaded.push_back(m.key.path);
      auto a = artifact();
      a.records[0].canonical_path = "/T";
      return QueryResult<DatasetCatalogArtifact>::success(std::move(a));
    }, [&](const std::vector<QuerySourceReference>& refs, const CatalogSelection& selection) {
      authorized = true;
      EXPECT_EQ(refs.size(), 2u); // Includes the time-miss source.
      EXPECT_EQ(selection.versions().size(), 2u);
      EXPECT_EQ(selection.storage_namespace, 99u);
      if (!selection.source_permissions) {
        ADD_FAILURE() << "discovery lost selected permission evidence";
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "no hints");
      }
      const auto& evidence = *selection.source_permissions;
      EXPECT_EQ(evidence.metadata_generation, 17u);
      EXPECT_EQ(evidence.observed_at, hints->observed_at);
      EXPECT_EQ(evidence.sources.size(), 2u);
      EXPECT_EQ(evidence.sources.count(older.key), 0u);
      EXPECT_EQ(evidence.sources.count(unrelated.key), 0u);
      EXPECT_EQ(evidence.parents.size(), 2u);
      EXPECT_EQ(evidence.parents.count("/other"), 0u);
      return QueryStatus::success();
    });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().selected_sources, 2u);
  EXPECT_EQ(result.value().time_candidate_sources, 1u);
  EXPECT_EQ(loaded, std::vector<std::string>{first.key.path});
}
TEST(DatasetCatalog, DiscoveryNeverForwardsForeignOrMismatchedSourceFacts) {
  const auto m = manifest(7);
  for (bool foreign_namespace : {false, true}) {
    auto hints = std::make_shared<SourcePermissionSnapshot>();
    hints->storage_namespace = foreign_namespace ? 100 : 99;
    hints->sources.emplace(m.key, CatalogSourcePermission{
      "hdf5", m.revision + 1, m.payload_size, m.layout_size, 0644, 1001, 1002, false});
    auto cat = make_refreshing_published_version_catalog(
      [&](const SourceSelector&) {
        return QueryResult<PublishedVersionSnapshot>::success({{m}, hints});
      }, 99);
    unsigned loads = 0;
    auto result = discover_datasets_reference(*cat, {},
      [&](const auto&) {
        ++loads;
        return QueryResult<DatasetCatalogArtifact>::success(artifact());
      }, [&](const auto& refs, const CatalogSelection& selection) {
        EXPECT_EQ(refs.size(), 1u);
        EXPECT_EQ(selection.storage_namespace, 99u);
        if (foreign_namespace) EXPECT_FALSE(selection.source_permissions);
        else {
          EXPECT_TRUE(selection.source_permissions);
          if (selection.source_permissions)
            EXPECT_TRUE(selection.source_permissions->sources.empty());
        }
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "fresh authorization required");
      });
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::PermissionDenied);
    EXPECT_EQ(loads, 0u);
  }
}
TEST(DatasetCatalog, RejectsUnauthorizedTimeMissBeforeCoverageValidation) {
  auto cat = make_in_memory_published_version_catalog({manifest(7)});
  DatasetCatalogRequest request;
  request.dataset_path = "/T";
  request.time_range = DatasetCatalogRequest::UtcTimeRange{10, 11};
  unsigned loads = 0;
  auto result = discover_datasets_reference(*cat, request,
    [&](const auto&) {
      ++loads;
      return QueryResult<DatasetCatalogArtifact>::success(artifact());
    }, [](const auto& refs, const CatalogSelection& selection) {
      EXPECT_EQ(refs.size(), 1u);
      EXPECT_EQ(selection.versions().size(), 1u);
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "denied");
    });
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(loads, 0u);
}
TEST(DatasetCatalog, LatestSelectionPrecedesPredicate) {
  auto cat =
      make_in_memory_published_version_catalog({manifest(1), manifest(2)});
  DatasetCatalogRequest r;
  r.predicate = eq();
  std::vector<int> loads;
  auto out = discover_datasets_reference(
      *cat, r,
      [&](const auto &m) {
        loads.push_back(m.key.version);
        auto a = artifact();
        if (m.key.version == 2)
          a.records[0].attributes[0].value = std::string("other");
        return QueryResult<DatasetCatalogArtifact>::success(std::move(a));
      },
      [](const auto &, const CatalogSelection&) { return QueryStatus::success(); });
  ASSERT_TRUE(out.ok()) << out.status().message();
  EXPECT_TRUE(out.value().datasets.empty());
  EXPECT_EQ(loads, std::vector<int>{2});
}
TEST(DatasetCatalog, MissingIndexIsExplicitFailure) {
  auto cat = make_in_memory_published_version_catalog({manifest(1)});
  DatasetCatalogRequest r;
  auto out = discover_datasets_reference(
      *cat, r,
      [](const auto &) {
        return QueryResult<DatasetCatalogArtifact>::failure(
            QueryStatus::failure(QueryError::LayoutUnavailable,
                                 QueryPhase::Catalog, "reingest"));
      },
      [](const auto &, const CatalogSelection&) { return QueryStatus::success(); });
  EXPECT_FALSE(out.ok());
  EXPECT_EQ(out.status().code(), QueryError::LayoutUnavailable);
}
TEST(DatasetCatalog, ResultsAreBoundedWithoutSilentTruncation) {
  auto cat = make_in_memory_published_version_catalog({manifest(1)});
  DatasetCatalogRequest r;
  r.max_datasets = 1;
  auto out = discover_datasets_reference(
      *cat, r,
      [](const auto &) {
        auto a = artifact();
        a.records.push_back(record("2"));
        return QueryResult<DatasetCatalogArtifact>::success(a);
      },
      [](const auto &, const CatalogSelection&) { return QueryStatus::success(); });
  EXPECT_FALSE(out.ok());
  EXPECT_EQ(out.status().code(), QueryError::ResourceExhausted);
}
TEST(DatasetCatalog, DeepOrCyclicExpressionIsBounded) {
  auto x = std::make_shared<QueryExpression>(
      LogicalExpression{LogicalOperator::Not, {}});
  std::get<LogicalExpression>(x->node).operands.push_back(x);
  auto result = compile_dataset_predicate_sql(x);
  EXPECT_FALSE(result.ok());
  std::get<LogicalExpression>(x->node).operands.clear();
}

TEST(DatasetCompiledPredicate, MatchesReferenceAcrossTypedNullAndUnavailableRows) {
  auto base = record();
  base.attributes.push_back({"signed", DatasetAttributeState::Complete,
      DatasetAttributeValue(std::int64_t(-7)), {}, {}});
  base.attributes.push_back({"real", DatasetAttributeState::Complete,
      DatasetAttributeValue(2.5), {}, {}});
  base.attributes.push_back({"flag", DatasetAttributeState::Complete,
      DatasetAttributeValue(false), {}, {}});
  base.attributes.push_back({"long name", DatasetAttributeState::Complete,
      DatasetAttributeValue(std::string("a'b")), {}, {}});
  std::vector<QueryExpressionPtr> expressions{nullptr, eq(), eq("other"),
      make_is_null({"hdf5.attr:absent"}), make_is_null({"hdf5.attr:n"}, true),
      make_logical(LogicalOperator::Not, {make_comparison({"hdf5.attr:absent"},
          ComparisonOperator::Equal, LogicalValue::utf8("x"))}),
      make_comparison({"hdf5.attr:long%20name"}, ComparisonOperator::Equal, LogicalValue::utf8("a'b")),
      make_comparison({"hdf5.attr:flag"}, ComparisonOperator::NotEqual, LogicalValue::boolean(true)),
      make_in({"hdf5.attr:standard_name"}, {LogicalValue::utf8("sst"), LogicalValue::utf8("other")}),
      make_in({"hdf5.attr:absent"}, {LogicalValue::utf8("x")}, true)};
  for (auto op : {ComparisonOperator::Equal, ComparisonOperator::NotEqual,
          ComparisonOperator::Less, ComparisonOperator::LessEqual,
          ComparisonOperator::Greater, ComparisonOperator::GreaterEqual}) {
    expressions.push_back(make_comparison({"hdf5.attr:n"}, op,
        LogicalValue::uint64(std::numeric_limits<std::uint64_t>::max())));
    expressions.push_back(make_comparison({"hdf5.attr:signed"}, op, LogicalValue::int64(-7)));
    expressions.push_back(make_comparison({"hdf5.attr:real"}, op, LogicalValue::float64(2.5)));
    expressions.push_back(make_comparison({"hdf5.attr:standard_name"}, op, LogicalValue::utf8("t")));
  }
  expressions.push_back(make_logical(LogicalOperator::Or, {eq(),
      make_logical(LogicalOperator::And, {make_is_null({"hdf5.attr:absent"}),
          make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal, LogicalValue::uint64(0))})}));
  std::vector<DatasetCatalogRecord> rows{base};
  auto changed = base;
  changed.attributes[0].value = std::string("other");
  changed.attributes[1].value = std::uint64_t(0);
  rows.push_back(changed);
  changed = base; changed.attributes.erase(changed.attributes.begin()); rows.push_back(changed);
  changed = base; changed.attributes_complete = false; rows.push_back(changed);
  changed = base; changed.attributes[1].value = double(3); rows.push_back(changed);
  changed = base; changed.attributes[0].state = DatasetAttributeState::Unsupported;
  changed.attributes[0].value.reset(); rows.push_back(changed);
  changed = base; changed.attributes[1].state = DatasetAttributeState::TooLarge;
  changed.attributes[1].value.reset(); rows.push_back(changed);
  for (std::size_t i = 0; i < expressions.size(); ++i) {
    SCOPED_TRACE(std::to_string(i));
    auto compiled = DatasetAttributePredicate::compile(expressions[i]);
    ASSERT_TRUE(compiled.ok()) << compiled.status().message();
    for (std::size_t j = 0; j < rows.size(); ++j) {
      SCOPED_TRACE(std::to_string(j));
      auto expected = evaluate_dataset_attribute_predicate(rows[j], expressions[i]);
      auto actual = compiled.value().evaluate(rows[j]);
      ASSERT_EQ(actual.ok(), expected.ok());
      if (expected.ok()) EXPECT_EQ(actual.value(), expected.value());
      else {
        EXPECT_EQ(actual.status().code(), expected.status().code());
        EXPECT_EQ(actual.status().phase(), expected.status().phase());
        EXPECT_EQ(actual.status().detail().field_id, expected.status().detail().field_id);
        EXPECT_EQ(actual.status().message(), expected.status().message());
      }
    }
  }
}
TEST(DatasetCompiledPredicate, ValidatesEveryAttributeBeforeShortCircuitOrTypeChecks) {
  auto r = record();
  auto expression = make_logical(LogicalOperator::Or, {eq(),
      make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal, LogicalValue::uint64(0))});
  auto compiled = DatasetAttributePredicate::compile(expression);
  ASSERT_TRUE(compiled.ok());
  r.attributes[1].state = DatasetAttributeState::Invalid;
  r.attributes[1].value.reset();
  EXPECT_EQ(compiled.value().evaluate(r).status().code(), QueryError::UnsupportedCapability);
  r = record();
  r.attributes[1].value = double(0);
  r.attributes[0].state = DatasetAttributeState::Unsupported;
  r.attributes[0].value.reset();
  EXPECT_EQ(compiled.value().evaluate(r).status().code(), QueryError::UnsupportedCapability);
}
TEST(DatasetCompiledPredicate, ReusedPlanDoesNotRetainRowValuesOrNullOnlyTypes) {
  auto compiled = DatasetAttributePredicate::compile(eq());
  ASSERT_TRUE(compiled.ok());
  auto r = record();
  EXPECT_TRUE(compiled.value().evaluate(r).value());
  r.attributes[0].value = std::string("other");
  EXPECT_FALSE(compiled.value().evaluate(r).value());
  r = record();
  EXPECT_TRUE(compiled.value().evaluate(r).value());
  auto null_only = DatasetAttributePredicate::compile(make_is_null({"hdf5.attr:standard_name"}));
  ASSERT_TRUE(null_only.ok());
  r.attributes[0].value = std::int64_t(7);
  EXPECT_FALSE(null_only.value().evaluate(r).value());
  r.attributes.clear();
  EXPECT_TRUE(null_only.value().evaluate(r).value());
}
TEST(DatasetCompiledPredicate, CompilerRetainsGrammarAndLiteralValidation) {
  std::vector<QueryExpressionPtr> invalid{
      make_comparison({"wrong:x"}, ComparisonOperator::Equal, LogicalValue::utf8("x")),
      make_comparison({"hdf5.attr:%61"}, ComparisonOperator::Equal, LogicalValue::utf8("x")),
      make_comparison({"hdf5.attr:x"}, ComparisonOperator::Equal, LogicalValue::null(LogicalTypeId::Utf8)),
      make_comparison({"hdf5.attr:x"}, ComparisonOperator::Less, LogicalValue::boolean(true)),
      make_comparison({"hdf5.attr:x"}, ComparisonOperator::Equal, LogicalValue::float64(std::numeric_limits<double>::infinity())),
      make_in({"hdf5.attr:x"}, {}),
      make_in({"hdf5.attr:x"}, {LogicalValue::int64(1), LogicalValue::utf8("x")}),
      make_in({"hdf5.attr:x"}, {LogicalValue::utf8("x"), LogicalValue::null(LogicalTypeId::Utf8)}),
      make_logical(LogicalOperator::And, {eq()}),
      make_logical(LogicalOperator::Not, {eq(), eq()}),
      make_logical(LogicalOperator::And, {eq(), nullptr})};
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    SCOPED_TRACE(std::to_string(i));
    auto expected = build_dataset_attribute_schema({}, invalid[i]);
    auto actual = DatasetAttributePredicate::compile(invalid[i]);
    ASSERT_FALSE(expected.ok());
    ASSERT_FALSE(actual.ok());
    EXPECT_EQ(actual.status().code(), expected.status().code());
    EXPECT_EQ(actual.status().phase(), expected.status().phase());
  }
}
TEST(DatasetCompiledPredicate, PreservesExpressionTypeErrorOrderAndComplexityBounds) {
  auto r = record();
  r.attributes[0].value = double(1);
  r.attributes[1].value = std::int64_t(1);
  auto expression = make_logical(LogicalOperator::And, {eq(),
      make_comparison({"hdf5.attr:n"}, ComparisonOperator::Equal, LogicalValue::uint64(1))});
  auto compiled = DatasetAttributePredicate::compile(expression);
  ASSERT_TRUE(compiled.ok());
  auto expected = evaluate_dataset_attribute_predicate(r, expression);
  auto actual = compiled.value().evaluate(r);
  ASSERT_FALSE(actual.ok());
  EXPECT_EQ(actual.status().detail().field_id, expected.status().detail().field_id);
  auto nested = eq();
  for (unsigned i = 0; i < 34; ++i) nested = make_logical(LogicalOperator::Not, {nested});
  EXPECT_EQ(DatasetAttributePredicate::compile(nested).status().code(), QueryError::ResourceExhausted);
  std::vector<QueryExpressionPtr> fields;
  for (unsigned i = 0; i < 33; ++i)
    fields.push_back(make_is_null({"hdf5.attr:f" + std::to_string(i)}));
  EXPECT_EQ(DatasetAttributePredicate::compile(make_logical(LogicalOperator::Or, fields))
      .status().code(), QueryError::ResourceExhausted);
}
