#include <gtest/gtest.h>
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include "iotdbfs/IoTDBDatasetCatalogCache.h"

using namespace query_utils;

TEST(PublishedCatalogSql, PushesExactPathVersionAndFormatWithEscapedLiterals) {
  iotdbfs::SourceSelector selector;
  selector.paths = {{iotdbfs::PathScopeKind::Exact, "weather/O'Reilly.nc4"}};
  selector.versions = iotdbfs::VersionSelection::exact_version(9);
  selector.formats = {iotdbfs::AdapterSelectionKind::Listed, {"hdf'5"}, false};
  auto sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok()) << sql.status().message();
  EXPECT_NE(sql.value().find("AND (path='/weather/O''Reilly.nc4')"), std::string::npos);
  EXPECT_NE(sql.value().find("AND time=9"), std::string::npos);
  EXPECT_NE(sql.value().find("AND (format_id='hdf''5')"), std::string::npos);
}

TEST(PublishedCatalogSql, EscapesSubtreeWildcardsAndKeepsSiblingBoundary) {
  iotdbfs::SourceSelector selector;
  selector.paths = {{iotdbfs::PathScopeKind::Subtree, "/a%_!'"},
                    {iotdbfs::PathScopeKind::Exact, "/b"}};
  selector.versions = iotdbfs::VersionSelection::between(3, 7);
  auto sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok()) << sql.status().message();
  EXPECT_NE(sql.value().find(
    "AND ((path='/a%_!''' OR path LIKE '/a!%!_!!''/%' ESCAPE '!') OR path='/b')"),
    std::string::npos);
  EXPECT_NE(sql.value().find("AND time>=3 AND time<7"), std::string::npos);
}

TEST(PublishedCatalogSql, RootSubtreeAndAllVersionsLeaveNamespaceUnfiltered) {
  iotdbfs::SourceSelector selector;
  selector.paths = {{iotdbfs::PathScopeKind::Subtree, "/"}};
  selector.versions = iotdbfs::VersionSelection::all();
  auto sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok());
  EXPECT_EQ(sql.value(),
    "SELECT time,path,format_id,metadata,size,layout_size,blob_phase,"
    "published,blob_revision,mode,uid,gid,is_dir FROM inode WHERE published=true AND "
    "is_dir=false ORDER BY time DESC");
}

TEST(PublishedCatalogSql, RepresentsNullAndEmptyPassThroughFormat) {
  iotdbfs::SourceSelector selector;
  selector.formats = {iotdbfs::AdapterSelectionKind::Listed, {}, true};
  auto sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok());
  EXPECT_NE(sql.value().find("AND (format_id IS NULL OR format_id='')"),
    std::string::npos);
  selector.formats.include_byte_passthrough = false;
  sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok());
  EXPECT_NE(sql.value().find("AND (false)"), std::string::npos);
  selector.formats = {iotdbfs::AdapterSelectionKind::All, {}, false};
  sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok());
  EXPECT_NE(sql.value().find("AND (format_id IS NOT NULL AND format_id<>'')"),
    std::string::npos);
}

TEST(PublishedCatalogSql, LatestAndFilePredicateStayInFinalEvaluator) {
  iotdbfs::SourceSelector selector;
  selector.versions = iotdbfs::VersionSelection::latest();
  selector.file_predicate = iotdbfs::make_comparison({"sigma.station"},
    iotdbfs::ComparisonOperator::Equal, iotdbfs::LogicalValue::utf8("secret"));
  const auto sql = iotdbfs::compile_published_catalog_sql(selector);
  ASSERT_TRUE(sql.ok());
  EXPECT_EQ(sql.value(), iotdbfs::compile_published_catalog_sql(
    iotdbfs::SourceSelector{}).value());
}

TEST(PublishedCatalogSql, RejectsInvalidSelectorsBeforeGeneratingSql) {
  iotdbfs::SourceSelector selector;
  selector.versions = iotdbfs::VersionSelection::exact_version(-1);
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector).ok());
  selector.versions = iotdbfs::VersionSelection::all();
  selector.paths = {{iotdbfs::PathScopeKind::Exact, std::string("/a\0b", 4)}};
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector).ok());
  selector.paths = {{static_cast<iotdbfs::PathScopeKind>(99), "/a"}};
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector).ok());
  selector.paths = {};
  selector.formats = {iotdbfs::AdapterSelectionKind::Listed,
    {std::string("h\0f", 3)}, false};
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector).ok());
}

TEST(PublishedCatalogSql, KeepsSourceFiltersSeparateFromAncestorBranch) {
  iotdbfs::SourceSelector selector;
  selector.paths = {{iotdbfs::PathScopeKind::Exact, "/science/a.nc4"}};
  selector.versions = iotdbfs::VersionSelection::exact_version(9);
  selector.formats = {iotdbfs::AdapterSelectionKind::Listed, {"hdf5"}, false};
  auto sql = iotdbfs::compile_published_catalog_sql(selector, {"/science", "/"});
  ASSERT_TRUE(sql.ok());
  EXPECT_NE(sql.value().find(
    "WHERE published=true AND path IN ('/','/science','/science/a.nc4') "
    "AND ((is_dir=false AND (path='/science/a.nc4') "
    "AND time=9 AND (format_id='hdf5')) OR path IN ('/','/science'))"),
    std::string::npos);
  EXPECT_EQ(sql.value().find("is_dir=true"), std::string::npos);
  std::vector<std::string> too_many;
  for (unsigned i = 0; i < 129; ++i) too_many.push_back("/" + std::to_string(i));
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector, too_many).ok());
}

TEST(PublishedCatalogSql, CommonCandidatePathFilterRequiresFiniteExactSourceScopes) {
  iotdbfs::SourceSelector selector;
  selector.paths = {{iotdbfs::PathScopeKind::Exact, "science/O'Reilly.nc4"},
                    {iotdbfs::PathScopeKind::Exact, "/science/b.nc4"}};
  auto exact = iotdbfs::compile_published_catalog_sql(selector,
    {"/science", "/", "/science"});
  ASSERT_TRUE(exact.ok());
  EXPECT_NE(exact.value().find(
    "WHERE published=true AND path IN ('/','/science','/science/O''Reilly.nc4','/science/b.nc4') AND (("),
    std::string::npos);
  selector.paths = {{iotdbfs::PathScopeKind::Subtree, "/science"}};
  auto subtree = iotdbfs::compile_published_catalog_sql(selector, {"/"});
  ASSERT_TRUE(subtree.ok());
  EXPECT_EQ(subtree.value().find("WHERE published=true AND path IN"), std::string::npos);
  selector.paths = {};
  auto all = iotdbfs::compile_published_catalog_sql(selector, {"/"});
  ASSERT_TRUE(all.ok());
  EXPECT_EQ(all.value().find("WHERE published=true AND path IN"), std::string::npos);
  selector.paths = {{iotdbfs::PathScopeKind::Exact, "/selected"}};
  std::vector<std::string> parents;
  for (unsigned i = 0; i < 128; ++i) parents.push_back("/parent" + std::to_string(i));
  EXPECT_FALSE(iotdbfs::compile_published_catalog_sql(selector, parents).ok());
}

TEST(InodePermissionSql, UsesBoundedDeduplicatedLatestPathQueriesWithoutDirectoryFilter) {
  auto small = iotdbfs::compile_inode_permission_sql_batches({"/", "/a'_%!", "/"});
  ASSERT_TRUE(small.ok());
  ASSERT_EQ(small.value().size(), 1u);
  EXPECT_EQ(small.value()[0],
    "SELECT time,path,mode,uid,gid,is_dir FROM inode WHERE published=true "
    "AND path IN ('/','/a''_%!') ORDER BY time DESC,mtime DESC");
  std::vector<std::string> many;
  for (unsigned i = 0; i < 129; ++i) many.push_back("/" + std::to_string(i));
  auto split = iotdbfs::compile_inode_permission_sql_batches(many);
  ASSERT_TRUE(split.ok());
  EXPECT_EQ(split.value().size(), 2u);
  for (const auto& sql : split.value()) {
    EXPECT_LE(sql.size(), iotdbfs::kMaxInodePermissionSqlBytes);
    EXPECT_EQ(sql.find("is_dir=true"), std::string::npos);
    EXPECT_EQ(sql.find("is_dir=false"), std::string::npos);
  }
  auto empty = iotdbfs::compile_inode_permission_sql_batches({});
  ASSERT_TRUE(empty.ok()); EXPECT_TRUE(empty.value().empty());
  EXPECT_FALSE(iotdbfs::compile_inode_permission_sql_batches({""}).ok());
  EXPECT_FALSE(iotdbfs::compile_inode_permission_sql_batches({std::string("/a\0b", 4)}).ok());
}

TEST(InodePermissionSql, SplitsBySqlBytesAsWellAsPathCount) {
  std::vector<std::string> paths;
  for (unsigned i = 0; i < 30; ++i)
    paths.push_back("/" + std::to_string(i) + std::string(4000, '\''));
  auto sql = iotdbfs::compile_inode_permission_sql_batches(paths);
  ASSERT_TRUE(sql.ok());
  EXPECT_GT(sql.value().size(), 1u);
  for (const auto& statement : sql.value())
    EXPECT_LE(statement.size(), iotdbfs::kMaxInodePermissionSqlBytes);
}

TEST(InodePermissionSql, MissingInvalidOrNonDirectoryNewestNeverUsesOlderDirectory) {
  iotdbfs::SourcePermissionSnapshot snapshot;
  snapshot.parents.emplace("/missing", std::nullopt);
  snapshot.parents.emplace("/invalid", std::nullopt);
  snapshot.parents.emplace("/replaced", std::nullopt);
  std::set<std::string> seen;
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/invalid", 3, std::nullopt, 1, 1, true));
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/invalid", 2, 0777, 1, 1, true));
  EXPECT_FALSE(snapshot.parents.at("/invalid"));
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/replaced", 3, 0644, 1, 1, false));
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/replaced", 2, 0777, 1, 1, true));
  ASSERT_TRUE(snapshot.parents.at("/replaced"));
  EXPECT_FALSE(snapshot.parents.at("/replaced")->is_dir);
  EXPECT_EQ(snapshot.parents.at("/replaced")->version, 3);
  EXPECT_FALSE(snapshot.parents.at("/missing"));
  EXPECT_FALSE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/unexpected", 4, 0777, 1, 1, true));
}

TEST(InodePermissionSql, OlderInvalidVersionDoesNotInvalidateNewestAndNewestInvalidStaysMissing) {
  iotdbfs::SourcePermissionSnapshot snapshot;
  snapshot.parents.emplace("/valid", std::nullopt);
  snapshot.parents.emplace("/invalid", std::nullopt);
  std::set<std::string> seen;
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/valid", 3, 0755, 1, 1, true));
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/valid", -1, 0777, 1, 1, true));
  ASSERT_TRUE(snapshot.parents.at("/valid"));
  EXPECT_EQ(snapshot.parents.at("/valid")->version, 3);
  EXPECT_EQ(snapshot.parents.at("/valid")->mode, 0755);
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/invalid", -1, 0777, 1, 1, true));
  EXPECT_TRUE(iotdbfs::observe_latest_parent_permission(snapshot, seen,
    "/invalid", 2, 0777, 1, 1, true));
  EXPECT_FALSE(snapshot.parents.at("/invalid"));
}

namespace {
iotdbfs::StructuredBlobManifest revalidation_manifest(std::string path, std::int64_t version) {
  iotdbfs::StructuredBlobManifest m;
  m.key = {std::move(path), version};
  m.format_id = "hdf5";
  m.payload_size = 42; m.layout_size = 12; m.revision = 8;
  m.phase = iotdbfs::StructuredBlobPhase::Published; m.published = true;
  return m;
}
}

TEST(ManifestRevalidationSql, NormalizesDeduplicatesAndKeepsExactVersionPairs) {
  auto batches = iotdbfs::compile_manifest_revalidation_sql_batches(
    {{"weather/O'Reilly", 9}, {"/weather/b", 10}, {"/weather/O'Reilly", 9},
     {"/weather/O'Reilly", 11}});
  ASSERT_TRUE(batches.ok()) << batches.status().message();
  ASSERT_EQ(batches.value().size(), 1u);
  const auto& batch = batches.value().front();
  ASSERT_EQ(batch.keys.size(), 3u);
  EXPECT_NE(batch.sql.find("path IN ('/weather/O''Reilly','/weather/b') AND ("), std::string::npos);
  EXPECT_NE(batch.sql.find("(path='/weather/O''Reilly' AND time=9)"), std::string::npos);
  EXPECT_NE(batch.sql.find("(path='/weather/O''Reilly' AND time=11)"), std::string::npos);
  EXPECT_NE(batch.sql.find("(path='/weather/b' AND time=10)"), std::string::npos);
  EXPECT_EQ(batch.sql.find("time IN"), std::string::npos);
  EXPECT_EQ(batch.sql.find("published=true"), std::string::npos);
  EXPECT_EQ(batch.sql.find("metadata"), std::string::npos);
}

TEST(ManifestRevalidationSql, Splits128Versus129AndEmptyIssuesNoQuery) {
  std::vector<iotdbfs::StructuredBlobVersionKey> keys;
  for (unsigned i = 0; i < 128; ++i) keys.push_back({"/source" + std::to_string(i), i});
  auto one = iotdbfs::compile_manifest_revalidation_sql_batches(keys);
  ASSERT_TRUE(one.ok()); ASSERT_EQ(one.value().size(), 1u);
  EXPECT_EQ(one.value()[0].keys.size(), 128u);
  keys.push_back({"/source128", 128});
  auto two = iotdbfs::compile_manifest_revalidation_sql_batches(keys);
  ASSERT_TRUE(two.ok()); ASSERT_EQ(two.value().size(), 2u);
  EXPECT_EQ(two.value()[0].keys.size(), 128u);
  EXPECT_EQ(two.value()[1].keys.size(), 1u);
  auto empty = iotdbfs::compile_manifest_revalidation_sql_batches({});
  ASSERT_TRUE(empty.ok()); EXPECT_TRUE(empty.value().empty());
  EXPECT_TRUE(iotdbfs::validate_manifest_revalidation_rows({}, {}).ok());
}

TEST(ManifestRevalidationSql, SplitsEscapedLongPathsBySqlBytes) {
  std::vector<iotdbfs::StructuredBlobVersionKey> keys;
  for (unsigned i = 0; i < 30; ++i)
    keys.push_back({"/" + std::to_string(i) + std::string(4000, '\''), i});
  auto batches = iotdbfs::compile_manifest_revalidation_sql_batches(keys);
  ASSERT_TRUE(batches.ok()); EXPECT_GT(batches.value().size(), 1u);
  std::size_t count = 0;
  for (const auto& batch : batches.value()) {
    EXPECT_LE(batch.keys.size(), 128u);
    EXPECT_LE(batch.sql.size(), 64u * 1024);
    count += batch.keys.size();
  }
  EXPECT_EQ(count, keys.size());
}

TEST(ManifestRevalidationSql, RejectsInvalidKeysAndExcessInputBeforeDeduplication) {
  for (const auto& key : std::vector<iotdbfs::StructuredBlobVersionKey>{
      {"", 0}, {std::string("/a\0b", 4), 0}, {std::string(4097, 'a'), 0}, {std::string(4096, 'a'), 0}, {"/a", -1}}) {
    auto invalid = iotdbfs::compile_manifest_revalidation_sql_batches({key});
    ASSERT_FALSE(invalid.ok());
    EXPECT_EQ(invalid.status().code(), iotdbfs::QueryError::InvalidQuery);
  }
  std::vector<iotdbfs::StructuredBlobVersionKey> keys(65536, {"/a", 0});
  EXPECT_TRUE(iotdbfs::compile_manifest_revalidation_sql_batches(keys).ok());
  keys.push_back({"/a", 0});
  auto invalid = iotdbfs::compile_manifest_revalidation_sql_batches(keys);
  ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), iotdbfs::QueryError::ResourceExhausted);
}

TEST(ManifestRevalidationRows, RequiresAllExactKeysAndRejectsCrossPairsDuplicatesAndLatest) {
  const auto a = revalidation_manifest("/a", 1);
  const auto b = revalidation_manifest("/b", 2);
  EXPECT_TRUE(iotdbfs::validate_manifest_revalidation_rows({a, b}, {b, a}).ok());
  auto wrong = a; wrong.key.version = 2;
  for (const auto& rows : std::vector<std::vector<iotdbfs::StructuredBlobManifest>>{
      {}, {a}, {a, a, b}, {wrong, b}, {a, b, revalidation_manifest("/a", 3)}}) {
    auto stale = iotdbfs::validate_manifest_revalidation_rows({a, b}, rows);
    EXPECT_EQ(stale.code(), iotdbfs::QueryError::StalePlan);
  }
}

TEST(ManifestRevalidationRows, RejectsEveryMutableIdentityFieldButReusesSelectedMetadata) {
  const auto expected = revalidation_manifest("/a", 1);
  auto same = expected;
  same.metadata.emplace("ignored", iotdbfs::MetadataValue("not reread"));
  EXPECT_TRUE(iotdbfs::validate_manifest_revalidation_rows({expected}, {same}).ok());
  for (unsigned field = 0; field < 7; ++field) {
    auto changed = expected;
    switch (field) {
      case 0: changed.published = false; break;
      case 1: changed.phase = iotdbfs::StructuredBlobPhase::Described; break;
      case 2: ++changed.revision; break;
      case 3: changed.format_id = "arrow"; break;
      case 4: changed.format_id.reset(); break;
      case 5: ++changed.payload_size; break;
      case 6: ++changed.layout_size; break;
    }
    EXPECT_EQ(iotdbfs::validate_manifest_revalidation_rows({expected}, {changed}).code(),
      iotdbfs::QueryError::StalePlan) << "field " << field;
  }
}

TEST(DatasetCatalogIndexSql, BatchesExactKeysCountsAndFiltersWithEscapedAttributes) {
  using namespace iotdbfs;
  DatasetPredicateSql predicate{{"u'nits"}, "a0.svalue='K'"};
  auto compiled = compile_dataset_catalog_index_sql_batches(
    {{{"/a'", 1}, 2, 3}, {{"/b", 2}, 0, 0}}, predicate, std::string("/T'"));
  ASSERT_TRUE(compiled.ok()); ASSERT_EQ(compiled.value().size(), 1u);
  const auto& batch = compiled.value()[0];
  EXPECT_NE(batch.entry_count_sql.find("SELECT path,time,count(*) FROM blob_dataset_entry"), std::string::npos);
  EXPECT_NE(batch.attribute_count_sql.find("FROM blob_dataset_attribute"), std::string::npos);
  EXPECT_NE(batch.entry_count_sql.find("path IN ('/a''','/b') AND ((path='/a''' AND time=1) OR (path='/b' AND time=2)) GROUP BY path,time LIMIT 3"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("SELECT d.path,d.time,d.object_id"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("JOIN inode i ON d.path=i.path AND d.time=i.time"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("WHERE i.published=true AND"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("a0.attribute_name='u''nits'"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("d.path IN ('/a''','/b') AND ((d.path='/a''' AND d.time=1) OR (d.path='/b' AND d.time=2))"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("d.dataset_path='/T'''"), std::string::npos);
  EXPECT_NE(batch.filter_sql.find("LIMIT 3"), std::string::npos);
  EXPECT_EQ(batch.expected_records, 2u);
}

TEST(DatasetCatalogIndexSql, OwnerSelectedKeysReplaceOnlySqlInodeVisibility) {
  using namespace iotdbfs;
  const std::vector<DatasetCatalogSqlSource> sources{{{"/a'", 1}, 2, 3}, {{"/b", 2}, 1, 2}};
  const DatasetPredicateSql predicate{{"u'nits"}, "a0.svalue='K'"};
  auto legacy = compile_dataset_catalog_index_sql_batches(sources, predicate, std::string("/T'"));
  auto owner = compile_dataset_catalog_index_sql_batches(sources, predicate, std::string("/T'"),
    DatasetCatalogVisibility::OwnerSelectedExactKeys);
  ASSERT_TRUE(legacy.ok()); ASSERT_TRUE(owner.ok());
  ASSERT_EQ(legacy.value().size(), 1u); ASSERT_EQ(owner.value().size(), 1u);
  const auto& before = legacy.value().front();
  const auto& after = owner.value().front();
  EXPECT_EQ(after.entry_count_sql, before.entry_count_sql);
  EXPECT_EQ(after.attribute_count_sql, before.attribute_count_sql);
  EXPECT_EQ(after.expected_records, before.expected_records);
  EXPECT_EQ(after.filter_sql.find("JOIN inode"), std::string::npos);
  EXPECT_EQ(after.filter_sql.find("published"), std::string::npos);
  // Only the legacy publication join and predicate disappear. Exact paired
  // path/version constraints, attribute joins, dataset path, ordering and the
  // sentinel result limit remain byte-for-byte identical.
  auto expected = before.filter_sql;
  const std::string join = " JOIN inode i ON d.path=i.path AND d.time=i.time";
  const std::string gate = "i.published=true AND ";
  expected.erase(expected.find(join), join.size());
  expected.erase(expected.find(gate), gate.size());
  EXPECT_EQ(after.filter_sql, expected);
  EXPECT_NE(after.filter_sql.find("(d.path='/a''' AND d.time=1) OR (d.path='/b' AND d.time=2)"), std::string::npos);
}

TEST(DatasetCatalogIndexSql, Splits128Versus129AndSqlBytesAndHandlesEmpty) {
  using namespace iotdbfs;
  std::vector<DatasetCatalogSqlSource> sources;
  for (unsigned i = 0; i < 128; ++i) sources.push_back({{"/" + std::to_string(i), i}, 2, 4});
  auto one = compile_dataset_catalog_index_sql_batches(sources, {{}, "TRUE"});
  ASSERT_TRUE(one.ok()); EXPECT_EQ(one.value().size(), 1u);
  sources.push_back({{"/129", 129}, 3, 0});
  auto two = compile_dataset_catalog_index_sql_batches(sources, {{}, "TRUE"});
  ASSERT_TRUE(two.ok()); ASSERT_EQ(two.value().size(), 2u);
  EXPECT_EQ(two.value()[0].sources.size(), 128u); EXPECT_EQ(two.value()[1].sources.size(), 1u);
  sources.clear();
  for (unsigned i = 0; i < 20; ++i) sources.push_back({{"/" + std::to_string(i) + std::string(4000, '\''), i}, 1, 0});
  auto long_paths = compile_dataset_catalog_index_sql_batches(sources, {{}, "TRUE"});
  ASSERT_TRUE(long_paths.ok()); EXPECT_GT(long_paths.value().size(), 1u);
  for (const auto& b : long_paths.value()) {
    EXPECT_LE(b.entry_count_sql.size(), 65536u);
    EXPECT_LE(b.attribute_count_sql.size(), 65536u); EXPECT_LE(b.filter_sql.size(), 65536u);
  }
  EXPECT_TRUE(compile_dataset_catalog_index_sql_batches({}, {{}, "TRUE"}).value().empty());
  EXPECT_EQ(compile_dataset_catalog_index_sql_batches({{{"/a", 1}, 1, 0}},
    {{}, std::string(65536, 'x')}).status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(compile_dataset_catalog_index_sql_batches(
    std::vector<DatasetCatalogSqlSource>(65537, {{"/a", 1}, 1, 0}), {{}, "TRUE"}).status().code(),
    QueryError::ResourceExhausted);
}

TEST(DatasetCatalogIndexCounts, MissingGroupsAreZeroAndEveryOtherMismatchIsCorrupt) {
  using namespace iotdbfs;
  const std::vector<DatasetCatalogSqlSource> expected{{{"/a", 1}, 2, 0}, {{"/b", 2}, 0, 3}};
  EXPECT_TRUE(validate_dataset_catalog_index_counts(expected, {{{"/a", 1}, 2}}, false).ok());
  EXPECT_TRUE(validate_dataset_catalog_index_counts(expected, {{{"/b", 2}, 3}}, true).ok());
  for (const auto& rows : std::vector<std::vector<DatasetCatalogIndexCountRow>>{
      {}, {{{"/a", 1}, -1}}, {{{"/a", 1}, 1}}, {{{"/a", 2}, 2}},
      {{{"/a", 1}, 2}, {{"/a", 1}, 2}}, {{{"/a", 1}, 2}, {{"/unexpected", 1}, 0}}}) {
    EXPECT_EQ(validate_dataset_catalog_index_counts(expected, rows, false).code(), QueryError::CorruptLayout);
  }
  EXPECT_TRUE(validate_dataset_catalog_index_counts({{{"/empty", 3}, 0, 0}}, {}, false).ok());
}

TEST(DatasetCatalogFilterRows, RejectsCrossPairsDuplicatesAndUnknownObjects) {
  using namespace iotdbfs;
  const DatasetCatalogSqlObjects expected{{{"/a", 1}, {"d0", "d1"}}, {{"/b", 2}, {"d0"}}};
  EXPECT_TRUE(validate_dataset_catalog_filter_rows(expected,
    {{{"/b", 2}, "d0"}, {{"/a", 1}, "d0"}}).ok());
  EXPECT_TRUE(validate_dataset_catalog_filter_rows(expected, {}).ok());
  for (const auto& rows : std::vector<std::vector<DatasetCatalogSqlObjectRow>>{
      {{{"/a", 2}, "d0"}}, {{{"/a", 1}, "unknown"}},
      {{{"/a", 1}, "d0"}, {{"/a", 1}, "d0"}}})
    EXPECT_EQ(validate_dataset_catalog_filter_rows(expected, rows).code(), QueryError::CorruptLayout);
}

namespace {
iotdbfs::StructuredBlobManifest marker_manifest(std::string path, std::int64_t version) {
  auto m = revalidation_manifest(std::move(path), version);
  m.metadata = {{"__iotdbfs.dataset_catalog.protocol", iotdbfs::MetadataValue("iotdbfs-dataset-catalog/1")},
    {"__iotdbfs.dataset_catalog.layout_digest", iotdbfs::MetadataValue(std::string(64, 'a'))}};
  return m;
}
iotdbfs::DatasetCatalogMarker valid_marker(std::string path, std::int64_t version) {
  return {{std::move(path), version}, 512, "iotdbfs-dataset-catalog/1", std::string(64, 'a'),
    "catalog-fixture/1", true, 1, 1};
}
}

TEST(DatasetCatalogMarkerSql, BatchesExactKeysWithFullIdentityAndSentinelRow) {
  using namespace iotdbfs;
  auto small = compile_dataset_catalog_marker_sql_batches({{"a/O'Reilly", 1}, {"/b", 2}});
  ASSERT_TRUE(small.ok()); ASSERT_EQ(small.value().size(), 1u);
  const auto& sql = small.value()[0].sql;
  EXPECT_NE(sql.find("SELECT time,path,size,protocol,layout_digest,layout_protocol,directory_complete,record_count,attribute_count FROM blob_dataset_catalog"), std::string::npos);
  EXPECT_NE(sql.find("path IN ('/a/O''Reilly','/b') AND ((path='/a/O''Reilly' AND time=1) OR (path='/b' AND time=2)) LIMIT 3"), std::string::npos);
  EXPECT_EQ(sql.find("READ_OBJECT"), std::string::npos);
  EXPECT_EQ(sql.find("time IN"), std::string::npos);
  std::vector<StructuredBlobVersionKey> keys;
  for (unsigned j = 0; j < 129; ++j) keys.push_back({"/source" + std::to_string(j), j});
  auto split = compile_dataset_catalog_marker_sql_batches(keys);
  ASSERT_TRUE(split.ok()); ASSERT_EQ(split.value().size(), 2u);
  EXPECT_EQ(split.value()[0].keys.size(), 128u);
  keys.clear();
  for (unsigned j = 0; j < 20; ++j) keys.push_back({"/" + std::to_string(j) + std::string(4000, '\''), j});
  split = compile_dataset_catalog_marker_sql_batches(keys);
  ASSERT_TRUE(split.ok()); EXPECT_GT(split.value().size(), 1u);
  for (const auto& batch : split.value()) EXPECT_LE(batch.sql.size(), 65536u);
  EXPECT_TRUE(compile_dataset_catalog_marker_sql_batches({}).value().empty());
  EXPECT_EQ(compile_dataset_catalog_marker_sql_batches({{"/bad", -1}}).status().code(), QueryError::InvalidQuery);
}

TEST(DatasetCatalogVerificationSql, ExactPairsAndIndependentSentinels) {
  using namespace iotdbfs;
  auto compiled = compile_dataset_catalog_verification_sql_batches(
    {{"a/O'Reilly", 1}, {"/b", 2}, {"/b", 3}}, {{"/b", 3}});
  ASSERT_TRUE(compiled.ok()); ASSERT_EQ(compiled.value().size(), 1u);
  const auto& batch = compiled.value()[0];
  EXPECT_EQ(batch.keys.size(), 3u); EXPECT_EQ(batch.marker_keys.size(), 1u);
  const auto split = batch.sql.find(") UNION ALL (");
  ASSERT_NE(split, std::string::npos);
  const auto sources = batch.sql.substr(0, split), markers = batch.sql.substr(split);
  EXPECT_NE(sources.find("CAST(0 AS INT64) AS kind"), std::string::npos);
  EXPECT_NE(sources.find("path IN ('/a/O''Reilly','/b')"), std::string::npos);
  EXPECT_NE(sources.find("(path='/b' AND time=2) OR (path='/b' AND time=3)"), std::string::npos);
  EXPECT_NE(sources.find("LIMIT 4"), std::string::npos);
  EXPECT_NE(markers.find("CAST(1 AS INT64) AS kind"), std::string::npos);
  EXPECT_NE(markers.find("FROM blob_dataset_catalog WHERE path IN ('/b') AND ((path='/b' AND time=3)) LIMIT 2"), std::string::npos);
  EXPECT_EQ(markers.find("time=2"), std::string::npos);
  EXPECT_EQ(batch.sql.find("time IN"), std::string::npos);
}

TEST(DatasetCatalogVerificationSql, BoundedBatchesKeepMarkersWithTheirSources) {
  using namespace iotdbfs;
  std::vector<StructuredBlobVersionKey> keys;
  for (unsigned i = 0; i < 129; ++i) keys.push_back({"/source" + std::to_string(i), i});
  auto compiled = compile_dataset_catalog_verification_sql_batches(keys, keys);
  ASSERT_TRUE(compiled.ok()); ASSERT_EQ(compiled.value().size(), 2u);
  EXPECT_EQ(compiled.value()[0].keys.size(), 128u);
  EXPECT_EQ(compiled.value()[1].keys.size(), 1u);
  for (const auto& batch : compiled.value()) EXPECT_EQ(batch.keys, batch.marker_keys);
  keys.pop_back();
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches(keys, keys).value().size(), 1u);
  EXPECT_TRUE(compile_dataset_catalog_verification_sql_batches({}, {}).value().empty());
  auto misses = compile_dataset_catalog_verification_sql_batches(keys, {});
  ASSERT_TRUE(misses.ok()); ASSERT_EQ(misses.value().size(), 1u);
  EXPECT_TRUE(misses.value()[0].marker_keys.empty());
  EXPECT_EQ(misses.value()[0].sql.find("UNION"), std::string::npos);
  EXPECT_EQ(misses.value()[0].sql.find("blob_dataset_catalog"), std::string::npos);
}

TEST(DatasetCatalogVerificationSql, LongEscapedPathsSplitByCombinedSqlSize) {
  using namespace iotdbfs;
  std::vector<StructuredBlobVersionKey> keys;
  for (unsigned i = 0; i < 20; ++i) keys.push_back({"/" + std::to_string(i) + std::string(4000, '\''), i});
  auto compiled = compile_dataset_catalog_verification_sql_batches(keys, keys);
  ASSERT_TRUE(compiled.ok()); EXPECT_GT(compiled.value().size(), 1u);
  std::size_t count = 0;
  for (const auto& batch : compiled.value()) {
    EXPECT_LE(batch.sql.size(), kMaxManifestRevalidationSqlBytes);
    EXPECT_EQ(batch.keys, batch.marker_keys); count += batch.keys.size();
  }
  EXPECT_EQ(count, keys.size());
}

TEST(DatasetCatalogVerificationSql, ValidatesBothInputsAndMarkerSubsetBeforeDeduplication) {
  using namespace iotdbfs;
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches({{"/a", 1}}, {{"/b", 1}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches({}, {{"/a", 1}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches({{"/a", -1}}, {}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches({{"/a", 1}}, {{"/a", -1}}).status().code(), QueryError::InvalidQuery);
  const std::vector<StructuredBlobVersionKey> excessive(65537, {"/a", 1});
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches(excessive, {}).status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(compile_dataset_catalog_verification_sql_batches({{"/a", 1}}, excessive).status().code(), QueryError::ResourceExhausted);
  auto normalized = compile_dataset_catalog_verification_sql_batches({{"a/b", 1}, {"/a/b", 1}}, {{"/a/b", 1}});
  ASSERT_TRUE(normalized.ok()); EXPECT_EQ(normalized.value()[0].keys.size(), 1u);
}

TEST(DatasetCatalogMarkerRows, RequiresAllPreciseKeysAndValidPublishedManifestMarkers) {
  using namespace iotdbfs;
  const auto a = marker_manifest("/a", 1), b = marker_manifest("/b", 2);
  const auto row_a = valid_marker("/a", 1), row_b = valid_marker("/b", 2);
  EXPECT_TRUE(validate_dataset_catalog_marker_rows({a, b}, {row_b, row_a}).ok());
  EXPECT_TRUE(validate_dataset_catalog_marker_rows({}, {}).ok());
  EXPECT_EQ(validate_dataset_catalog_marker_rows({a, b}, {row_a}).code(), QueryError::LayoutUnavailable);
  for (const auto& rows : std::vector<std::vector<DatasetCatalogMarker>>{
      {row_a, row_a, row_b}, {valid_marker("/a", 2), row_b}, {row_a, row_b, valid_marker("/a", 3)}})
    EXPECT_EQ(validate_dataset_catalog_marker_rows({a, b}, rows).code(), QueryError::CorruptLayout);
  auto missing = a; missing.metadata.clear();
  EXPECT_EQ(validate_dataset_catalog_marker_rows({missing}, {row_a}).code(), QueryError::LayoutUnavailable);
}

TEST(DatasetCatalogMarkerRows, RejectsInvalidSizesCountsFlagsAndIdentity) {
  using namespace iotdbfs;
  const auto m = marker_manifest("/a", 1);
  for (unsigned field = 0; field < 12; ++field) {
    auto row = valid_marker("/a", 1);
    switch (field) {
      case 0: row.size = 0; break;
      case 1: row.size = -1; break;
      case 2: row.size = kMaxDatasetCatalogBytes + 1; break;
      case 3: row.record_count = -1; break;
      case 4: row.record_count = 65537; break;
      case 5: row.attribute_count = -1; break;
      case 6: row.attribute_count = 16385; break;
      case 7: row.protocol = "wrong"; break;
      case 8: row.layout_digest = std::string(64, 'b'); break;
      case 9: row.layout_digest = std::string(64, 'z'); break;
      case 10: row.layout_protocol.clear(); break;
      case 11: row.directory_complete = false; break;
    }
    EXPECT_EQ(validate_dataset_catalog_marker_rows({m}, {row}).code(), QueryError::CorruptLayout) << field;
  }
}

TEST(DatasetCatalogMarkerArtifact, ChecksFullCountsFormatAndSerializedSize) {
  using namespace iotdbfs;
  const auto m = marker_manifest("/a", 1);
  const auto row = valid_marker("/a", 1);
  DatasetCatalogArtifact artifact;
  artifact.format_id = "hdf5"; artifact.layout_digest = std::string(64, 'a');
  artifact.layout_protocol = "catalog-fixture/1";
  artifact.records = {{"d0", "/T", true, {{"units", DatasetAttributeState::Complete,
    DatasetAttributeValue(std::string("K")), {}, {}}}}};
  EXPECT_TRUE(validate_dataset_catalog_marker_artifact(row, m, artifact, 512).ok());
  for (unsigned field = 0; field < 7; ++field) {
    auto changed = artifact; auto changed_row = row;
    switch (field) {
      case 0: changed.records.clear(); break;
      case 1: changed.records[0].attributes.clear(); break;
      case 2: changed.format_id = "arrow"; break;
      case 3: changed.layout_protocol = "other"; break;
      case 4: changed.layout_digest = std::string(64, 'b'); break;
      case 5: changed.protocol = "other"; break;
      case 6: changed_row.key.version = 2; break;
    }
    EXPECT_EQ(validate_dataset_catalog_marker_artifact(changed_row, m, changed, 512).code(), QueryError::CorruptLayout);
  }
  EXPECT_EQ(validate_dataset_catalog_marker_artifact(row, m, artifact, 511).code(), QueryError::CorruptLayout);
}

TEST(DatasetCatalogParsedCache, SharedConstArtifactsUseCompleteNamespaceAndMarkerIdentity) {
  using namespace iotdbfs;
  DatasetCatalogParsedCache cache;
  const auto m = marker_manifest("/a", 1);
  const auto marker = valid_marker("/a", 1);
  const auto identity = make_dataset_catalog_cache_identity(1, m, marker);
  const auto artifact = std::make_shared<const DatasetCatalogArtifact>();
  ASSERT_TRUE(cache.put_validated(identity, artifact));
  EXPECT_EQ(cache.get(identity).get(), artifact.get());
  for (unsigned field = 0; field < 14; ++field) {
    auto changed = identity;
    switch (field) {
      case 0: ++changed.storage_namespace; break;
      case 1: ++changed.manifest_revision; break;
      case 2: changed.format_id = "arrow"; break;
      case 3: ++changed.payload_size; break;
      case 4: ++changed.layout_size; break;
      case 5: changed.marker.key.path = "/b"; break;
      case 6: ++changed.marker.key.version; break;
      case 7: ++changed.marker.size; break;
      case 8: changed.marker.protocol = "other"; break;
      case 9: changed.marker.layout_digest[0] = 'b'; break;
      case 10: changed.marker.layout_protocol = "other"; break;
      case 11: changed.marker.directory_complete = false; break;
      case 12: ++changed.marker.record_count; break;
      case 13: ++changed.marker.attribute_count; break;
    }
    EXPECT_FALSE(cache.get(changed)) << field;
  }
}

TEST(DatasetCatalogParsedCache, LruEvictionAndByteBudgetPreservePinnedArtifacts) {
  using namespace iotdbfs;
  const auto a = make_dataset_catalog_cache_identity(1, marker_manifest("/a", 1), valid_marker("/a", 1));
  const auto b = make_dataset_catalog_cache_identity(1, marker_manifest("/b", 2), valid_marker("/b", 2));
  const auto c = make_dataset_catalog_cache_identity(1, marker_manifest("/c", 3), valid_marker("/c", 3));
  const auto artifact = std::make_shared<const DatasetCatalogArtifact>();
  DatasetCatalogParsedCache cache(2, 1024 * 1024);
  ASSERT_TRUE(cache.put_validated(a, artifact)); ASSERT_TRUE(cache.put_validated(b, artifact));
  auto pinned = cache.get(a);
  ASSERT_TRUE(cache.put_validated(c, artifact));
  EXPECT_FALSE(cache.get(b)); EXPECT_EQ(cache.get(a).get(), pinned.get());
  EXPECT_EQ(cache.size(), 2u); EXPECT_EQ(cache.evictions(), 1u);
  const auto charge = estimate_dataset_catalog_cache_bytes(a, *artifact);
  DatasetCatalogParsedCache bytes(10, charge);
  ASSERT_TRUE(bytes.put_validated(a, artifact));
  ASSERT_TRUE(bytes.put_validated(b, artifact));
  EXPECT_FALSE(bytes.get(a)); EXPECT_LE(bytes.memory_bytes(), charge);
  auto huge = std::make_shared<DatasetCatalogArtifact>();
  huge->layout_protocol.assign(charge + 1, 'x');
  EXPECT_FALSE(bytes.put_validated(c, huge));
  EXPECT_EQ(pinned.get(), artifact.get());
  DatasetCatalogParsedCache disabled(0, 0);
  EXPECT_FALSE(disabled.put_validated(a, artifact)); EXPECT_FALSE(disabled.get(a));
}

TEST(DatasetCatalogParsedCache, ExactKeyAndSubtreeInvalidationRespectSiblingBoundaries) {
  using namespace iotdbfs;
  DatasetCatalogParsedCache cache;
  const auto artifact = std::make_shared<const DatasetCatalogArtifact>();
  const auto key = [](std::string path, std::int64_t version) {
    return make_dataset_catalog_cache_identity(1, marker_manifest(path, version), valid_marker(path, version));
  };
  const auto a1 = key("/a/one", 1), a2 = key("/a/one", 2), sibling = key("/ab/two", 3);
  ASSERT_TRUE(cache.put_validated(a1, artifact)); ASSERT_TRUE(cache.put_validated(a2, artifact));
  ASSERT_TRUE(cache.put_validated(sibling, artifact));
  EXPECT_EQ(cache.invalidate_path("a/one", 1), 1u);
  EXPECT_FALSE(cache.get(a1)); EXPECT_TRUE(cache.get(a2));
  EXPECT_EQ(cache.invalidate_tree("/a"), 1u);
  EXPECT_FALSE(cache.get(a2)); EXPECT_TRUE(cache.get(sibling));
  EXPECT_EQ(cache.invalidate_tree("/"), 1u); EXPECT_EQ(cache.memory_bytes(), 0u);
}

TEST(DatasetCatalogObjectSql, FreezesPerKeySizesAndBoundsKeysSqlAndReturnedBytes) {
  using namespace iotdbfs;
  const auto a = valid_marker("/a'", 1);
  auto b = valid_marker("/b", 2); b.size = 768;
  auto small = compile_dataset_catalog_object_sql_batches({a, b});
  ASSERT_TRUE(small.ok()); ASSERT_EQ(small.value().size(), 1u);
  EXPECT_NE(small.value()[0].sql.find("SELECT time,path,size,READ_OBJECT(data,0,size) FROM blob_dataset_catalog"), std::string::npos);
  EXPECT_NE(small.value()[0].sql.find("(path='/a''' AND time=1 AND size=512)"), std::string::npos);
  EXPECT_NE(small.value()[0].sql.find("(path='/b' AND time=2 AND size=768)"), std::string::npos);
  EXPECT_EQ(small.value()[0].expected_bytes, 1280u);
  std::vector<DatasetCatalogMarker> markers;
  for (unsigned j = 0; j < 129; ++j) markers.push_back(valid_marker("/" + std::to_string(j), j));
  auto split = compile_dataset_catalog_object_sql_batches(markers);
  ASSERT_TRUE(split.ok()); EXPECT_EQ(split.value().size(), 2u);
  markers.clear();
  for (unsigned j = 0; j < 3; ++j) {
    auto marker = valid_marker("/" + std::to_string(j), j); marker.size = 8 * 1024 * 1024;
    markers.push_back(marker);
  }
  split = compile_dataset_catalog_object_sql_batches(markers);
  ASSERT_TRUE(split.ok()); ASSERT_EQ(split.value().size(), 2u);
  EXPECT_EQ(split.value()[0].expected_bytes, 16u * 1024 * 1024);
  markers.clear();
  for (unsigned j = 0; j < 20; ++j) markers.push_back(valid_marker("/" + std::to_string(j) + std::string(4000, '\''), j));
  split = compile_dataset_catalog_object_sql_batches(markers);
  ASSERT_TRUE(split.ok()); EXPECT_GT(split.value().size(), 1u);
  for (const auto& batch : split.value()) {
    EXPECT_LE(batch.sql.size(), 65536u); EXPECT_LE(batch.markers.size(), 128u);
    EXPECT_LE(batch.expected_bytes, 16u * 1024 * 1024);
  }
  EXPECT_TRUE(compile_dataset_catalog_object_sql_batches({}).value().empty());
  auto bad = a; bad.size = -1;
  EXPECT_EQ(compile_dataset_catalog_object_sql_batches({bad}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(compile_dataset_catalog_object_sql_batches({a, a}).status().code(), QueryError::InvalidQuery);
}

TEST(DatasetCatalogObjectRows, RejectsMissingCrossPairedDuplicateAndShortObjectRows) {
  using namespace iotdbfs;
  const auto a = valid_marker("/a", 1), b = valid_marker("/b", 2);
  const DatasetCatalogObjectRow row_a{{"/a", 1}, 512, std::string(512, 'a')};
  const DatasetCatalogObjectRow row_b{{"/b", 2}, 512, std::string(512, 'b')};
  EXPECT_TRUE(validate_dataset_catalog_object_rows({a, b}, {row_b, row_a}).ok());
  EXPECT_EQ(validate_dataset_catalog_object_rows({a, b}, {row_a}).code(), QueryError::ReadFailure);
  auto short_row = row_a; short_row.bytes.pop_back();
  EXPECT_EQ(validate_dataset_catalog_object_rows({a}, {short_row}).code(), QueryError::ReadFailure);
  auto wrong_size = row_a; wrong_size.size = 511;
  EXPECT_EQ(validate_dataset_catalog_object_rows({a}, {wrong_size}).code(), QueryError::ReadFailure);
  auto wrong_version = row_a; wrong_version.key.version = 2;
  EXPECT_EQ(validate_dataset_catalog_object_rows({a, b}, {wrong_version, row_b}).code(), QueryError::CorruptLayout);
  EXPECT_EQ(validate_dataset_catalog_object_rows({a, b}, {row_a, row_a, row_b}).code(), QueryError::CorruptLayout);
}

TEST(QueryUtils, ParseKvQuery_Empty) {
  auto kv = parse_kv_query("");
  EXPECT_TRUE(kv.empty());
}

TEST(QueryUtils, ParseKvQuery_SinglePair) {
  auto kv = parse_kv_query("key=val");
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["key"], "val");
}

TEST(QueryUtils, ParseKvQuery_MultipleAmpersand) {
  auto kv = parse_kv_query("a=1&b=2");
  EXPECT_EQ(kv.size(), 2);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
}

TEST(QueryUtils, ParseKvQuery_MultipleSemicolon) {
  auto kv = parse_kv_query("a=1;b=2");
  EXPECT_EQ(kv.size(), 2);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
}

TEST(QueryUtils, ParseKvQuery_MultipleComma) {
  auto kv = parse_kv_query("a=1,b=2");
  EXPECT_EQ(kv.size(), 2);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
}

TEST(QueryUtils, ParseKvQuery_Whitespace) {
  auto kv = parse_kv_query(" a = 1 , b = 2 ");
  EXPECT_EQ(kv.size(), 2);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
}

TEST(QueryUtils, ParseKvQuery_EmptyKey) {
  auto kv = parse_kv_query("=val");
  EXPECT_TRUE(kv.empty());
}

TEST(QueryUtils, ParseKvQuery_NoValue) {
  auto kv = parse_kv_query("key");
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["key"], "");
}

TEST(QueryUtils, ParseKvQuery_DuplicateKeys) {
  auto kv = parse_kv_query("a=1&a=2");
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["a"], "2");
}

TEST(QueryUtils, ParseKvQuery_MixedSeparators) {
  auto kv = parse_kv_query("a=1&b=2;c=3");
  EXPECT_EQ(kv.size(), 3);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
  EXPECT_EQ(kv["c"], "3");
}

TEST(QueryUtils, ParseKvQuery_WithTrailingSeparator) {
  auto kv = parse_kv_query("a=1&");
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["a"], "1");
}

TEST(QueryUtils, ParseKvQuery_WithLeadingSeparator) {
  auto kv = parse_kv_query("&a=1");
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["a"], "1");
}

TEST(QueryUtils, ParseKvQuery_ConsecutiveSeparators) {
  auto kv = parse_kv_query("a=1&&b=2");
  EXPECT_EQ(kv.size(), 2);
  EXPECT_EQ(kv["a"], "1");
  EXPECT_EQ(kv["b"], "2");
}

TEST(QueryUtils, IsCtrlKey_Type) {
  EXPECT_TRUE(is_ctrl_key("type"));
}

TEST(QueryUtils, IsCtrlKey_Function) {
  EXPECT_TRUE(is_ctrl_key("function"));
}

TEST(QueryUtils, IsCtrlKey_Dtype) {
  EXPECT_TRUE(is_ctrl_key("dtype"));
}

TEST(QueryUtils, IsCtrlKey_Endian) {
  EXPECT_TRUE(is_ctrl_key("endian"));
}

TEST(QueryUtils, IsCtrlKey_Interp) {
  EXPECT_TRUE(is_ctrl_key("interp"));
}

TEST(QueryUtils, IsCtrlKey_OutNi) {
  EXPECT_TRUE(is_ctrl_key("outni"));
}

TEST(QueryUtils, IsCtrlKey_OutNj) {
  EXPECT_TRUE(is_ctrl_key("outnj"));
}

TEST(QueryUtils, IsCtrlKey_Dlon) {
  EXPECT_TRUE(is_ctrl_key("dlon"));
}

TEST(QueryUtils, IsCtrlKey_Dlat) {
  EXPECT_TRUE(is_ctrl_key("dlat"));
}

TEST(QueryUtils, IsCtrlKey_Border) {
  EXPECT_TRUE(is_ctrl_key("border"));
}

TEST(QueryUtils, IsCtrlKey_CropKeys) {
  EXPECT_TRUE(is_ctrl_key("lat0"));
  EXPECT_TRUE(is_ctrl_key("lon0"));
  EXPECT_TRUE(is_ctrl_key("lat1"));
  EXPECT_TRUE(is_ctrl_key("lon1"));
}

TEST(QueryUtils, IsCtrlKey_CaseInsensitive) {
  EXPECT_TRUE(is_ctrl_key("DTYPE"));
  EXPECT_TRUE(is_ctrl_key("Interp"));
  EXPECT_TRUE(is_ctrl_key("BORDER"));
  EXPECT_TRUE(is_ctrl_key("Lat0"));
}

TEST(QueryUtils, IsCtrlKey_NonControl) {
  EXPECT_FALSE(is_ctrl_key("shortName"));
  EXPECT_FALSE(is_ctrl_key("level"));
  EXPECT_FALSE(is_ctrl_key("other"));
}

TEST(QueryUtils, FilterIndexMatchKv_AllControl) {
  std::map<std::string, std::string> kv = {{"dtype", "f64"}, {"interp", "bilinear"}};
  auto filtered = filter_index_match_kv(kv);
  EXPECT_TRUE(filtered.empty());
}

TEST(QueryUtils, FilterIndexMatchKv_AllNonControl) {
  std::map<std::string, std::string> kv = {{"shortName", "t"}, {"level", "500"}};
  auto filtered = filter_index_match_kv(kv);
  EXPECT_EQ(filtered, kv);
}

TEST(QueryUtils, FilterIndexMatchKv_Mixed) {
  std::map<std::string, std::string> kv = {
    {"shortName", "t"},
    {"dtype", "f64"},
    {"level", "500"}
  };
  auto filtered = filter_index_match_kv(kv);
  EXPECT_EQ(filtered.size(), 2);
  EXPECT_EQ(filtered["shortName"], "t");
  EXPECT_EQ(filtered["level"], "500");
  EXPECT_EQ(filtered.count("dtype"), 0);
}

TEST(QueryUtils, FilterIndexMatchKv_Empty) {
  std::map<std::string, std::string> kv = {};
  auto filtered = filter_index_match_kv(kv);
  EXPECT_TRUE(filtered.empty());
}

TEST(QueryUtils, FilterIndexMatchKv_CaseInsensitiveControl) {
  std::map<std::string, std::string> kv = {
    {"shortName", "t"},
    {"DTYPE", "f64"}
  };
  auto filtered = filter_index_match_kv(kv);
  EXPECT_EQ(filtered.size(), 1);
  EXPECT_EQ(filtered["shortName"], "t");
}

TEST(QueryUtils, FilterIndexMatchKv_RemovesCropKeys) {
  std::map<std::string, std::string> kv = {
    {"shortName", "t"},
    {"level", "500"},
    {"lat0", "30"},
    {"lon0", "100"},
    {"lat1", "10"},
    {"lon1", "140"}
  };
  auto filtered = filter_index_match_kv(kv);
  EXPECT_EQ(filtered.size(), 2);
  EXPECT_EQ(filtered["shortName"], "t");
  EXPECT_EQ(filtered["level"], "500");
  EXPECT_EQ(filtered.count("lat0"), 0);
  EXPECT_EQ(filtered.count("lon0"), 0);
  EXPECT_EQ(filtered.count("lat1"), 0);
  EXPECT_EQ(filtered.count("lon1"), 0);
}

TEST(QueryUtils, SqlLiteralEscapesSingleQuotes) {
  EXPECT_EQ(sql_literal("O'Reilly"), "'O''Reilly'");
  EXPECT_EQ(sql_literal("line\nvalue"), "'line\nvalue'");
}

TEST(QueryUtils, LikePatternEscapesWildcardsAndEscapeCharacter) {
  EXPECT_EQ(like_pattern("/a%_\\b!"), "/a!%!_\\b!!");
}

TEST(QueryUtils, LikeSubtreePredicateUsesEscapeClause) {
  EXPECT_EQ(like_subtree_predicate("/a%"),
    "path LIKE '/a!%/%' ESCAPE '!'");
}

TEST(QueryUtils, LikeSubtreePredicateEscapesSqlQuoteAndLikeMetacharacters) {
  EXPECT_EQ(like_subtree_predicate("/a'_!%\\b"),
    "path LIKE '/a''!_!!!%\\b/%' ESCAPE '!'");
}


TEST(QueryUtils, IdentifierValidationRejectsSqlSyntax) {
  EXPECT_TRUE(is_safe_identifier("dfs_01"));
  EXPECT_FALSE(is_safe_identifier("bad'name"));
  EXPECT_FALSE(is_safe_identifier("dfs;DROP DATABASE x"));
  EXPECT_FALSE(is_safe_identifier(""));
}

TEST(QueryUtils, IdentifierLengthAndLeadingCharacterBoundaries) {
  EXPECT_TRUE(is_safe_identifier("_private_01"));
  EXPECT_TRUE(is_safe_identifier(std::string(128, 'a')));
  EXPECT_FALSE(is_safe_identifier(std::string(129, 'a')));
  EXPECT_FALSE(is_safe_identifier("1table"));
  EXPECT_FALSE(is_safe_identifier(" table"));
  EXPECT_FALSE(is_safe_identifier("table\n"));
  EXPECT_FALSE(is_safe_identifier(std::string("table\0suffix", 12)));
}

TEST(QueryUtils, SqlEscapingPreservesUtf8AndDoublesEveryQuote) {
  EXPECT_EQ(sql_escape("'天气' O'Reilly"), "''天气'' O''Reilly");
  EXPECT_EQ(sql_escape(""), "");
  EXPECT_EQ(sql_escape_literal("''"), "''''''");
  EXPECT_EQ(sql_literal(""), "''");
  EXPECT_EQ(sql_escape_literal(std::string("a\0b", 3)), std::string("'a\0b'", 5));
}

TEST(QueryUtils, ParserPreservesEmbeddedEqualsAndDoesNotDecodePercentEscapes) {
  const auto parsed = parse_kv_query(" \t ; token=a=b=c&name=天气&path=a%2Fb&empty= \r\n");
  const std::map<std::string, std::string> expected{
    {"token", "a=b=c"}, {"name", "天气"}, {"path", "a%2Fb"}, {"empty", ""}};
  EXPECT_EQ(parsed, expected);
  EXPECT_TRUE(parse_kv_query(" \t ; , & =value ").empty());
}
