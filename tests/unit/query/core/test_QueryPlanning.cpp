#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <thread>
#include <type_traits>

using namespace iotdbfs;

namespace {

LogicalType type(LogicalTypeId id, bool nullable = false) {
  return {id, nullable, std::nullopt};
}

LogicalSchema value_schema() {
  auto schema = LogicalSchema::create({
    {"domain.time", "Time",
      {LogicalTypeId::TimestampNs, false, "UTC"},
      FieldRole::Dimension, "ns"},
    {"data.value", "Value", type(LogicalTypeId::Float64, true),
      FieldRole::Measure, std::nullopt},
  });
  EXPECT_TRUE(schema.ok()) << schema.status().message();
  return schema.value();
}

StructuredBlobManifest manifest(std::string path, std::int64_t version,
    std::optional<std::string> format_id = std::string("test-format"),
    bool published = true) {
  StructuredBlobManifest result;
  result.key = {std::move(path), version};
  result.format_id = std::move(format_id);
  result.metadata = {{"station", MetadataValue("A")}};
  result.payload_size = 4;
  result.layout_size = result.format_id ? 3 : 0;
  result.phase = published ? StructuredBlobPhase::Published
                           : StructuredBlobPhase::Described;
  result.published = published;
  result.revision = published ? 5 : 4;
  return result;
}

class ReadOnlyBackend final : public StructuredBlobStorageBackend {
public:
  explicit ReadOnlyBackend(StructuredBlobManifest manifest,
      StructuredBlobBytes payload = {1, 2, 3, 4},
      StructuredBlobBytes layout = {8, 9, 10},
      bool delay_payload_reads = false)
    : manifest_(std::move(manifest)), payload_(std::move(payload)),
      layout_(std::move(layout)),
      delay_payload_reads_(delay_payload_reads) {}

  StructuredBlobStatus create_manifest(
      const StructuredBlobManifest&) override {
    return unsupported();
  }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    ++manifest_load_count_;
    if (!(key == manifest_.key))
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "not found");
    return StructuredBlobResult<StructuredBlobManifest>::success(manifest_);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobManifest&) override {
    return unsupported();
  }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobBytes&) override {
    return unsupported();
  }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,
      const StructuredBlobBytes&) override {
    return unsupported();
  }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,
      const MetadataMap&) override {
    return unsupported();
  }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
      const StructuredBlobVersionKey&, std::uint64_t offset,
      std::uint64_t length) const override {
    ++payload_read_count_;
    const auto active = active_payload_reads_.fetch_add(1) + 1;
    auto maximum = max_active_payload_reads_.load();
    while (maximum < active &&
        !max_active_payload_reads_.compare_exchange_weak(maximum, active)) {}
    if (delay_payload_reads_)
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto begin = payload_.begin() + static_cast<std::size_t>(offset);
    auto result = StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
    --active_payload_reads_;
    return result;
  }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobVersionKey&,
      const StructuredBlobPayloadRanges& ranges) const override {
    ++payload_batch_read_count_;
    StructuredBlobPayloadBatch batch;
    batch.reserve(ranges.size());
    for (const auto& range : ranges) {
      const auto begin = payload_.begin() +
        static_cast<std::size_t>(range.offset);
      batch.emplace_back(begin,
        begin + static_cast<std::size_t>(range.length));
    }
    if (short_batch_read_ && !batch.empty()) batch.back().pop_back();
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(
      std::move(batch));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey&, std::uint64_t offset,
      std::uint64_t length) const override {
    ++layout_read_count_;
    last_layout_offset_ = offset;
    last_layout_length_ = length;
    const auto begin = layout_.begin() + static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&,
      std::uint64_t) override {
    return unsupported();
  }

  std::size_t manifest_load_count() const { return manifest_load_count_.load(); }
  std::size_t payload_read_count() const { return payload_read_count_.load(); }
  std::size_t payload_batch_read_count() const {
    return payload_batch_read_count_.load();
  }
  void set_short_batch_read(bool value) { short_batch_read_ = value; }
  std::size_t layout_read_count() const { return layout_read_count_.load(); }
  std::size_t max_active_payload_reads() const {
    return max_active_payload_reads_.load();
  }
  std::uint64_t last_layout_offset() const { return last_layout_offset_; }
  std::uint64_t last_layout_length() const { return last_layout_length_; }

private:
  static StructuredBlobStatus unsupported() {
    return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
      "read-only test backend");
  }

  StructuredBlobManifest manifest_;
  StructuredBlobBytes payload_;
  StructuredBlobBytes layout_;
  bool delay_payload_reads_{false};
  mutable std::atomic<std::size_t> manifest_load_count_{0};
  mutable std::atomic<std::size_t> payload_read_count_{0};
  mutable std::atomic<std::size_t> payload_batch_read_count_{0};
  bool short_batch_read_{false};
  mutable std::atomic<std::size_t> layout_read_count_{0};
  mutable std::atomic<std::size_t> active_payload_reads_{0};
  mutable std::atomic<std::size_t> max_active_payload_reads_{0};
  mutable std::uint64_t last_layout_offset_{0};
  mutable std::uint64_t last_layout_length_{0};
};

class TestExecutionPlan final : public AdapterExecutionPlan {};

static_assert(!std::is_copy_constructible_v<AdapterExecutionPlan>,
  "opaque adapter execution plans must not be copied during execution");

static_assert(!std::is_copy_constructible_v<QueryPlanningContext>);
static_assert(!std::is_move_constructible_v<QueryPlanningContext>);

class TestQueryAdapter final : public FormatQueryAdapter {
public:
  explicit TestQueryAdapter(std::string id) : id_(std::move(id)) {}

  const std::string& id() const override { return id_; }

  QueryResult<QueryCapabilities> capabilities(
      const QueryPlanningContext&, const QueryFragment&) const override {
    return QueryResult<QueryCapabilities>::success(QueryCapabilities(
      1, id_, "test-1", value_schema(), {}, {DomainKind::TimeSeries},
      true, false, OrderingGuarantee::Domain));
  }

  QueryResult<AdapterPlan> prepare(const QueryPlanningContext&,
      const QueryFragment& fragment) const override {
    return QueryResult<AdapterPlan>::success(AdapterPlan(
      std::make_shared<const TestExecutionPlan>(), value_schema(), nullptr,
      {}, fragment.projection));
  }

  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext&,
      std::shared_ptr<const AdapterExecutionPlan>,
      const QueryExecutionOptions&) const override {
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(
      QueryStatus::failure(QueryError::UnsupportedCapability,
        QueryPhase::Execution, "not used by planning tests"));
  }

private:
  std::string id_;
};

QueryRequest semantic_request() {
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Subtree, "/science"}};
  request.sources.versions = VersionSelection::latest();
  request.subset = QueryFragment{
    {{"data.value"}}, nullptr,
    {{{"domain.time"},
      DomainBound{LogicalValue::timestamp_ns(100), BoundKind::Inclusive},
      DomainBound{LogicalValue::timestamp_ns(200), BoundKind::Exclusive}}},
    {}};
  return request;
}

QueryRequest hdf5_request() {
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.h5"}};
  request.sources.versions = VersionSelection::exact_version(7);
  request.subset = QueryFragment{};
  request.subset->projection = {{"hdf5.value:%2Fa"}};
  request.subset->slices = {{{"hdf5.axis.0"},
    DomainBound{LogicalValue::uint64(1), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(5), BoundKind::Exclusive}}};
  return request;
}

} // namespace

TEST(PublishedVersionCatalog, SelectsOnlyPublishedLatestMatchingVersions) {
  auto residual = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("A"));
  auto catalog = make_in_memory_published_version_catalog({
    manifest("/science/a.tsfile", 1),
    manifest("/science/a.tsfile", 3),
    manifest("/science/a.tsfile", 4, std::string("test-format"), false),
    manifest("/science/b.arrow", 2, std::string("arrow-ipc")),
    manifest("/other/c.bin", 5, std::nullopt),
  });
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Subtree, "/science"}};
  selector.versions = VersionSelection::latest();
  selector.formats.adapters = AdapterSelectionKind::Listed;
  selector.formats.adapter_ids = {"test-format"};
  selector.formats.include_byte_passthrough = false;
  selector.file_predicate = residual;

  auto result = catalog->scan(selector, 10);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().versions().size(), 1u);
  EXPECT_EQ(result.value().versions()[0].key.path, "/science/a.tsfile");
  EXPECT_EQ(result.value().versions()[0].key.version, 3);
  EXPECT_EQ(result.value().residual_file_predicate, residual);
}

TEST(PublishedVersionCatalog, RefreshesLatestSelectionForEveryScan) {
  auto snapshots = std::make_shared<std::vector<StructuredBlobManifest>>(
    std::initializer_list<StructuredBlobManifest>{
      manifest("/science/a.tsfile", 1)});
  auto loads = std::make_shared<std::atomic<std::size_t>>(0);
  auto catalog = make_refreshing_published_version_catalog(
    [snapshots, loads](const SourceSelector&) {
      loads->fetch_add(1, std::memory_order_relaxed);
      return QueryResult<PublishedVersionSnapshot>::success(
        PublishedVersionSnapshot{*snapshots, {}});
    });
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  selector.versions = VersionSelection::latest();

  auto first = catalog->scan(selector, 10);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_EQ(first.value().versions().size(), 1u);
  EXPECT_EQ(first.value().versions()[0].key.version, 1);

  snapshots->push_back(manifest("/science/a.tsfile", 2));
  auto second = catalog->scan(selector, 10);
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().versions().size(), 1u);
  EXPECT_EQ(second.value().versions()[0].key.version, 2);
  EXPECT_EQ(loads->load(std::memory_order_relaxed), 2u);
}

TEST(PublishedVersionCatalog, PreservesSnapshotLoadFailures) {
  auto expected = QueryStatus::failure(QueryError::ReadFailure,
    QueryPhase::Catalog, "injected catalog failure",
    {{}, {}, std::string("catalog-test"), true});
  auto catalog = make_refreshing_published_version_catalog(
    [expected](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::failure(
        expected);
    });
  SourceSelector selector;
  selector.versions = VersionSelection::latest();

  auto result = catalog->scan(selector, 10);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ReadFailure);
  EXPECT_EQ(result.status().phase(), QueryPhase::Catalog);
  EXPECT_EQ(result.status().message(), "injected catalog failure");
  ASSERT_TRUE(result.status().detail().operation);
  EXPECT_EQ(*result.status().detail().operation, "catalog-test");
}

TEST(PublishedVersionCatalog, RemovingLatestRevealsSurvivingVersionWithoutRebindingExact) {
  const auto old = manifest("/science/a.h5", 1, std::string("hdf5"));
  const auto latest = manifest("/science/a.h5", 2, std::string("hdf5"));
  auto visible = std::make_shared<std::vector<StructuredBlobManifest>>(
    std::initializer_list<StructuredBlobManifest>{old, latest});
  auto catalog = make_refreshing_published_version_catalog(
    [visible](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success({*visible, {}});
    });
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, old.key.path}};
  selector.versions = VersionSelection::latest();
  const auto before = catalog->scan(selector, 1);
  ASSERT_TRUE(before.ok());
  ASSERT_EQ(before.value().versions().size(), 1u);
  EXPECT_EQ(before.value().versions()[0].key.version, 2);

  // Model the published snapshot after the exact latest inode was removed.
  // This freezes selection semantics; persistent tombstones are tested by storage.
  visible->pop_back();
  const auto after = catalog->scan(selector, 1);
  ASSERT_TRUE(after.ok());
  ASSERT_EQ(after.value().versions().size(), 1u);
  EXPECT_EQ(after.value().versions()[0].key.version, 1);
  EXPECT_EQ(before.value().versions()[0].key.version, 2);

  selector.versions = VersionSelection::exact_version(2);
  const auto removed = catalog->scan(selector, 1);
  ASSERT_TRUE(removed.ok());
  EXPECT_TRUE(removed.value().versions().empty());
  selector.versions = VersionSelection::exact_version(1);
  const auto surviving = catalog->scan(selector, 1);
  ASSERT_TRUE(surviving.ok());
  ASSERT_EQ(surviving.value().versions().size(), 1u);
  EXPECT_EQ(surviving.value().versions()[0].key.version, 1);
}

TEST(PublishedVersionCatalog, LatestAfterDeletionIncludesEmptyVersionsUnlessFormatFiltered) {
  const auto old = manifest("/science/a.h5", 1, std::string("hdf5"));
  auto empty = manifest("/science/a.h5", 2, std::nullopt);
  empty.payload_size = 0;
  // An O_TRUNC/create publication is a real byte-passthrough version. After
  // deleting a later HDF5 version it remains eligible for an all-format scan.
  auto catalog = make_in_memory_published_version_catalog({old, empty});
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, old.key.path}};
  selector.versions = VersionSelection::latest();
  const auto all_formats = catalog->scan(selector, 1);
  ASSERT_TRUE(all_formats.ok());
  ASSERT_EQ(all_formats.value().versions().size(), 1u);
  EXPECT_EQ(all_formats.value().versions()[0].key.version, 2);
  EXPECT_FALSE(all_formats.value().versions()[0].format_id);
  EXPECT_EQ(all_formats.value().versions()[0].payload_size, 0u);

  selector.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  const auto hdf5 = catalog->scan(selector, 1);
  ASSERT_TRUE(hdf5.ok());
  ASSERT_EQ(hdf5.value().versions().size(), 1u);
  EXPECT_EQ(hdf5.value().versions()[0].key.version, 1);
}

TEST(PublishedVersionCatalog, LoaderReceivesSelectorButFinalSelectionStillApplies) {
  unsigned calls = 0;
  SourceSelector seen;
  auto catalog = make_refreshing_published_version_catalog(
    [&](const SourceSelector& selector) {
      ++calls;
      seen = selector;
      // A conservative loader can return extras, including a later version
      // with a different format. It cannot change latest or residual semantics.
      return QueryResult<PublishedVersionSnapshot>::success({{
        manifest("/science/a", 1, std::string("hdf5")),
        manifest("/science/a", 2, std::string("hdf5")),
        manifest("/science/a", 3, std::string("grib")),
        manifest("/other/b", 4, std::string("hdf5"))}, {}});
    });
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, "/science/a"}};
  selector.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  selector.file_predicate = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("A"));
  auto selected = catalog->scan(selector, 1);
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  ASSERT_EQ(selected.value().versions().size(), 1u);
  EXPECT_EQ(selected.value().versions()[0].key.version, 2);
  EXPECT_EQ(selected.value().residual_file_predicate, selector.file_predicate);
  ASSERT_EQ(seen.paths.size(), 1u);
  EXPECT_EQ(seen.paths[0].path, "/science/a");
  EXPECT_EQ(seen.formats.adapter_ids, selector.formats.adapter_ids);
  EXPECT_EQ(seen.file_predicate, selector.file_predicate);
  selector.versions = VersionSelection::exact_version(-1);
  EXPECT_FALSE(catalog->scan(selector, 1).ok());
  EXPECT_EQ(calls, 1u);
  selector.versions = VersionSelection::latest();
  EXPECT_FALSE(catalog->scan(selector, 0).ok());
  EXPECT_EQ(calls, 1u);
}

TEST(PublishedVersionCatalog, RetainsOnlyExactSelectedPermissionEvidence) {
  auto current = manifest("/science/a", 2, std::string("hdf5"));
  current.revision = 7; current.payload_size = 128; current.layout_size = 64;
  auto old = current; old.key.version = 1;
  auto other = current; other.key.path = "/other/b";
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = 42;
  permissions->metadata_generation = 9;
  permissions->observed_at = std::chrono::steady_clock::now();
  const CatalogSourcePermission fact{"hdf5", 7, 128, 64, 0600, 1001, 1002, false};
  for (const auto& m : {old, current, other})
    permissions->sources.emplace(m.key, fact);
  permissions->parents.emplace("/science", CatalogParentPermission{8, 0755, 1, 2, true});
  permissions->parents.emplace("/", std::nullopt);
  permissions->parents.emplace("/other", CatalogParentPermission{9, 0700, 1, 2, true});
  auto catalog = make_refreshing_published_version_catalog(
    [=](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success(
        {{old, current, other}, permissions});
    }, 42);
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, current.key.path}};
  auto selected = catalog->scan(selector, 1);
  ASSERT_TRUE(selected.ok());
  ASSERT_TRUE(selected.value().source_permissions);
  const auto& evidence = *selected.value().source_permissions;
  EXPECT_EQ(evidence.storage_namespace, 42u);
  EXPECT_EQ(evidence.metadata_generation, 9u);
  EXPECT_EQ(evidence.observed_at, permissions->observed_at);
  ASSERT_EQ(evidence.sources.size(), 1u);
  const auto& copied = evidence.sources.at(current.key);
  EXPECT_EQ(copied.uid, 1001);
  EXPECT_EQ(copied.gid, 1002);
  EXPECT_EQ(copied.mode, 0600);
  ASSERT_EQ(evidence.parents.size(), 2u);
  ASSERT_TRUE(evidence.parents.at("/science"));
  EXPECT_EQ(evidence.parents.at("/science")->version, 8);
  EXPECT_FALSE(evidence.parents.at("/"));
  EXPECT_EQ(evidence.parents.count("/other"), 0u);
  // Mutating the loader's next snapshot cannot alter a previous selection.
  permissions->sources.at(current.key).mode = 0000;
  EXPECT_EQ(copied.mode, 0600);
  EXPECT_EQ(selected.value().versions()[0].revision, current.revision);
}

TEST(PublishedVersionCatalog, HistoricalPermissionEvidenceKeepsExactInodeAndCurrentParents) {
  const auto old = manifest("/science/a.h5", 1, std::string("hdf5"));
  const auto current = manifest("/science/a.h5", 2, std::string("hdf5"));
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = 42;
  permissions->metadata_generation = 9;
  permissions->observed_at = std::chrono::steady_clock::now();
  permissions->sources.emplace(old.key, CatalogSourcePermission{
    "hdf5", old.revision, old.payload_size, old.layout_size, 0640, 1001, 1002, false});
  permissions->sources.emplace(current.key, CatalogSourcePermission{
    "hdf5", current.revision, current.payload_size, current.layout_size, 0000, 2001, 2002, false});
  permissions->parents.emplace("/", CatalogParentPermission{3, 0755, 0, 0, true});
  permissions->parents.emplace("/science", CatalogParentPermission{10, 0710, 3001, 3002, true});
  auto catalog = make_refreshing_published_version_catalog(
    [=](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success({{old, current}, permissions});
    }, 42);
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, old.key.path}};
  selector.versions = VersionSelection::exact_version(1);
  const auto selected = catalog->scan(selector, 1);
  ASSERT_TRUE(selected.ok());
  ASSERT_TRUE(selected.value().source_permissions);
  const auto& evidence = *selected.value().source_permissions;
  ASSERT_EQ(evidence.sources.size(), 1u);
  EXPECT_EQ(evidence.sources.count(current.key), 0u);
  EXPECT_EQ(evidence.sources.at(old.key).mode, 0640);
  EXPECT_EQ(evidence.sources.at(old.key).uid, 1001);
  EXPECT_EQ(evidence.sources.at(old.key).gid, 1002);
  ASSERT_TRUE(evidence.parents.at("/science"));
  EXPECT_EQ(evidence.parents.at("/science")->version, 10);
  EXPECT_EQ(evidence.parents.at("/science")->mode, 0710);
  EXPECT_EQ(evidence.parents.at("/science")->uid, 3001);
}

TEST(PublishedVersionCatalog, ParentObservationsDoNotNeedSourcePermissionOrBecomeSources) {
  auto m = manifest("/science/deep/a", 2, std::string("hdf5"));
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = 42;
  permissions->parents.emplace("/science/deep", CatalogParentPermission{8, 0644, 1, 2, false});
  permissions->parents.emplace("/science", CatalogParentPermission{9, -1, 1, 2, true});
  permissions->parents.emplace("/", std::nullopt);
  permissions->parents.emplace("/unrelated", CatalogParentPermission{10, 0755, 1, 2, true});
  auto catalog = make_refreshing_published_version_catalog(
    [=](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success({{m}, permissions});
    }, 42);
  auto selected = catalog->scan(SourceSelector{}, 1);
  ASSERT_TRUE(selected.ok());
  ASSERT_EQ(selected.value().versions().size(), 1u);
  ASSERT_TRUE(selected.value().source_permissions);
  const auto& evidence = *selected.value().source_permissions;
  EXPECT_TRUE(evidence.sources.empty());
  ASSERT_EQ(evidence.parents.size(), 3u);
  ASSERT_TRUE(evidence.parents.at("/science/deep"));
  EXPECT_FALSE(evidence.parents.at("/science/deep")->is_dir);
  EXPECT_FALSE(evidence.parents.at("/science"));
  EXPECT_FALSE(evidence.parents.at("/"));
  EXPECT_EQ(evidence.parents.count("/unrelated"), 0u);
}

TEST(PublishedVersionCatalog, DropsForeignOrMismatchedPermissionEvidence) {
  auto m = manifest("/science/a", 2, std::string("hdf5"));
  m.revision = 7; m.payload_size = 128; m.layout_size = 64;
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = 42;
  const CatalogSourcePermission good{"hdf5", 7, 128, 64, 0600, 1001, 1002, false};
  permissions->sources.emplace(m.key, good);
  auto catalog = make_refreshing_published_version_catalog(
    [=](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success({{m}, permissions});
    }, 42);
  for (int mismatch = 0; mismatch < 8; ++mismatch) {
    auto& fact = permissions->sources.at(m.key);
    fact = good;
    switch (mismatch) {
    case 0: fact.format_id = "grib"; break;
    case 1: ++fact.manifest_revision; break;
    case 2: ++fact.payload_size; break;
    case 3: ++fact.layout_size; break;
    case 4: fact.is_dir = true; break;
    case 5: fact.mode = -1; break;
    case 6: fact.uid = -1; break;
    case 7: fact.gid = -1; break;
    }
    auto selected = catalog->scan(SourceSelector{}, 1);
    ASSERT_TRUE(selected.ok());
    ASSERT_TRUE(selected.value().source_permissions);
    EXPECT_TRUE(selected.value().source_permissions->sources.empty());
    EXPECT_EQ(selected.value().versions().size(), 1u);
  }
  permissions->sources.at(m.key) = good;
  permissions->storage_namespace = 99;
  auto foreign = catalog->scan(SourceSelector{}, 1);
  ASSERT_TRUE(foreign.ok());
  EXPECT_FALSE(foreign.value().source_permissions);
  permissions->storage_namespace = 0;
  auto unknown = catalog->scan(SourceSelector{}, 1);
  ASSERT_TRUE(unknown.ok());
  EXPECT_FALSE(unknown.value().source_permissions);
}

TEST(AdapterPlan, CopiesShareTheImmutablePlanState) {
  auto implementation = std::make_shared<const TestExecutionPlan>();
  AdapterPlan original(implementation, value_schema(), nullptr, {},
    {{"data.value"}});
  const AdapterPlan copied = original;

  EXPECT_EQ(copied.implementation().get(), implementation.get());
  EXPECT_EQ(&copied.adapter_output_schema(), &original.adapter_output_schema());
  EXPECT_EQ(&copied.residual_slices(), &original.residual_slices());
  EXPECT_EQ(&copied.final_projection(), &original.final_projection());
}

TEST(PublishedVersionCatalog, SupportsVersionRangesAndExplicitPassThrough) {
  auto catalog = make_in_memory_published_version_catalog({
    manifest("/science/a.bin", 1, std::nullopt),
    manifest("/science/a.bin", 2, std::nullopt),
    manifest("/science/a.tsfile", 2),
  });
  SourceSelector selector;
  selector.versions = VersionSelection::between(1, 3);
  selector.formats.adapters = AdapterSelectionKind::Listed;
  selector.formats.include_byte_passthrough = true;

  auto result = catalog->scan(selector, 10);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().versions().size(), 2u);
  EXPECT_TRUE(std::all_of(result.value().versions().begin(),
    result.value().versions().end(), [](const auto& item) {
      return !item.format_id;
    }));
  EXPECT_EQ(catalog->scan(selector, 1).status().code(),
    QueryError::ResourceExhausted);
}

TEST(QuerySource, OpensPublishedSnapshotAndBoundsPayloadAndLayoutReads) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 7));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto opened = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 7});
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  EXPECT_EQ(opened.value()->manifest().revision, 5u);

  auto payload = opened.value()->read_payload(1, 2);
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), (StructuredBlobBytes{2, 3}));
  auto layout = opened.value()->read_layout(1, 2);
  ASSERT_TRUE(layout.ok());
  EXPECT_EQ(layout.value(), (StructuredBlobBytes{9, 10}));

  EXPECT_EQ(backend->manifest_load_count(), 1u);
  EXPECT_EQ(backend->payload_read_count(), 1u);
  EXPECT_EQ(backend->layout_read_count(), 1u);
  EXPECT_EQ(backend->last_layout_offset(), 1u);
  EXPECT_EQ(backend->last_layout_length(), 2u);

  EXPECT_EQ(opened.value()->read_payload(4, 1).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(opened.value()->read_layout(3, 1).status().code(),
    QueryError::InvalidQuery);
}

TEST(QuerySource, BatchesExactVersionRangesThroughOneBackendCall) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/batch.arrow", 11));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto opened = open_structured_blob_query_source(storage,
    {"/science/batch.arrow", 11});
  ASSERT_TRUE(opened.ok()) << opened.status().message();

  const StructuredBlobPayloadRanges ranges{{2, 2}, {0, 1}, {1, 2}};
  auto result = opened.value()->read_payload_ranges(ranges);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value(), (StructuredBlobPayloadBatch{
    {3, 4}, {1}, {2, 3}}));
  EXPECT_EQ(backend->manifest_load_count(), 1u);
  EXPECT_EQ(backend->payload_batch_read_count(), 1u);
  EXPECT_EQ(backend->payload_read_count(), 0u);

  backend->set_short_batch_read(true);
  EXPECT_EQ(opened.value()->read_payload_ranges(ranges).status().code(),
    QueryError::ReadFailure);
}

TEST(QuerySource, RejectsInvalidPayloadBatchesBeforeBackendRead) {
  auto item = manifest("/science/batch.arrow", 12);
  item.payload_size = 16ULL * 1024 * 1024;
  auto backend = std::make_shared<ReadOnlyBackend>(item);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto opened = open_structured_blob_query_source(storage, item.key);
  ASSERT_TRUE(opened.ok());

  EXPECT_EQ(opened.value()->read_payload_ranges({}).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(opened.value()->read_payload_ranges({{0, 0}}).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(opened.value()->read_payload_ranges({{item.payload_size, 1}})
    .status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(opened.value()->read_payload_ranges(
    {{0, kMaxPayloadBytesPerBatch}, {0, 1}}).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(opened.value()->read_payload_ranges(
    StructuredBlobPayloadRanges(kMaxPayloadRangesPerBatch + 1,
      {0, 1})).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(backend->payload_batch_read_count(), 0u);
}

TEST(QuerySource, NamespaceIsSharedOnlyByTheSameBackendInstance) {
  const auto item = manifest("/science/shared.arrow", 7);
  auto backend = std::make_shared<ReadOnlyBackend>(item);
  auto other_backend = std::make_shared<ReadOnlyBackend>(item);
  auto first_storage = std::make_shared<StructuredBlobStorage>(backend);
  auto second_storage = std::make_shared<StructuredBlobStorage>(backend);
  auto isolated_storage = std::make_shared<StructuredBlobStorage>(other_backend);
  auto first = open_structured_blob_query_source(first_storage, item.key);
  auto second = open_structured_blob_query_source(second_storage, item.key);
  auto isolated = open_structured_blob_query_source(isolated_storage, item.key);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(isolated.ok());
  EXPECT_NE(first.value().get(), second.value().get());
  EXPECT_EQ(first.value()->storage_namespace(), backend->storage_namespace());
  EXPECT_EQ(first.value()->storage_namespace(), second.value()->storage_namespace());
  EXPECT_NE(first.value()->storage_namespace(), isolated.value()->storage_namespace());

  const auto namespace_id = first.value()->storage_namespace();
  backend.reset();
  first_storage.reset();
  second_storage.reset();
  EXPECT_EQ(first.value()->storage_namespace(), namespace_id);
  ASSERT_TRUE(first.value()->read_payload(0, 1).ok());
}

TEST(QuerySource, CopiedStandaloneSourcesReceiveFreshNamespaces) {
  struct StandaloneSource final : QuerySource {
    StructuredBlobManifest item;
    const StructuredBlobManifest& manifest() const override { return item; }
    QueryResult<StructuredBlobBytes> read_payload(std::uint64_t,
        std::uint64_t) const override {
      return QueryResult<StructuredBlobBytes>::success({});
    }
    QueryResult<StructuredBlobBytes> read_layout(std::uint64_t,
        std::uint64_t) const override {
      return QueryResult<StructuredBlobBytes>::success({});
    }
  };
  const StandaloneSource first;
  const StandaloneSource copy(first);
  EXPECT_NE(first.storage_namespace(), copy.storage_namespace());
  const ReadOnlyBackend backend(first.manifest());
  EXPECT_NE(first.storage_namespace(), backend.storage_namespace());
}

TEST(QuerySource, RejectsUnpublishedOrUnknownVersions) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 7, std::string("test-format"), false));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto opened = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 7});
  ASSERT_FALSE(opened.ok());
  EXPECT_EQ(opened.status().code(), QueryError::SourceNotFound);
}

TEST(QuerySource, ConcurrentRangeReadsReusePublishedManifest) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 8), StructuredBlobBytes{1, 2, 3, 4},
    StructuredBlobBytes{8, 9, 10}, true);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto opened = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 8});
  ASSERT_TRUE(opened.ok()) << opened.status().message();

  std::vector<std::future<QueryResult<StructuredBlobBytes>>> reads;
  for (std::uint64_t offset = 0; offset < 4; ++offset) {
    reads.push_back(std::async(std::launch::async,
      [source = opened.value(), offset] {
        return source->read_payload(offset, 1);
      }));
  }
  for (auto& read : reads) ASSERT_TRUE(read.get().ok());

  EXPECT_EQ(backend->manifest_load_count(), 1u);
  EXPECT_EQ(backend->payload_read_count(), 4u);
  EXPECT_GT(backend->max_active_payload_reads(), 1u);
}

TEST(QueryRequest, SeparatesFileSelectionFromTypedInFileSubset) {
  QueryRequest file_selection;
  file_selection.sources.versions = VersionSelection::all();
  EXPECT_TRUE(validate_query_request(file_selection).ok());

  auto semantic = semantic_request();
  EXPECT_TRUE(validate_query_request(semantic).ok());

  semantic.subset->projection.clear();
  EXPECT_EQ(validate_query_request(semantic).code(), QueryError::InvalidQuery);
  semantic = semantic_request();
  semantic.subset->slices.push_back(semantic.subset->slices.front());
  EXPECT_EQ(validate_query_request(semantic).code(), QueryError::InvalidQuery);
}

TEST(QueryRequest, RejectsAmbiguousVersionAndUnsafeLimitConfigurations) {
  QueryRequest request;
  request.sources.versions = VersionSelection::exact_version(-1);
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request.sources.versions = VersionSelection::between(5, 5);
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request.sources.versions = VersionSelection::latest();
  request.limits.max_ranges = 0;
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
}

TEST(QueryRequest, PhysicalArraySubjectRequiresExactSourceAndValidPaths) {
  auto request = semantic_request();
  request.subset->physical_array = PhysicalArraySubject{
    "target", 1, 3, {{{2}}, {{0}}}};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request.sources.paths = {{PathScopeKind::Exact, "/science/a.arrow"}};
  request.sources.versions = VersionSelection::exact_version(7);
  EXPECT_TRUE(validate_query_request(request).ok());

  request.subset->physical_array->values.push_back({{2}});
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request.subset->physical_array->values = {{{}}};
  EXPECT_TRUE(validate_query_request(request).ok());

  request.subset->output.interpolation =
    QueryFragment::GridInterpolation{};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
}

TEST(QueryRequest, PhysicalArrayPathsRespectRangeLimitBeforePathValidation) {
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.arrow"}};
  request.sources.versions = VersionSelection::exact_version(7);
  request.limits.max_ranges = 2;
  request.subset->physical_array = PhysicalArraySubject{
    "target", 0, 0, {{{0}}, {{1}}}};
  EXPECT_TRUE(validate_query_request(request).ok());

  // The third path duplicates an earlier path. Resource validation must run
  // first so an oversized request never builds an unbounded uniqueness set.
  request.subset->physical_array->values.push_back({{0}});
  auto status = validate_query_request(request);
  EXPECT_EQ(status.code(), QueryError::ResourceExhausted);
  EXPECT_EQ(status.phase(), QueryPhase::Validation);
}

TEST(QueryRequest, Hdf5FieldBindingRequiresSingleExactSourceBeforeCatalogPlanning) {
  auto request = hdf5_request();
  auto valid = validate_query_request(request);
  ASSERT_TRUE(valid.ok()) << valid.message();
  EXPECT_TRUE(query_fragment_requires_exact_source(*request.subset));

  request.sources.versions = VersionSelection::latest();
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
  request = hdf5_request();
  request.sources.paths = {{PathScopeKind::Subtree, request.sources.paths.front().path}};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
  request = hdf5_request();
  request.sources.paths = {{PathScopeKind::Exact, request.sources.paths.front().path},
    {PathScopeKind::Exact, "/science/b.h5"}};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
  request = hdf5_request();
  request.sources.paths = {};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
}

TEST(QueryRequest, Hdf5ModelValidationCannotBeBypassedThroughGenericQueryRequest) {
  auto request = hdf5_request();
  request.subset->physical_array = PhysicalArraySubject{"target", 0, 0, {{{}}}};
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request = hdf5_request();
  request.subset->projection = {{"hdf5.value:%ZZ"}};
  EXPECT_FALSE(validate_query_request(request).ok());
  request = hdf5_request();
  request.subset->projection.push_back({"hdf5.value:%2Fb"});
  EXPECT_FALSE(validate_query_request(request).ok());
  request = hdf5_request();
  request.subset->slices.front().lower->value = LogicalValue::int64(1);
  request.subset->slices.front().upper->value = LogicalValue::int64(5);
  EXPECT_FALSE(validate_query_request(request).ok());
  request = hdf5_request();
  request.subset->slices.front().upper->kind = BoundKind::Inclusive;
  EXPECT_FALSE(validate_query_request(request).ok());
}

TEST(QueryRequest, ExactSourceContractDetectionPreservesOtherFormats) {
  auto regular = semantic_request();
  EXPECT_FALSE(query_fragment_requires_exact_source(*regular.subset));
  EXPECT_TRUE(validate_query_request(regular).ok());
  regular.subset->physical_array = PhysicalArraySubject{"target", 0, 0, {{{}}}};
  EXPECT_TRUE(query_fragment_requires_exact_source(*regular.subset));

  regular = semantic_request();
  regular.subset->predicate = make_comparison({"hdf5.axis.0"},
    ComparisonOperator::Equal, LogicalValue::uint64(1));
  EXPECT_TRUE(query_fragment_requires_exact_source(*regular.subset));
  EXPECT_FALSE(validate_query_request(regular).ok());
}

TEST(QueryRequest, ValidatesTypedOutputStructureBeforeAdapterPlanning) {
  auto request = semantic_request();
  request.subset->output.type_overrides = {
    {{"data.value"}, LogicalTypeId::Float32}};
  EXPECT_TRUE(validate_query_request(request).ok());

  request.subset->output.type_overrides.push_back(
    {{"data.value"}, LogicalTypeId::Float64});
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request = semantic_request();
  QueryFragment::GridInterpolation interpolation;
  interpolation.axes = {
    {{"domain.latitude"}, 10, std::nullopt},
    {{"domain.longitude"}, std::nullopt,
      LogicalValue::float64(0.25)},
  };
  request.subset->output.interpolation = interpolation;
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);

  request = semantic_request();
  request.subset->output.mode = QueryFragment::ResultMode::BinaryView;
  EXPECT_EQ(validate_query_request(request).code(), QueryError::InvalidQuery);
}

TEST(FormatQueryAdapter, CapabilityDescriptorMustBeSelfConsistent) {
  FieldQueryCapability value;
  value.field = {"data.value"};
  value.projectable = true;
  value.predicate_operators = {ComparisonOperator::Greater};
  value.predicate_pushdown = PushdownLevel::Candidate;
  QueryCapabilities valid(1, "test-format", "test-1", value_schema(),
    {value}, {DomainKind::TimeSeries}, true, false,
    OrderingGuarantee::Domain);
  EXPECT_TRUE(validate_query_capabilities(valid).ok());

  value.predicate_pushdown = PushdownLevel::None;
  QueryCapabilities residual(1, "test-format", "test-1",
    value_schema(), {value}, {DomainKind::TimeSeries}, true, false,
    OrderingGuarantee::Domain);
  EXPECT_TRUE(validate_query_capabilities(residual).ok());

  value.predicate_operators.clear();
  value.predicate_pushdown = PushdownLevel::Candidate;
  QueryCapabilities inconsistent(1, "test-format", "test-1",
    value_schema(), {value}, {DomainKind::TimeSeries}, true, false,
    OrderingGuarantee::Domain);
  EXPECT_EQ(validate_query_capabilities(inconsistent).code(),
    QueryError::Internal);

  QueryCapabilities unknown_field(1, "test-format", "test-1",
    value_schema(), {{{"data.missing"}, true, {}, PushdownLevel::None,
      false, PushdownLevel::None, {}, false, false, {}}}, {}, true, false,
    OrderingGuarantee::None);
  EXPECT_EQ(validate_query_capabilities(unknown_field).code(),
    QueryError::Internal);
}

TEST(PreparedQuery, PinsPublishedSourcesAndOpaqueAdapterPlans) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 7));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto source = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 7});
  ASSERT_TRUE(source.ok());
  auto adapter = std::make_shared<TestQueryAdapter>("test-format");
  auto plan = adapter->prepare(QueryPlanningContext(*source.value()), *semantic_request().subset);
  ASSERT_TRUE(plan.ok());

  auto prepared = PreparedQuery::create(semantic_request(), value_schema(),
    {{source.value(), adapter, plan.value()}});
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);
  EXPECT_EQ(prepared.value().sources()[0].source->manifest().key.version, 7);
  EXPECT_NE(prepared.value().sources()[0].plan->implementation(), nullptr);
}

namespace {
struct CandidateContextObservation {
  std::vector<std::vector<StructuredBlobManifest>> selections;
  std::vector<StructuredBlobVersionKey> materialized;
  std::weak_ptr<const QueryReadContext> selected;
  bool reject_selection{false};
};
class CandidateReadContext final : public QueryReadContext {
public:
  CandidateReadContext(std::vector<StructuredBlobManifest> manifests,
      std::shared_ptr<CandidateContextObservation> observed, bool selected = false)
    : manifests_(std::move(manifests)), observed_(std::move(observed)), selected_(selected) {
    if (selected_) for (const auto& m : manifests_)
      sources_.push_back({m.key.path, m.key.version, m.format_id.value_or("")});
  }
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>& candidates) const override {
    using Result = QueryResult<std::shared_ptr<const QueryReadContext>>;
    observed_->selections.push_back(candidates);
    if (observed_->reject_selection || selected_)
      return Result::failure(QueryStatus::failure(QueryError::StalePlan,
        QueryPhase::Catalog, "injected candidate context selection failure"));
    std::vector<StructuredBlobManifest> contents;
    for (const auto& candidate : candidates) {
      const auto found = std::find_if(manifests_.begin(), manifests_.end(), [&](const auto& m) {
        return m.key == candidate.key;
      });
      if (found == manifests_.end()) return Result::failure(QueryStatus::failure(
        QueryError::StalePlan, QueryPhase::Catalog, "candidate outside mock snapshot"));
      contents.push_back(*found);
    }
    auto child = std::make_shared<const CandidateReadContext>(std::move(contents), observed_, true);
    observed_->selected = child;
    return Result::success(std::move(child));
  }
  const std::vector<QuerySourceReference>& sources() const override { return sources_; }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal&) const override {
    // The mock deliberately denies a retained context: callers cannot replace
    // it with an independently constructed context containing only output hits.
    return QueryResult<std::shared_ptr<QueryAdmission>>::failure(QueryStatus::failure(
      QueryError::PermissionDenied, QueryPhase::Catalog, "all-candidate mock denial"));
  }
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey& key) const override {
    observed_->materialized.push_back(key);
    const auto found = std::find_if(manifests_.begin(), manifests_.end(), [&](const auto& m) {
      return m.key == key;
    });
    if (!selected_ || found == manifests_.end()) return QueryResult<StructuredBlobManifest>::failure(
      QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog, "unselected context source"));
    return QueryResult<StructuredBlobManifest>::success(*found);
  }
private:
  std::vector<StructuredBlobManifest> manifests_;
  std::shared_ptr<CandidateContextObservation> observed_;
  bool selected_;
  std::vector<QuerySourceReference> sources_;
};
} // namespace

TEST(PreparedQuery, ReadContextRetainsPredicateMissesAndZeroHitCandidates) {
  auto a = manifest("/science/a", 7);
  auto b = manifest("/science/b", 9);
  b.metadata["station"] = MetadataValue("B");
  auto backend = std::make_shared<ReadOnlyBackend>(a);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto observation = std::make_shared<CandidateContextObservation>();
  auto root = std::make_shared<const CandidateReadContext>(
    std::vector<StructuredBlobManifest>{a, b}, observation);
  auto catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::success({{a, b}, {}, root});
  }, storage->storage_namespace());
  auto module = QueryModule::create(catalog, storage, {});
  ASSERT_TRUE(module.ok());
  for (const std::string station : {"A", "no-hit"}) {
    auto request = QueryRequest{};
    request.sources.paths = {{PathScopeKind::Subtree, "/science"}};
    request.sources.versions = VersionSelection::latest();
    request.sources.file_predicate = make_comparison({"sigma.station"},
      ComparisonOperator::Equal, LogicalValue::utf8(station));
    auto prepared = module.value()->prepare(request);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    EXPECT_EQ(prepared.value().sources().size(), station == "A" ? 1u : 0u);
    const auto retained = prepared.value().read_context();
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained, observation->selected.lock());
    ASSERT_EQ(retained->sources().size(), 2u);
    EXPECT_EQ(retained->sources()[0].path, a.key.path);
    EXPECT_EQ(retained->sources()[1].path, b.key.path);
    // Even the predicate miss remains in the admission dependency set.
    auto denied = retained->admit(QueryPrincipal{});
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  }
  ASSERT_EQ(observation->selections.size(), 2u);
  EXPECT_EQ(observation->selections[0].size(), 2u);
  EXPECT_EQ(observation->selections[1].size(), 2u);
  EXPECT_EQ(observation->materialized.size(), 4u);
  EXPECT_EQ(backend->manifest_load_count(), 0u);
}

TEST(PublishedVersionCatalog, ReadContextSelectionFailurePropagatesBeforePreparingSources) {
  const auto source = manifest("/science/a", 7);
  auto observation = std::make_shared<CandidateContextObservation>();
  observation->reject_selection = true;
  auto root = std::make_shared<const CandidateReadContext>(
    std::vector<StructuredBlobManifest>{source}, observation);
  auto catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::success({{source}, {}, root});
  });
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Subtree, "/science"}};
  selector.versions = VersionSelection::latest();
  auto selected = catalog->scan(selector, 10);
  ASSERT_FALSE(selected.ok());
  EXPECT_EQ(selected.status().code(), QueryError::StalePlan);
  EXPECT_EQ(selected.status().message(), "injected candidate context selection failure");
  ASSERT_EQ(observation->selections.size(), 1u);
  EXPECT_TRUE(observation->materialized.empty());
}

TEST(QueryMetadataBudget, ExactBoundaryAndOverflowDoNotWrapOrPartiallyCharge) {
  QueryMetadataBudget budget;
  budget.observe_unique(QueryMetadataBudget::maximum_bytes - 1);
  EXPECT_TRUE(budget.status().ok());
  EXPECT_EQ(budget.consumed_bytes(), QueryMetadataBudget::maximum_bytes - 1);
  budget.observe_unique(1);
  EXPECT_TRUE(budget.status().ok());
  EXPECT_EQ(budget.consumed_bytes(), QueryMetadataBudget::maximum_bytes);
  EXPECT_THROW(budget.observe_unique(1), QueryMetadataBudgetExceeded);
  EXPECT_EQ(budget.consumed_bytes(), QueryMetadataBudget::maximum_bytes);
  EXPECT_EQ(budget.status().code(), QueryError::ResourceExhausted);
  QueryMetadataBudget oversized;
  EXPECT_THROW(oversized.observe_unique(QueryMetadataBudget::maximum_bytes + 1), QueryMetadataBudgetExceeded);
  EXPECT_EQ(oversized.consumed_bytes(), 0u);
}

TEST(QueryMetadataBudget, CatchingFailureCannotResumeEvenWithCachedOrSmallerEvidence) {
  QueryMetadataBudget budget;
  budget.observe("ns/path@1/shared-layout", QueryMetadataBudget::maximum_bytes);
  EXPECT_THROW(budget.observe("ns/path@2/shared-layout", 1), QueryMetadataBudgetExceeded);
  // A failed request cannot recover by catching the exception and requesting
  // already charged, smaller or zero-byte evidence.
  EXPECT_THROW(budget.observe("ns/path@1/shared-layout", 0), QueryMetadataBudgetExceeded);
  EXPECT_THROW(budget.observe_unique(0), QueryMetadataBudgetExceeded);
  EXPECT_EQ(budget.status(QueryPhase::Planning).code(), QueryError::ResourceExhausted);
  EXPECT_EQ(budget.status(QueryPhase::Planning).phase(), QueryPhase::Planning);
  EXPECT_EQ(budget.consumed_bytes(), QueryMetadataBudget::maximum_bytes);
}

TEST(QueryMetadataBudget, RepeatedExactIdentityIsIdempotentButSharedContentAcrossVersionsIsNot) {
  QueryMetadataBudget budget;
  budget.observe("ns/path@1/shared-layout", 1024);
  budget.observe("ns/path@1/shared-layout", 1024);
  budget.observe("ns/path@1/shared-layout", 512);
  EXPECT_EQ(budget.consumed_bytes(), 1024u);
  budget.observe("ns/path@1/shared-layout", 1536);
  EXPECT_EQ(budget.consumed_bytes(), 1536u);
  budget.observe("ns/path@2/shared-layout", 1536);
  EXPECT_EQ(budget.consumed_bytes(), 3072u);
  budget.observe("ns/other-path@1/shared-layout", 1536);
  EXPECT_EQ(budget.consumed_bytes(), 4608u);
}

TEST(QueryMetadataBudget, NestedScopesReuseOneRequestAndExplicitIndependentScopeRestoresParent) {
  ASSERT_FALSE(current_query_metadata_budget());
  std::shared_ptr<QueryMetadataBudget> original;
  {
    ScopedQueryMetadataBudget outer;
    original = outer.budget();
    require_query_metadata("source-A", 10);
    {
      ScopedQueryMetadataBudget nested;
      EXPECT_EQ(nested.budget(), original);
      require_query_metadata("source-B", 20);
      EXPECT_EQ(original->consumed_bytes(), 30u);
    }
    EXPECT_EQ(current_query_metadata_budget(), original);
    {
      const auto independent = std::make_shared<QueryMetadataBudget>();
      ScopedQueryMetadataBudget nested(independent);
      EXPECT_NE(nested.budget(), original);
      require_query_metadata("source-C", 40);
      EXPECT_EQ(independent->consumed_bytes(), 40u);
      EXPECT_EQ(original->consumed_bytes(), 30u);
    }
    EXPECT_EQ(current_query_metadata_budget(), original);
  }
  EXPECT_FALSE(current_query_metadata_budget());
  ScopedQueryMetadataBudget next_request;
  EXPECT_NE(next_request.budget(), original);
  EXPECT_EQ(next_request.budget()->consumed_bytes(), 0u);
}

TEST(QueryMetadataBudget, WorkersExplicitlyShareRequestLedgerAndRestoreThreadLocalState) {
  ScopedQueryMetadataBudget request;
  const auto shared = request.budget();
  std::promise<void> start;
  auto ready = start.get_future().share();
  std::vector<std::future<void>> workers;
  for (int index = 0; index < 4; ++index) {
    workers.push_back(std::async(std::launch::async, [shared, ready, index] {
      EXPECT_FALSE(current_query_metadata_budget());
      ready.wait();
      {
        ScopedQueryMetadataBudget worker(shared);
        for (int iteration = 0; iteration < 10; ++iteration) {
          require_query_metadata("same-source/same-content", 128);
          require_query_metadata("source-" + std::to_string(index) + "/shared-content", 256);
        }
      }
      EXPECT_FALSE(current_query_metadata_budget());
    }));
  }
  start.set_value();
  for (auto& worker : workers) worker.get();
  EXPECT_EQ(shared->consumed_bytes(), 128u + 4u * 256u);
  EXPECT_TRUE(request.status().ok());
}

TEST(QueryMetadataBudget, WarmIdentityReuseDoesNotExemptAnotherSourceFromLogicalLimit) {
  ScopedQueryMetadataBudget request;
  require_query_metadata("source-A/shared-content", QueryMetadataBudget::maximum_bytes - 64);
  // Repeated cache hits within the same exact source are the same demand.
  require_query_metadata("source-A/shared-content", QueryMetadataBudget::maximum_bytes - 64);
  require_query_metadata("source-B/shared-content", 64);
  EXPECT_TRUE(request.status().ok());
  EXPECT_THROW(require_query_metadata("source-C/shared-content", 1), QueryMetadataBudgetExceeded);
  EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(request.budget()->consumed_bytes(), QueryMetadataBudget::maximum_bytes);
}

TEST(PreparedQuery, RejectsMissingOrMismatchedAdaptersWithoutAffectingBaseline) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 7));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto source = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 7});
  ASSERT_TRUE(source.ok());

  auto missing = PreparedQuery::create(semantic_request(), value_schema(),
    {{source.value(), nullptr, std::nullopt}});
  EXPECT_EQ(missing.status().code(), QueryError::AdapterUnavailable);

  auto wrong_adapter = std::make_shared<TestQueryAdapter>("other-format");
  auto wrong_plan = wrong_adapter->prepare(QueryPlanningContext(*source.value()),
    *semantic_request().subset);
  ASSERT_TRUE(wrong_plan.ok());
  auto mismatched = PreparedQuery::create(semantic_request(), value_schema(),
    {{source.value(), wrong_adapter, wrong_plan.value()}});
  EXPECT_EQ(mismatched.status().code(), QueryError::AdapterUnavailable);

  QueryRequest baseline;
  baseline.sources.versions = VersionSelection::exact_version(7);
  auto file_query = PreparedQuery::create(std::move(baseline), std::nullopt,
    {{source.value(), nullptr, std::nullopt}});
  EXPECT_TRUE(file_query.ok()) << file_query.status().message();
}

TEST(PreparedQuery, RejectsInvalidResidualAndFinalProjectionContracts) {
  auto backend = std::make_shared<ReadOnlyBackend>(
    manifest("/science/a.tsfile", 7));
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto source = open_structured_blob_query_source(storage,
    {"/science/a.tsfile", 7});
  ASSERT_TRUE(source.ok());
  auto adapter = std::make_shared<TestQueryAdapter>("test-format");

  AdapterPlan bad_residual(std::make_shared<const TestExecutionPlan>(),
    value_schema(), make_comparison({"data.missing"},
      ComparisonOperator::Equal, LogicalValue::float64(1.0)), {},
    {{"data.value"}});
  auto residual = PreparedQuery::create(semantic_request(), value_schema(),
    {{source.value(), adapter, std::move(bad_residual)}});
  EXPECT_EQ(residual.status().code(), QueryError::UnknownField);

  AdapterPlan bad_projection(std::make_shared<const TestExecutionPlan>(),
    value_schema(), nullptr, {}, {{"data.missing"}});
  auto projection = PreparedQuery::create(semantic_request(), value_schema(),
    {{source.value(), adapter, std::move(bad_projection)}});
  EXPECT_EQ(projection.status().code(), QueryError::SchemaMismatch);
}

TEST(PathScopeSet, CopiesShareCompiledIdentityAndReplacementCannotMutateFrozenCopy) {
  std::vector<PathScope> input{{PathScopeKind::Exact, "science/b"},
    {PathScopeKind::Subtree, "/science/a"}};
  SourceSelector original;
  original.paths = input;
  auto frozen = original;
  EXPECT_EQ(&original.paths.scopes(), &frozen.paths.scopes());
  EXPECT_EQ(&original.paths.normalized_paths(), &frozen.paths.normalized_paths());
  EXPECT_EQ(&original.paths.identity_key(), &frozen.paths.identity_key());
  input.front().path = "/outside";
  EXPECT_EQ(original.paths.front().path, "science/b");
  auto changed = original.paths.scopes();
  changed.front().path = "/outside";
  original.paths = std::move(changed);
  EXPECT_NE(original.paths.identity_key(), frozen.paths.identity_key());
  EXPECT_TRUE(frozen.paths.matches_normalized_path("/science/b"));
  EXPECT_FALSE(original.paths.matches_normalized_path("/science/b"));
  EXPECT_EQ(frozen.paths.front().path, "science/b");
  static_assert(std::is_const_v<std::remove_reference_t<
    decltype(std::declval<PathScopeSet&>().front())>>);
  static_assert(std::is_const_v<std::remove_reference_t<
    decltype(std::declval<PathScopeSet&>()[0])>>);
}

TEST(PathScopeSet, CompiledLifetimeAndIdentityPreserveNormalizationAndScopeKinds) {
  const auto frozen = [] {
    SourceSelector request;
    request.paths = {{PathScopeKind::Subtree, "science/a"},
      {PathScopeKind::Exact, "/science/b"}};
    return request;
  }();
  SourceSelector reordered;
  reordered.paths = {{PathScopeKind::Exact, "science/b"},
    {PathScopeKind::Subtree, "/science/a"}};
  EXPECT_EQ(frozen.paths.identity_key(), reordered.paths.identity_key());
  EXPECT_EQ(frozen.paths.identity_key(), "/paths:2:010:/science/b:110:/science/a");
  EXPECT_TRUE(frozen.paths.matches_normalized_path("/science/a"));
  EXPECT_TRUE(frozen.paths.matches_normalized_path("/science/a/child"));
  EXPECT_TRUE(frozen.paths.matches_normalized_path("/science/b"));
  EXPECT_FALSE(frozen.paths.matches_normalized_path("/science/ab"));
  EXPECT_FALSE(frozen.paths.matches_normalized_path("/science/b/child"));
  reordered.paths = {{PathScopeKind::Subtree, "/science/b"},
    {PathScopeKind::Subtree, "/science/a"}};
  EXPECT_NE(frozen.paths.identity_key(), reordered.paths.identity_key());
}

TEST(PathScopeSet, ReplacementRecompilesValidationAndVersionErrorsRemainFirst) {
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, "science/a"},
    {PathScopeKind::Exact, "/science/a"}};
  const auto frozen = selector;
  EXPECT_EQ(validate_source_selector(selector).message(), "path scopes must be unique");
  selector.versions = VersionSelection::exact_version(-1);
  EXPECT_EQ(validate_source_selector(selector).message(),
    "exact version selection requires one non-negative version");
  selector.paths = {{PathScopeKind::Exact, "/science/a"}};
  selector.versions = VersionSelection::latest();
  EXPECT_TRUE(validate_source_selector(selector).ok());
  EXPECT_EQ(validate_source_selector(frozen).message(), "path scopes must be unique");
  selector.paths = {{PathScopeKind::Exact, std::string("/a\0b", 4)}};
  EXPECT_EQ(validate_source_selector(selector).message(), "path scope cannot contain a NUL byte");
  selector.paths = {{PathScopeKind::Exact, ""}};
  EXPECT_EQ(validate_source_selector(selector).message(), "path scope cannot be empty");
  selector.paths = {{static_cast<PathScopeKind>(99), "/a"}};
  EXPECT_EQ(validate_source_selector(selector).message(), "unknown path scope kind");
}
