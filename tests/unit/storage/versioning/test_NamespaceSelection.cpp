#include "iotdbfs/storage/versioning/NamespaceSelection.h"
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <vector>

namespace iotdbfs::versioning {
namespace {

// Actual Owner tree/journal operations, without SDK, content or service mocks.
class SelectionMemoryStore final : public Store {
public:
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  std::optional<Bytes> head;
  std::map<Key, Bytes> objects;
  std::size_t catalog_record_reads{0};
  static Key key(const NamespaceIdentity& ns, const ObjectRef& ref) {
    return {ns.namespace_id, ns.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override { return head; }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override {
    if (ref.type == "catalog-record") ++catalog_record_reads;
    const auto found = objects.find(key(ns, ref));
    if (found == objects.end()) throw Error(ErrorCode::Missing, "missing fixture object");
    return found->second;
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    std::vector<Bytes> result;
    for (const auto& request : requests) {
      auto value = get_object(ns, request.ref);
      if (value.size() != request.size) throw Error(ErrorCode::Corrupt, "fixture object size");
      result.push_back(std::move(value));
    }
    return result;
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& data) override {
    if (content_id(ref.type, ref.protocol, data) != ref.id)
      throw Error(ErrorCode::Corrupt, "fixture object identity");
    const auto inserted = objects.emplace(key(ns, ref), data);
    if (!inserted.second && inserted.first->second != data)
      throw Error(ErrorCode::Conflict, "fixture immutable replacement");
  }
  HeadWriteResult write_head(const Bytes& value) override {
    head = value;
    return HeadWriteResult::Acknowledged;
  }
};

class SelectionMemoryJournal final : public Journal {
public:
  std::optional<JournalState> state;
  std::optional<JournalState> load() override { return state; }
  void save(const JournalState& next) override { state = next; }
};

NamespaceRecord file_record(const std::string& path, std::int64_t version,
    std::optional<std::string> format = std::string("hdf5")) {
  NamespaceRecord record;
  auto& inode = record.inode;
  inode.path = path;
  inode.parent = path_utils::parent_of(path);
  inode.name = path_utils::base(path);
  inode.row_time = version;
  inode.size = format ? 16 : 0;
  inode.mode = 0640;
  inode.uid = 1001;
  inode.gid = 40077;
  inode.nlink = 1;
  inode.ctime = inode.mtime = inode.atime = 1;
  inode.format_id = format.value_or("");
  inode.metadata = "{}";
  record.manifest.key = {path, version};
  record.manifest.format_id = std::move(format);
  record.manifest.payload_size = inode.size;
  record.manifest.layout_size = record.manifest.format_id ? 8 : 0;
  record.manifest.revision = 3;
  record.manifest.published = true;
  record.manifest.phase = StructuredBlobPhase::Published;
  record.descriptor_id = std::string(64, 'a');
  record.seal_id = std::string(64, 'b');
  // Encoding validates the same namespace-record/3 grammar as production.
  (void)encode_namespace_record(record);
  return record;
}

NamespaceRecord directory_record(const std::string& path, std::int64_t version, int mode = 0755) {
  auto record = file_record(path, version, std::nullopt);
  record.inode.is_dir = true;
  record.inode.mode = mode;
  record.descriptor_id.clear();
  record.seal_id.clear();
  (void)encode_namespace_record(record);
  return record;
}

std::string format_key(const NamespaceRecord& record) {
  return record.inode.path + std::string(1, '\0') + record.inode.format_id;
}

class NamespaceSelectionTest : public ::testing::Test {
protected:
  std::shared_ptr<SelectionMemoryStore> store = std::make_shared<SelectionMemoryStore>();
  std::shared_ptr<SelectionMemoryJournal> journal = std::make_shared<SelectionMemoryJournal>();
  Owner owner{store, journal};
  std::map<StructuredBlobVersionKey, NamespaceRecord> records;
  std::set<std::string> current_keys;
  struct Observed { Table table; std::string key; Bytes bytes; };
  std::vector<Observed> observed;

  std::map<std::string, NamespaceRecord> current_records() const {
    std::map<std::string, NamespaceRecord> current;
    for (const auto& item : records) {
      const auto& record = item.second;
      for (const auto& key : {record.inode.path, format_key(record)}) {
        const auto before = current.find(key);
        if (before == current.end() || before->second.inode.row_time < record.inode.row_time)
          current[key] = record;
      }
    }
    return current;
  }

  void initialize(std::vector<NamespaceRecord> entries) {
    entries.push_back(directory_record("/", 1));
    entries.push_back(directory_record("/science", 2));
    for (auto& record : entries) records[record.manifest.key] = std::move(record);
    std::vector<Owner::Seed> seeds;
    for (const auto& item : records) {
      const auto key = exact_key(item.first.path, item.first.version);
      const auto encoded = encode_namespace_record(item.second);
      seeds.emplace_back(Table::Exact, key, encoded);
      seeds.emplace_back(Table::Permission, key, encoded);
      seeds.emplace_back(Table::Liveness, key, Bytes{1});
    }
    for (const auto& item : current_records()) {
      current_keys.insert(item.first);
      seeds.emplace_back(Table::Current, item.first, encode_namespace_record(item.second));
    }
    owner.initialize({"123e4567-e89b-42d3-a456-426614174000",
                      "123e4567-e89b-42d3-a456-426614174001"}, seeds);
  }

  void commit(std::vector<NamespaceRecord> updates,
      const std::vector<StructuredBlobVersionKey>& removals = {}) {
    auto transaction = owner.begin("namespace-selection-fixture");
    for (const auto& key : removals) {
      records.erase(key);
      transaction->erase(Table::Exact, exact_key(key.path, key.version));
      transaction->erase(Table::Permission, exact_key(key.path, key.version));
      transaction->put(Table::Liveness, exact_key(key.path, key.version), Bytes{0});
    }
    for (const auto& record : updates) {
      records[record.manifest.key] = record;
      const auto key = exact_key(record.manifest.key.path, record.manifest.key.version);
      const auto encoded = encode_namespace_record(record);
      transaction->put(Table::Exact, key, encoded);
      transaction->put(Table::Permission, key, encoded);
      transaction->put(Table::Liveness, key, Bytes{1});
    }
    for (const auto& key : current_keys) transaction->erase(Table::Current, key);
    current_keys.clear();
    for (const auto& item : current_records()) {
      current_keys.insert(item.first);
      transaction->put(Table::Current, item.first, encode_namespace_record(item.second));
    }
    transaction->commit();
  }

  NamespaceSelection select(const SourceSelector& selector, const View& view) {
    observed.clear();
    store->catalog_record_reads = 0;
    return select_namespace(owner, view, selector,
      [&](Table table, const std::string& key, const Bytes& bytes) {
        observed.push_back({table, key, bytes});
      });
  }
  NamespaceSelection select(const SourceSelector& selector) { return select(selector, owner.view()); }

  std::map<StructuredBlobVersionKey, NamespaceRecord> parsed_exact(const View& view) {
    std::map<StructuredBlobVersionKey, NamespaceRecord> parsed;
    for (const auto& item : owner.scan(view, Table::Exact)) {
      auto record = decode_namespace_record(item.second);
      EXPECT_EQ(item.first, exact_key(record.inode.path, record.inode.row_time));
      if (!record.inode.is_dir) {
        record.inode.metadata = "{}";
        record.manifest.metadata_materialized = false;
      }
      parsed.emplace(record.manifest.key, std::move(record));
    }
    return parsed;
  }

  NamespaceSelection project(const SourceSelector& selector, const View& view,
      const std::map<StructuredBlobVersionKey, NamespaceRecord>& parsed) {
    observed.clear();
    return project_namespace(owner, view, selector, parsed,
      [&](Table table, const std::string& key, const Bytes& bytes) {
        observed.push_back({table, key, bytes});
      });
  }

  static SourceSelector hdf5(const std::string& path, PathScopeKind kind = PathScopeKind::Exact) {
    SourceSelector selector;
    selector.paths = {{kind, path}};
    selector.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
    return selector;
  }

  static std::set<StructuredBlobVersionKey> keys(const NamespaceSelection& selected) {
    std::set<StructuredBlobVersionKey> result;
    for (const auto& item : selected.sources) result.insert(item.first);
    return result;
  }

  void expect_reference(const SourceSelector& selector) {
    std::vector<StructuredBlobManifest> manifests;
    for (const auto& item : records)
      if (!item.second.inode.is_dir) manifests.push_back(item.second.manifest);
    auto catalog = make_in_memory_published_version_catalog(std::move(manifests));
    const auto expected = catalog->scan(selector, kScanEntries);
    ASSERT_TRUE(expected.ok()) << expected.status().message();
    const auto actual = select(selector);
    std::set<StructuredBlobVersionKey> expected_keys;
    for (const auto& manifest : expected.value().versions()) {
      expected_keys.insert(manifest.key);
      const auto found = actual.sources.find(manifest.key);
      ASSERT_NE(found, actual.sources.end());
      EXPECT_EQ(found->second.manifest.format_id, manifest.format_id);
      EXPECT_EQ(found->second.manifest.revision, manifest.revision);
      EXPECT_EQ(found->second.manifest.payload_size, manifest.payload_size);
      EXPECT_EQ(found->second.manifest.layout_size, manifest.layout_size);
      EXPECT_FALSE(found->second.manifest.metadata_materialized);
      EXPECT_EQ(found->second.inode.metadata, "{}");
    }
    EXPECT_EQ(keys(actual), expected_keys);
    EXPECT_EQ(expected.value().residual_file_predicate, selector.file_predicate);
  }
};

TEST_F(NamespaceSelectionTest, LatestIsPerPathAndFormatBeforePredicate) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/science/a", 30, std::string("grib")),
    file_record("/science/b", 11), file_record("/science/b", 21, std::nullopt)});
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  selector.file_predicate = make_comparison({"sigma.nonexistent"},
    ComparisonOperator::Equal, LogicalValue::utf8("no-match"));
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), (std::set<StructuredBlobVersionKey>{{"/science/a", 20}, {"/science/b", 11}}));
  selector.formats = {AdapterSelectionKind::All, {}, false};
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), (std::set<StructuredBlobVersionKey>{{"/science/a", 30}, {"/science/b", 11}}));
}

TEST_F(NamespaceSelectionTest, EmptyByteVersionWinsAllFormatsButNotRecognizedFormatFilter) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20, std::nullopt)});
  SourceSelector selector;
  selector.paths = {{PathScopeKind::Exact, "/science/a"}};
  expect_reference(selector);
  auto latest = select(selector);
  ASSERT_EQ(latest.sources.size(), 1u);
  EXPECT_EQ(latest.sources.begin()->first.version, 20);
  EXPECT_FALSE(latest.sources.begin()->second.manifest.format_id);
  EXPECT_EQ(latest.sources.begin()->second.manifest.payload_size, 0u);
  expect_reference(hdf5("/science/a"));
  selector.formats = {AdapterSelectionKind::Listed, {}, true};
  expect_reference(selector);
  EXPECT_EQ(select(selector).sources.begin()->first.version, 20);
  selector.formats.include_byte_passthrough = false;
  expect_reference(selector);
  EXPECT_TRUE(select(selector).sources.empty());
}

TEST_F(NamespaceSelectionTest, ExactTimestampAppliesToEveryRequestedLogicalPath) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/science/b", 10), file_record("/science/b", 21)});
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  selector.versions = VersionSelection::exact_version(10);
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), (std::set<StructuredBlobVersionKey>{{"/science/a", 10}, {"/science/b", 10}}));
}

TEST_F(NamespaceSelectionTest, BetweenUsesHalfOpenActualVersionBounds) {
  initialize({file_record("/science/a", 9), file_record("/science/a", 10),
    file_record("/science/a", 19), file_record("/science/a", 20), file_record("/science/b", 10)});
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  selector.versions = VersionSelection::between(10, 20);
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), (std::set<StructuredBlobVersionKey>{{"/science/a", 10}, {"/science/a", 19}, {"/science/b", 10}}));
}

TEST_F(NamespaceSelectionTest, OverlappingNormalizedScopesDeduplicateWithoutPrefixLeakage) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/science/a/child", 11), file_record("/science/ab", 12),
    file_record("/science2/other", 13), file_record("/elsewhere/other", 14)});
  auto selector = hdf5("science/a", PathScopeKind::Subtree);
  selector.paths = {{PathScopeKind::Subtree, "science/a"},
    {PathScopeKind::Exact, "/science/a"}, {PathScopeKind::Exact, "/science/a/child"}};
  selector.versions = VersionSelection::all();
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), (std::set<StructuredBlobVersionKey>{{"/science/a", 10}, {"/science/a", 20}, {"/science/a/child", 11}}));
  selector.paths = {{PathScopeKind::Subtree, "/science"}};
  expect_reference(selector);
  EXPECT_EQ(select(selector).sources.size(), 4u);
}

TEST_F(NamespaceSelectionTest, EmptyPathScopesAndRootSubtreeMatchSamePublishedFiles) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/elsewhere/b", 11, std::string("grib")), directory_record("/empty", 9)});
  SourceSelector selector;
  selector.versions = VersionSelection::all();
  expect_reference(selector);
  const auto all = keys(select(selector));
  selector.paths = {{PathScopeKind::Subtree, "/"}};
  expect_reference(selector);
  EXPECT_EQ(keys(select(selector)), all);
  EXPECT_EQ(all.size(), 3u);
}

TEST_F(NamespaceSelectionTest, RemovingLatestRevealsOlderAndDoesNotResurrectDeletedExact) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20), file_record("/science/a", 30)});
  auto selector = hdf5("/science/a");
  const auto before = owner.view();
  commit({}, {{"/science/a", 30}});
  expect_reference(selector);
  EXPECT_EQ(select(selector).sources.begin()->first.version, 20);
  EXPECT_EQ(select(selector, before).sources.begin()->first.version, 30);
  selector.versions = VersionSelection::exact_version(30);
  expect_reference(selector);
  EXPECT_TRUE(select(selector).sources.empty());
  ASSERT_TRUE(owner.get(owner.view(), Table::Liveness, exact_key("/science/a", 30)));
  EXPECT_EQ(*owner.get(owner.view(), Table::Liveness, exact_key("/science/a", 30)), Bytes{0});
}

TEST_F(NamespaceSelectionTest, CapturedViewDoesNotRebindAfterNewLatestPublication) {
  initialize({file_record("/science/a", 10)});
  const auto before = owner.view();
  const auto selector = hdf5("/science/a");
  commit({file_record("/science/a", 20)});
  EXPECT_EQ(select(selector, before).sources.begin()->first.version, 10);
  EXPECT_EQ(select(selector).sources.begin()->first.version, 20);
  auto exact = selector;
  exact.versions = VersionSelection::exact_version(10);
  EXPECT_EQ(select(exact).sources.begin()->first.version, 10);
}

TEST_F(NamespaceSelectionTest, HistoricalFileUsesCurrentAncestorsOutsideVersionAndFormatScope) {
  initialize({file_record("/science/a", 10), directory_record("/unrelated", 3)});
  commit({directory_record("/science", 100, 0600)});
  auto selector = hdf5("/science/a");
  selector.versions = VersionSelection::exact_version(10);
  const auto result = select(selector);
  ASSERT_EQ(result.sources.size(), 1u);
  ASSERT_TRUE(result.parents.at("/science"));
  EXPECT_EQ(result.parents.at("/science")->inode.row_time, 100);
  EXPECT_EQ(result.parents.at("/science")->inode.mode, 0600);
  ASSERT_TRUE(result.parents.at("/"));
  EXPECT_EQ(result.parents.size(), 2u);
  EXPECT_EQ(result.parents.count("/unrelated"), 0u);
}

TEST_F(NamespaceSelectionTest, MissingAndNonDirectoryCurrentAncestorsNeverFallBack) {
  initialize({file_record("/missing/a", 10), file_record("/science/a", 11)});
  auto missing = select(hdf5("/missing/a"));
  ASSERT_EQ(missing.parents.count("/missing"), 1u);
  EXPECT_FALSE(missing.parents.at("/missing"));
  commit({file_record("/science", 100, std::string("grib"))});
  const auto parent_is_file = select(hdf5("/science/a"));
  ASSERT_TRUE(parent_is_file.parents.at("/science"));
  EXPECT_FALSE(parent_is_file.parents.at("/science")->inode.is_dir);
  EXPECT_EQ(parent_is_file.parents.at("/science")->inode.row_time, 100);
}

TEST_F(NamespaceSelectionTest, ExactColdReadDoesNotEnumerateOtherFilesOrHistory) {
  std::vector<NamespaceRecord> entries;
  for (int version = 1; version <= 128; ++version) entries.push_back(file_record("/science/a", version));
  for (int file = 0; file < 128; ++file) entries.push_back(file_record("/elsewhere/f" + std::to_string(file), 9));
  initialize(std::move(entries));
  auto selector = hdf5("/science/a");
  selector.versions = VersionSelection::exact_version(17);
  owner.clear_read_cache();
  const auto result = select(selector);
  ASSERT_EQ(result.sources.size(), 1u);
  EXPECT_EQ(result.sources.begin()->first.version, 17);
  std::set<std::string> exact_observations;
  for (const auto& item : observed) {
    if (item.table == Table::Exact) exact_observations.insert(item.key);
    const auto record = decode_namespace_record(item.bytes);
    EXPECT_TRUE(record.inode.path == "/science/a" || record.inode.path == "/science" || record.inode.path == "/");
  }
  EXPECT_EQ(exact_observations, (std::set<std::string>{exact_key("/science/a", 17),
    exact_key("/science", 2), exact_key("/", 1)}));
  EXPECT_LE(store->catalog_record_reads, 3u);
  EXPECT_GT(store->catalog_record_reads, 0u);
}

TEST_F(NamespaceSelectionTest, LatestColdReadUsesCurrentIndexInsteadOfAllExactVersions) {
  std::vector<NamespaceRecord> entries;
  for (int version = 1; version <= 128; ++version) entries.push_back(file_record("/science/a", version));
  for (int file = 0; file < 64; ++file) entries.push_back(file_record("/elsewhere/f" + std::to_string(file), 9));
  initialize(std::move(entries));
  owner.clear_read_cache();
  const auto result = select(hdf5("/science/a"));
  ASSERT_EQ(result.sources.size(), 1u);
  EXPECT_EQ(result.sources.begin()->first.version, 128);
  std::set<std::string> exact_observations;
  for (const auto& item : observed) {
    if (item.table == Table::Exact) exact_observations.insert(item.key);
    const auto record = decode_namespace_record(item.bytes);
    EXPECT_TRUE(record.inode.path == "/science/a" || record.inode.path == "/science" || record.inode.path == "/");
    if (!record.inode.is_dir) EXPECT_EQ(record.inode.row_time, 128);
  }
  EXPECT_EQ(exact_observations, (std::set<std::string>{exact_key("/science/a", 128),
    exact_key("/science", 2), exact_key("/", 1)}));
  EXPECT_LE(store->catalog_record_reads, 3u);
  EXPECT_GT(store->catalog_record_reads, 0u);
}

TEST_F(NamespaceSelectionTest, InvalidSelectorFailsBeforeAnyObservedRecordRead) {
  initialize({file_record("/science/a", 10)});
  auto selector = hdf5("/science/a");
  selector.paths = {{selector.paths.front().kind, selector.paths.front().path},
    {PathScopeKind::Exact, "science/a"}};
  try {
    (void)select(selector);
    FAIL() << "normalized duplicate scopes accepted";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::Invalid);
  }
  EXPECT_TRUE(observed.empty());
  EXPECT_EQ(store->catalog_record_reads, 0u);
}

TEST_F(NamespaceSelectionTest, ExhaustedRequestBudgetRemainsStickyForEmptyFormatSelection) {
  initialize({file_record("/science/a", 10)});
  const auto view = owner.view();
  auto selector = hdf5("/science/a");
  selector.formats = {AdapterSelectionKind::Listed, {}, false};
  ScopedQueryMetadataBudget budget;
  EXPECT_THROW(require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1), QueryMetadataBudgetExceeded);
  EXPECT_THROW((void)select(selector, view), QueryMetadataBudgetExceeded);
  EXPECT_EQ(budget.status().code(), QueryError::ResourceExhausted);
}

TEST_F(NamespaceSelectionTest, LatestRejectsRecoverableCurrentOrphanWithoutOlderFallback) {
  initialize({file_record("/science/a", 5), file_record("/science/a", 10)});
  auto transaction = owner.begin("fixture-orphan-current");
  const auto key = exact_key("/science/a", 10);
  transaction->erase(Table::Exact, key);
  transaction->erase(Table::Permission, key);
  transaction->put(Table::Liveness, key, Bytes{0});
  // Leave both legitimate Current indexes pointing at the deleted record.
  transaction->commit();
  ASSERT_NO_THROW(owner.recover());  // Object closure/hash validity alone is insufficient.
  for (bool all_formats : {false, true}) {
    auto selector = hdf5("/science/a");
    if (all_formats) selector.formats = {AdapterSelectionKind::All, {}, true};
    try {
      (void)select(selector);
      FAIL() << "Current orphan was returned, ignored, or rebound to surviving version 5";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
  }
}

TEST_F(NamespaceSelectionTest, LatestRejectsEveryCanonicalCurrentSourceMismatch) {
  auto original = file_record("/science/a", 10);
  original.inode.mode = 0000;
  initialize({file_record("/science/a", 5), original});
  const std::vector<std::pair<std::string, std::function<void(NamespaceRecord&)>>> changes{
    {"mode", [](auto& record) { record.inode.mode = 0644; }},
    {"uid", [](auto& record) { record.inode.uid = 1002; }},
    {"gid", [](auto& record) { record.inode.gid = 40078; }},
    {"revision", [](auto& record) { ++record.manifest.revision; }},
    {"descriptor", [](auto& record) { record.descriptor_id = std::string(64, 'c'); }},
    {"seal", [](auto& record) { record.seal_id = std::string(64, 'd'); }},
    {"mtime", [](auto& record) { ++record.inode.mtime; }},
  };
  for (const auto& change : changes) {
    SCOPED_TRACE(change.first);
    auto forged = original;
    change.second(forged);
    const auto encoded = encode_namespace_record(forged);
    auto transaction = owner.begin("fixture-mismatched-current");
    transaction->put(Table::Current, original.inode.path, encoded);
    transaction->put(Table::Current, format_key(original), encoded);
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    try {
      (void)select(hdf5("/science/a"));
      FAIL() << "Current mismatch was returned or silently fell back";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    const auto exact = std::find_if(observed.begin(), observed.end(), [&](const auto& item) {
      return item.table == Table::Exact && item.key == exact_key("/science/a", 10);
    });
    ASSERT_NE(exact, observed.end());
    EXPECT_EQ(decode_namespace_record(exact->bytes).inode.mode, 0000);
  }
}

TEST_F(NamespaceSelectionTest, CurrentAncestorOrphanRejectsLatestAndExactSourceSelection) {
  initialize({file_record("/science/a", 10)});
  auto transaction = owner.begin("fixture-orphan-current-parent");
  transaction->erase(Table::Exact, exact_key("/science", 2));
  transaction->commit();
  ASSERT_NO_THROW(owner.recover());
  for (bool exact : {false, true}) {
    auto selector = hdf5("/science/a");
    if (exact) selector.versions = VersionSelection::exact_version(10);
    try {
      (void)select(selector);
      FAIL() << "Current ancestor without Exact membership was accepted";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
  }
}

TEST_F(NamespaceSelectionTest, CurrentAncestorMismatchCannotReplaceExactPermissionOrIdentity) {
  initialize({file_record("/science/a", 10)});
  auto original = directory_record("/science", 100, 0000);
  commit({original});
  const std::vector<std::pair<std::string, std::function<void(NamespaceRecord&)>>> changes{
    {"mode", [](auto& record) { record.inode.mode = 0755; }},
    {"uid", [](auto& record) { record.inode.uid = 1002; }},
    {"revision", [](auto& record) { ++record.manifest.revision; }},
  };
  for (const auto& change : changes) {
    SCOPED_TRACE(change.first);
    auto forged = original;
    change.second(forged);
    auto transaction = owner.begin("fixture-mismatched-current-parent");
    transaction->put(Table::Current, original.inode.path, encode_namespace_record(forged));
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    try {
      (void)select(hdf5("/science/a"));
      FAIL() << "Current ancestor mismatch was accepted or fell back to old parent";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    const auto exact = std::find_if(observed.begin(), observed.end(), [&](const auto& item) {
      return item.table == Table::Exact && item.key == exact_key("/science", 100);
    });
    ASSERT_NE(exact, observed.end());
    EXPECT_EQ(decode_namespace_record(exact->bytes).inode.mode, 0000);
  }
}

TEST_F(NamespaceSelectionTest, StartupCurrentValidationAcceptsFormatsBytesAndVerifiedProfileWithoutRereadingExact) {
  std::vector<NamespaceRecord> entries;
  for (int version = 1; version <= 64; ++version) entries.push_back(file_record("/science/a", version));
  entries.push_back(file_record("/science/a", 70, std::string("grib")));
  entries.push_back(file_record("/science/a", 80, std::nullopt));
  entries.push_back(file_record("/science/b", 12));
  initialize(std::move(entries));
  auto transaction = owner.begin("fixture-supported-profile");
  transaction->put(Table::Current, content_profile_key(), content_profile());
  transaction->commit();
  const auto view = owner.view();
  const auto exact = owner.scan(view, Table::Exact);
  std::set<Bytes> current_values{content_profile()};
  for (const auto& item : current_records()) current_values.insert(encode_namespace_record(item.second));
  owner.clear_read_cache();
  store->catalog_record_reads = 0;
  EXPECT_NO_THROW(validate_current_index(owner, view, exact));
  // The caller already supplied Exact closure bytes. Only Current's distinct
  // values (including the caller-verified profile) require object reads here.
  EXPECT_EQ(store->catalog_record_reads, current_values.size());
  EXPECT_LT(store->catalog_record_reads, exact.size());
}

TEST_F(NamespaceSelectionTest, StartupCurrentValidationRejectsMissingSourceAndAncestorIndexes) {
  const auto file = file_record("/science/a", 10);
  initialize({file});
  const auto expected = current_records();
  for (const auto& key : {file.inode.path, format_key(file), std::string("/science"),
                         format_key(directory_record("/science", 2))}) {
    SCOPED_TRACE(key);
    auto transaction = owner.begin("fixture-missing-current-index");
    transaction->erase(Table::Current, key);
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    const auto view = owner.view();
    const auto exact = owner.scan(view, Table::Exact);
    try {
      validate_current_index(owner, view, exact);
      FAIL() << "missing required Current index was accepted";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    auto restore = owner.begin("fixture-restore-current-index");
    restore->put(Table::Current, key, encode_namespace_record(expected.at(key)));
    restore->commit();
  }
}

TEST_F(NamespaceSelectionTest, StartupCurrentValidationRejectsExtraOrphanIndexes) {
  initialize({file_record("/science/a", 10)});
  const auto ghost = file_record("/ghost", 100);
  for (const auto& key : {ghost.inode.path, format_key(ghost)}) {
    SCOPED_TRACE(key);
    auto transaction = owner.begin("fixture-extra-current-index");
    transaction->put(Table::Current, key, encode_namespace_record(ghost));
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    const auto view = owner.view();
    const auto exact = owner.scan(view, Table::Exact);
    try {
      validate_current_index(owner, view, exact);
      FAIL() << "extra Current record without Exact source was accepted";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    auto restore = owner.begin("fixture-remove-extra-current-index");
    restore->erase(Table::Current, key);
    restore->commit();
  }
}

TEST_F(NamespaceSelectionTest, StartupCurrentValidationRejectsOlderStillExactSourceAndAncestorRank) {
  const auto a10 = file_record("/science/a", 10);
  const auto a20 = file_record("/science/a", 20);
  initialize({a10, a20, file_record("/science/a", 30, std::string("grib")),
    file_record("/science/a", 40, std::nullopt)});
  commit({directory_record("/science", 100, 0000)});
  const auto expected = current_records();
  const auto old_parent = directory_record("/science", 2);
  const std::vector<std::pair<std::string, NamespaceRecord>> stale{
    {a10.inode.path, a20}, {format_key(a10), a10},
    {old_parent.inode.path, old_parent}, {format_key(old_parent), old_parent},
  };
  for (const auto& item : stale) {
    SCOPED_TRACE(item.first);
    auto transaction = owner.begin("fixture-stale-current-rank");
    transaction->put(Table::Current, item.first, encode_namespace_record(item.second));
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    const auto view = owner.view();
    const auto exact = owner.scan(view, Table::Exact);
    ASSERT_TRUE(owner.get(view, Table::Exact,
      exact_key(item.second.inode.path, item.second.inode.row_time)));
    try {
      validate_current_index(owner, view, exact);
      FAIL() << "membership alone incorrectly proved Current maximum rank";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    auto restore = owner.begin("fixture-restore-current-rank");
    restore->put(Table::Current, item.first, encode_namespace_record(expected.at(item.first)));
    restore->commit();
  }
}

TEST_F(NamespaceSelectionTest, StartupCurrentValidationRejectsSourceAndAncestorCanonicalMismatch) {
  const auto source = file_record("/science/a", 10);
  const auto parent = directory_record("/science", 2);
  initialize({source});
  const auto expected = current_records();
  auto source_mode = source;
  source_mode.inode.mode = 0777;
  auto source_descriptor = source;
  source_descriptor.descriptor_id = std::string(64, 'c');
  auto parent_mode = parent;
  parent_mode.inode.mode = 0777;
  auto parent_revision = parent;
  ++parent_revision.manifest.revision;
  const std::vector<std::pair<std::string, NamespaceRecord>> forged{
    {source.inode.path, source_mode}, {format_key(source), source_descriptor},
    {parent.inode.path, parent_mode}, {format_key(parent), parent_revision},
  };
  for (const auto& item : forged) {
    SCOPED_TRACE(item.first);
    auto transaction = owner.begin("fixture-current-canonical-mismatch");
    transaction->put(Table::Current, item.first, encode_namespace_record(item.second));
    transaction->commit();
    ASSERT_NO_THROW(owner.recover());
    const auto view = owner.view();
    const auto exact = owner.scan(view, Table::Exact);
    try {
      validate_current_index(owner, view, exact);
      FAIL() << "Current source/ancestor canonical mismatch was accepted";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::Corrupt);
    }
    auto restore = owner.begin("fixture-restore-current-canonical-record");
    restore->put(Table::Current, item.first, encode_namespace_record(expected.at(item.first)));
    restore->commit();
  }
}

// Regressions for Owner::scan's post-reference key filter.
// Prefix enumeration, page fees and the original scan limit precede filtering.
TEST_F(NamespaceSelectionTest, BetweenColdScanReadsOnlyRangeRecordsAndCurrentParents) {
  std::vector<NamespaceRecord> entries;
  for (int version = 1; version <= 128; ++version) {
    entries.push_back(file_record("/science/a", version));
    entries.push_back(file_record("/science/b", version));
  }
  entries.push_back(file_record("/science2/not-in-scope", 50));
  initialize(std::move(entries));
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  selector.versions = VersionSelection::between(50, 52);
  expect_reference(selector);
  owner.clear_read_cache();
  const auto result = select(selector);
  EXPECT_EQ(keys(result), (std::set<StructuredBlobVersionKey>{
    {"/science/a", 50}, {"/science/a", 51}, {"/science/b", 50}, {"/science/b", 51}}));
  std::set<std::string> selected_exact;
  for (const auto& item : observed) {
    const auto record = decode_namespace_record(item.bytes);
    if (item.table == Table::Exact && !record.inode.is_dir) {
      selected_exact.insert(item.key);
      EXPECT_GE(record.inode.row_time, 50);
      EXPECT_LT(record.inode.row_time, 52);
      EXPECT_TRUE(record.inode.path == "/science/a" || record.inode.path == "/science/b");
    } else {
      EXPECT_TRUE(item.table == Table::Current || item.table == Table::Exact);
      EXPECT_TRUE(record.inode.path == "/science" || record.inode.path == "/");
      EXPECT_TRUE(record.inode.is_dir);
    }
  }
  EXPECT_EQ(selected_exact.size(), 4u);
  EXPECT_EQ(store->catalog_record_reads, 6u);  // Four selected blobs plus two parents.
  ASSERT_TRUE(result.parents.at("/science"));
  EXPECT_EQ(result.parents.at("/science")->inode.row_time, 2);
}

TEST_F(NamespaceSelectionTest, LatestColdCurrentScanReadsOnlyEligibleIndexRecords) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/science/a", 30, std::string("grib")), file_record("/science/a", 40, std::nullopt),
    file_record("/science/b", 11), file_record("/science/b", 21, std::string("grib")),
    file_record("/science/b", 31, std::nullopt)});
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  expect_reference(selector);
  owner.clear_read_cache();
  auto result = select(selector);
  EXPECT_EQ(keys(result), (std::set<StructuredBlobVersionKey>{{"/science/a", 20}, {"/science/b", 11}}));
  for (const auto& item : observed) {
    EXPECT_TRUE(item.table == Table::Current || item.table == Table::Exact);
    const auto record = decode_namespace_record(item.bytes);
    if (!record.inode.is_dir) {
      ASSERT_TRUE(record.manifest.format_id);
      EXPECT_EQ(*record.manifest.format_id, "hdf5");
      EXPECT_EQ(item.key, item.table == Table::Current ? format_key(record) :
        exact_key(record.inode.path, record.inode.row_time));
    }
  }
  EXPECT_EQ(store->catalog_record_reads, 4u);  // Two eligible sources plus two parents.

  selector.formats = {AdapterSelectionKind::All, {}, true};
  expect_reference(selector);
  owner.clear_read_cache();
  result = select(selector);
  EXPECT_EQ(keys(result), (std::set<StructuredBlobVersionKey>{{"/science/a", 40}, {"/science/b", 31}}));
  for (const auto& item : observed) {
    const auto record = decode_namespace_record(item.bytes);
    if (!record.inode.is_dir) EXPECT_EQ(item.key,
      item.table == Table::Current ? record.inode.path :
        exact_key(record.inode.path, record.inode.row_time));
  }
  EXPECT_EQ(store->catalog_record_reads, 4u);
}

TEST_F(NamespaceSelectionTest, KeyFilterCannotHidePreFilterScanLimitExhaustion) {
  initialize({file_record("/science/a", 10), file_record("/science/b", 20), file_record("/science/c", 30)});
  const auto view = owner.view();
  owner.clear_read_cache();
  store->catalog_record_reads = 0;
  unsigned predicate_calls = 0;
  try {
    (void)owner.scan(view, Table::Exact, "/science/", 2,
      [&](const std::string&) { ++predicate_calls; return false; });
    FAIL() << "filter incorrectly hid three prefix references behind a limit of two";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::Limit);
  }
  // Tree::scan must finish its original bounded enumeration before a filter
  // executes or any selected value blob is fetched.
  EXPECT_EQ(predicate_calls, 0u);
  EXPECT_EQ(store->catalog_record_reads, 0u);
  EXPECT_FALSE(owner.frozen());
}

TEST_F(NamespaceSelectionTest, ZeroMatchKeyFilterChargesSameColdAndWarmPageFees) {
  initialize({file_record("/science/a", 10), file_record("/science/b", 20)});
  const auto view = owner.view();
  owner.clear_read_cache();
  std::uint64_t cold_bytes = 0;
  const auto reject = [](const std::string&) { return false; };
  for (int pass = 0; pass < 2; ++pass) {
    store->catalog_record_reads = 0;
    const auto before = owner.stats();
    ScopedQueryMetadataBudget request;
    EXPECT_TRUE(owner.scan(view, Table::Exact, "/science/", kScanEntries, reject).empty());
    EXPECT_GT(request.budget()->consumed_bytes(), 0u);
    EXPECT_TRUE(request.status().ok());
    EXPECT_EQ(store->catalog_record_reads, 0u);
    if (pass == 0) {
      cold_bytes = request.budget()->consumed_bytes();
      EXPECT_GT(owner.stats().object_reads, before.object_reads);
    } else {
      EXPECT_EQ(request.budget()->consumed_bytes(), cold_bytes);
      EXPECT_EQ(owner.stats().object_reads, before.object_reads);
      EXPECT_GT(owner.stats().page_cache_hits, before.page_cache_hits);
    }
  }
}

TEST_F(NamespaceSelectionTest, ZeroMatchKeyFilterCannotBypassWarmPageBudgetOrStickyFailure) {
  initialize({file_record("/science/a", 10)});
  const auto view = owner.view();
  const auto reject = [](const std::string&) { return false; };
  ASSERT_TRUE(owner.scan(view, Table::Exact, "/science/", kScanEntries, reject).empty());
  const auto before = owner.stats();
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW((void)owner.scan(view, Table::Exact, "/science/", kScanEntries, reject),
      QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_THROW((void)owner.scan(view, Table::Exact, "/absent/", kScanEntries, reject),
      QueryMetadataBudgetExceeded);
    EXPECT_FALSE(owner.frozen());
    EXPECT_EQ(owner.stats().object_reads, before.object_reads);
  }
  ScopedQueryMetadataBudget fresh;
  EXPECT_TRUE(owner.scan(view, Table::Exact, "/science/", kScanEntries, reject).empty());
  EXPECT_GT(fresh.budget()->consumed_bytes(), 0u);
  EXPECT_TRUE(fresh.status().ok());
}


TEST_F(NamespaceSelectionTest, ParsedProjectionMatchesAuthenticatedSelectionAcrossSelectors) {
  initialize({file_record("/science/a", 10), file_record("/science/a", 20),
    file_record("/science/a", 30, std::string("grib")),
    file_record("/science/b", 11), file_record("/science/b", 31, std::nullopt),
    file_record("/science2/c", 21), directory_record("/empty", 25)});
  const auto view = owner.view();
  const auto parsed = parsed_exact(view);
  auto base = hdf5("/science", PathScopeKind::Subtree);
  base.file_predicate = make_comparison({"sigma.nonexistent"},
    ComparisonOperator::Equal, LogicalValue::utf8("no-match"));
  for (const auto& versions : {VersionSelection::latest(), VersionSelection::all(),
      VersionSelection::exact_version(20), VersionSelection::between(10, 21)}) {
    for (const auto& formats : std::vector<FormatSelector>{
        base.formats, {AdapterSelectionKind::All, {}, false},
        {AdapterSelectionKind::All, {}, true}, {AdapterSelectionKind::Listed, {}, true}}) {
      auto selector = base;
      selector.versions = versions;
      selector.formats = formats;
      expect_reference(selector);
      const auto expected = select(selector, view);
      const auto actual = project(selector, view, parsed);
      EXPECT_EQ(keys(actual), keys(expected));
      ASSERT_EQ(actual.parents.size(), expected.parents.size());
      for (const auto& parent : expected.parents) {
        ASSERT_EQ(actual.parents.count(parent.first), 1u);
        ASSERT_EQ(actual.parents.at(parent.first).has_value(), parent.second.has_value());
        if (parent.second) EXPECT_EQ(encode_namespace_record(*actual.parents.at(parent.first)),
          encode_namespace_record(*parent.second));
      }
    }
  }
  auto overlap = base;
  auto overlap_paths = overlap.paths.scopes();
  overlap_paths.push_back({PathScopeKind::Exact, "/science/a"});
  overlap_paths.push_back({PathScopeKind::Subtree, "/science/a"});
  overlap.paths = std::move(overlap_paths);
  EXPECT_EQ(keys(project(overlap, view, parsed)), keys(select(overlap, view)));
  overlap.paths = {};
  EXPECT_EQ(keys(project(overlap, view, parsed)), keys(select(overlap, view)));
}

TEST_F(NamespaceSelectionTest, ParsedProjectionObservesOnlyCurrentAncestorsNotCachedSources) {
  std::vector<NamespaceRecord> entries;
  for (std::int64_t version = 10; version < 50; ++version)
    entries.push_back(file_record("/science/a", version));
  initialize(std::move(entries));
  const auto view = owner.view();
  const auto parsed = parsed_exact(view);
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  const auto selected = project(selector, view, parsed);
  EXPECT_EQ(keys(selected), (std::set<StructuredBlobVersionKey>{{"/science/a", 49}}));
  std::set<std::pair<Table, std::string>> reads;
  for (const auto& item : observed) reads.emplace(item.table, item.key);
  EXPECT_EQ(reads, (std::set<std::pair<Table, std::string>>{
    {Table::Current, "/"}, {Table::Current, "/science"},
    {Table::Exact, exact_key("/", 1)}, {Table::Exact, exact_key("/science", 2)}}));
  EXPECT_EQ(observed.size(), 4u);
}

TEST_F(NamespaceSelectionTest, ParsedProjectionPinsViewWhileUsingCurrentRatherThanHistoricalParents) {
  initialize({file_record("/science/a", 10), directory_record("/science", 20, 0700)});
  const auto before = owner.view();
  const auto old_parsed = parsed_exact(before);
  commit({file_record("/science/a", 30), directory_record("/science", 40, 0711)});
  const auto after = owner.view();
  auto selector = hdf5("/science", PathScopeKind::Subtree);
  const auto frozen = project(selector, before, old_parsed);
  EXPECT_EQ(keys(frozen), (std::set<StructuredBlobVersionKey>{{"/science/a", 10}}));
  ASSERT_TRUE(frozen.parents.at("/science"));
  EXPECT_EQ(frozen.parents.at("/science")->inode.row_time, 20);
  EXPECT_EQ(frozen.parents.at("/science")->inode.mode, 0700);
  selector.versions = VersionSelection::exact_version(10);
  const auto current = project(selector, after, parsed_exact(after));
  EXPECT_EQ(keys(current), keys(frozen));
  ASSERT_TRUE(current.parents.at("/science"));
  EXPECT_EQ(current.parents.at("/science")->inode.row_time, 40);
  EXPECT_EQ(current.parents.at("/science")->inode.mode, 0711);
  selector.versions = VersionSelection::latest();
  EXPECT_EQ(keys(project(selector, after, parsed_exact(after))),
    (std::set<StructuredBlobVersionKey>{{"/science/a", 30}}));
}

TEST_F(NamespaceSelectionTest, ParsedProjectionStillRejectsOrphanCurrentAncestor) {
  initialize({file_record("/science/a", 10)});
  auto transaction = owner.begin("project-orphan-current-parent");
  transaction->erase(Table::Exact, exact_key("/science", 2));
  transaction->commit();
  const auto view = owner.view();
  const auto parsed = parsed_exact(view);
  try {
    (void)project(hdf5("/science", PathScopeKind::Subtree), view, parsed);
    FAIL() << "parsed sources must not bypass current ancestor membership";
  } catch (const Error& error) { EXPECT_EQ(error.code(), ErrorCode::Corrupt); }
}

TEST_F(NamespaceSelectionTest, ParsedProjectionRejectsWrongIdentityAndStickyEmptyBudget) {
  initialize({file_record("/science/a", 10)});
  const auto view = owner.view();
  auto parsed = parsed_exact(view);
  auto wrong = parsed.at({"/science/a", 10});
  parsed.erase({"/science/a", 10});
  parsed.emplace(StructuredBlobVersionKey{"/science/a", 11}, std::move(wrong));
  try {
    (void)project(hdf5("/science", PathScopeKind::Subtree), view, parsed);
    FAIL() << "cached key/record mismatch must be rejected";
  } catch (const Error& error) { EXPECT_EQ(error.code(), ErrorCode::Corrupt); }
  const auto valid_parsed = parsed_exact(view);
  auto empty = hdf5("/science", PathScopeKind::Subtree);
  empty.formats = {AdapterSelectionKind::Listed, {}, false};
  {
    ScopedQueryMetadataBudget budget;
    EXPECT_THROW(require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1),
      QueryMetadataBudgetExceeded);
    EXPECT_THROW((void)project(empty, view, valid_parsed), QueryMetadataBudgetExceeded);
  }
  ScopedQueryMetadataBudget fresh;
  EXPECT_TRUE(project(empty, view, valid_parsed).sources.empty());
  EXPECT_TRUE(fresh.status().ok());
}

} // namespace
} // namespace iotdbfs::versioning
