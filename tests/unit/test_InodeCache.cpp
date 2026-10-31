#include <gtest/gtest.h>
#include "iotdbfs/InodeCache.h"
#include "iotdbfs/PathUtils.h"
#include <set>
#include <thread>
#include <vector>
#include <atomic>

using namespace path_utils;

class InodeCacheTest : public ::testing::Test {
protected:
  void SetUp() override {
    auto& cache = InodeCache::instance();
    cache.clear();
    hooks_ = InodeCache::DbHooks{
      .load_by_path = [this](const std::string& p) -> std::optional<Inode> {
        ++load_count;
        auto it = fake_db_.find(p);
        return it != fake_db_.end() ? std::optional<Inode>(it->second) : std::nullopt;
      },
      .load_children = [this](const std::string& parent) -> std::vector<Inode> {
        ++children_load_count;
        std::vector<Inode> result;
        for (auto& [p, ino] : fake_db_)
          if (ino.parent == parent) result.push_back(ino);
        return result;
      },
      .upsert = [this](const Inode& ino) -> bool {
        ++upsert_count;
        if (failing_upserts.count(ino.path))
          return false;
        fake_db_[ino.path] = ino;
        return true;
      },
      .erase_one = [this](const std::string& p, std::optional<long long> row_time) -> bool {
        ++erase_count;
        erased_row_time = row_time;
        if (!erase_succeeds)
          return false;
        fake_db_.erase(p);
        return true;
      },
      .erase_tree = [this](const std::string& p) -> bool {
        ++erase_tree_count;
        if (!erase_succeeds)
          return false;
        for (auto it = fake_db_.begin(); it != fake_db_.end();) {
          if (it->first == p || (it->first.size() > p.size() &&
            it->first.compare(0, p.size(), p) == 0 && it->first[p.size()] == '/'))
            it = fake_db_.erase(it);
          else ++it;
        }
        return true;
      }
    };
    cache.configure(hooks_, 10, std::chrono::milliseconds(500));
  }

  void TearDown() override {
    InodeCache::instance().clear();
    InodeCache::instance().configure({}, 10, std::chrono::milliseconds(500));
    fake_db_.clear();
    load_count = upsert_count = erase_count = erase_tree_count = 0;
  }

  Inode make_inode(const std::string& path, const std::string& parent, bool is_dir = false) {
    Inode ino;
    ino.path = path;
    ino.parent = parent;
    ino.name = base(path);
    ino.is_dir = is_dir;
    ino.row_time = 123456789;
    return ino;
  }

  std::map<std::string, Inode> fake_db_;
  InodeCache::DbHooks hooks_;
  std::set<std::string> failing_upserts;
  std::optional<long long> erased_row_time;
  bool erase_succeeds = true;
  int load_count = 0;
  int children_load_count = 0;
  int upsert_count = 0;
  int erase_count = 0;
  int erase_tree_count = 0;
};

TEST_F(InodeCacheTest, PutAndGet) {
  auto& cache = InodeCache::instance();
  auto ino = make_inode("/file", "/");
  cache.put(ino, false);

  auto got = cache.get("/file");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->path, "/file");
  EXPECT_EQ(got->name, "file");
  EXPECT_EQ(load_count, 0);
}

TEST_F(InodeCacheTest, GetMiss_NotInCache) {
  auto& cache = InodeCache::instance();
  auto got = cache.get("/nonexistent");
  EXPECT_FALSE(got.has_value());
  EXPECT_EQ(load_count, 1);
}

TEST_F(InodeCacheTest, GetMiss_LoadFromDb) {
  auto& cache = InodeCache::instance();
  fake_db_["/file"] = make_inode("/file", "/");

  auto got = cache.get("/file");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->path, "/file");
  EXPECT_EQ(load_count, 1);
}

TEST_F(InodeCacheTest, GetHit_CacheHit) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);

  auto got = cache.get("/file");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(load_count, 0);
}

TEST_F(InodeCacheTest, PutMarkDirty) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), true);

  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 1);
}

TEST_F(InodeCacheTest, PutNotDirty) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);

  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 0);
}

TEST_F(InodeCacheTest, DirtyStaysDirty) {
  auto& cache = InodeCache::instance();
  auto ino = make_inode("/file", "/");
  cache.put(ino, true);
  cache.put(ino, false);

  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 1);
}

TEST_F(InodeCacheTest, Flush_NonExistent) {
  auto& cache = InodeCache::instance();
  EXPECT_FALSE(cache.flush("/nonexistent"));
}

TEST_F(InodeCacheTest, Erase) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);
  EXPECT_TRUE(cache.erase("/file"));
  EXPECT_EQ(erase_count, 1);

  auto got = cache.get("/file");
  EXPECT_FALSE(got.has_value());
}

TEST_F(InodeCacheTest, EraseSubtree) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir"] = make_inode("/dir", "/", true);
  fake_db_["/dir/a"] = make_inode("/dir/a", "/dir");
  fake_db_["/dir/b"] = make_inode("/dir/b", "/dir");
  fake_db_["/dir/sub/c"] = make_inode("/dir/sub/c", "/dir/sub");

  EXPECT_TRUE(cache.erase_subtree("/dir"));
  EXPECT_EQ(erase_tree_count, 1);
  EXPECT_EQ(fake_db_.count("/dir"), 0);
  EXPECT_EQ(fake_db_.count("/dir/a"), 0);
  EXPECT_EQ(fake_db_.count("/dir/b"), 0);
  EXPECT_EQ(fake_db_.count("/dir/sub/c"), 0);
}

TEST_F(InodeCacheTest, FlushAll) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file1", "/"), true);
  cache.put(make_inode("/file2", "/"), true);

  cache.flush_all();
  EXPECT_EQ(upsert_count, 2);
}

TEST_F(InodeCacheTest, ListChildren_FromCache) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/dir", "/", true), false);
  cache.put(make_inode("/dir/a", "/dir"), false);
  cache.put(make_inode("/dir/b", "/dir"), false);

  auto children = cache.list_children("/dir");
  EXPECT_EQ(children.size(), 2);
}

TEST_F(InodeCacheTest, ListChildren_DoesNotReturnParentItself) {
  auto& cache = InodeCache::instance();
  auto root = make_inode("/", "/", true);
  root.name = "/";
  fake_db_["/"] = root;
  fake_db_["/file"] = make_inode("/file", "/");

  auto loaded_root = cache.get("/");
  ASSERT_TRUE(loaded_root.has_value());

  auto children = cache.list_children("/");
  std::set<std::string> paths;
  for (const auto& child : children)
    paths.insert(child.path);

  EXPECT_EQ(paths.size(), 1);
  EXPECT_TRUE(paths.count("/file"));
  EXPECT_FALSE(paths.count("/"));
}

TEST_F(InodeCacheTest, ListChildren_IncludesDirtyUnflushedChildren) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir"] = make_inode("/dir", "/", true);

  cache.put(make_inode("/dir/new-file", "/dir"), true);

  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1);
  EXPECT_EQ(children[0].path, "/dir/new-file");
  EXPECT_EQ(upsert_count, 0);
}

TEST_F(InodeCacheTest, ListChildren_MergesDbAndCachedChildren) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir"] = make_inode("/dir", "/", true);
  fake_db_["/dir/db-file"] = make_inode("/dir/db-file", "/dir");

  cache.put(make_inode("/dir/cached-file", "/dir"), true);

  auto children = cache.list_children("/dir");
  std::set<std::string> paths;
  for (const auto& child : children)
    paths.insert(child.path);

  EXPECT_EQ(paths.size(), 2);
  EXPECT_TRUE(paths.count("/dir/db-file"));
  EXPECT_TRUE(paths.count("/dir/cached-file"));
}

TEST_F(InodeCacheTest, ListChildren_CachedChildListSurvivesCleanChildEviction) {
  auto& cache = InodeCache::instance();
  cache.configure(InodeCache::DbHooks{
    .load_by_path = [this](const std::string& p) -> std::optional<Inode> {
      ++load_count;
      auto it = fake_db_.find(p);
      return it != fake_db_.end() ? std::optional<Inode>(it->second) : std::nullopt;
    },
    .load_children = [this](const std::string& parent) -> std::vector<Inode> {
      std::vector<Inode> result;
      for (auto& [p, ino] : fake_db_)
        if (ino.parent == parent) result.push_back(ino);
      return result;
    },
    .upsert = [this](const Inode& ino) -> bool {
      fake_db_[ino.path] = ino;
      return true;
    },
    .erase_one = [this](const std::string& p, std::optional<long long>) -> bool {
      fake_db_.erase(p);
      return true;
    },
    .erase_tree = [this](const std::string& p) -> bool {
      fake_db_.erase(p);
      return true;
    }
  }, 2, std::chrono::milliseconds(500));

  fake_db_["/dir/a"] = make_inode("/dir/a", "/dir");
  fake_db_["/dir/b"] = make_inode("/dir/b", "/dir");
  fake_db_["/dir/c"] = make_inode("/dir/c", "/dir");

  auto first = cache.list_children("/dir");
  ASSERT_EQ(first.size(), 3);

  cache.get("/dir/a");
  auto second = cache.list_children("/dir");
  std::set<std::string> paths;
  for (const auto& child : second)
    paths.insert(child.path);

  EXPECT_EQ(paths.size(), 3);
  EXPECT_TRUE(paths.count("/dir/a"));
  EXPECT_TRUE(paths.count("/dir/b"));
  EXPECT_TRUE(paths.count("/dir/c"));
}

TEST_F(InodeCacheTest, ListChildren_FromDb) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir"] = make_inode("/dir", "/", true);
  fake_db_["/dir/a"] = make_inode("/dir/a", "/dir");
  fake_db_["/dir/b"] = make_inode("/dir/b", "/dir");

  auto children = cache.list_children("/dir");
  EXPECT_EQ(children.size(), 2);
  EXPECT_EQ(load_count, 0);
}

TEST_F(InodeCacheTest, Update) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);

  cache.update("/file", [](Inode& i) { i.size = 100; }, true);
  auto got = cache.get("/file");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->size, 100);

  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 1);
}

TEST_F(InodeCacheTest, Update_NotInCacheLoadsFromDb) {
  auto& cache = InodeCache::instance();
  fake_db_["/file"] = make_inode("/file", "/");

  cache.update("/file", [](Inode& i) { i.size = 100; }, true);
  auto got = cache.get("/file");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->size, 100);
  EXPECT_EQ(load_count, 1);
}

TEST_F(InodeCacheTest, Discard) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);
  cache.discard("/file");

  auto got = cache.get("/file");
  EXPECT_FALSE(got.has_value());
  EXPECT_EQ(erase_count, 0);
}

TEST_F(InodeCacheTest, LruEviction) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/f1", "/"), false);
  cache.put(make_inode("/f2", "/"), false);

  for (int i = 3; i <= 11; ++i) {
    cache.put(make_inode("/f" + std::to_string(i), "/"), false);
    cache.get("/f1");
  }

  auto got1 = cache.get("/f1");
  auto got2 = cache.get("/f2");
  EXPECT_TRUE(got1.has_value());
  EXPECT_FALSE(got2.has_value());
}

TEST_F(InodeCacheTest, DirtyNotEvicted) {
  auto& cache = InodeCache::instance();

  for (int i = 1; i <= 10; ++i) {
    cache.put(make_inode("/d" + std::to_string(i), "/"), true);
  }
  cache.put(make_inode("/clean", "/"), false);

  auto got = cache.get("/clean");
  EXPECT_FALSE(got.has_value());
}

TEST_F(InodeCacheTest, ConcurrentAccess) {
  auto& cache = InodeCache::instance();
  // Keep all entries resident: eviction between put/get is valid behavior and
  // must not turn this thread-safety test into a scheduler-dependent failure.
  cache.configure(hooks_, 400, std::chrono::milliseconds(500));
  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([this, &cache, &success_count, i]() {
      for (int j = 0; j < 100; ++j) {
        std::string path = "/file_" + std::to_string(i) + "_" + std::to_string(j);
        cache.put(make_inode(path, "/"), false);
        auto got = cache.get(path);
        if (got.has_value() && got->path == path) {
          ++success_count;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 400);
}

TEST_F(InodeCacheTest, ListChildren_EmptyDirectory) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir"] = make_inode("/dir", "/", true);

  auto children = cache.list_children("/dir");
  EXPECT_TRUE(children.empty());
}

TEST_F(InodeCacheTest, Erase_NonExistent) {
  auto& cache = InodeCache::instance();
  EXPECT_TRUE(cache.erase("/nonexistent"));
  EXPECT_EQ(erase_count, 1);
}

TEST_F(InodeCacheTest, EraseSubtree_NonExistent) {
  auto& cache = InodeCache::instance();
  EXPECT_TRUE(cache.erase_subtree("/nonexistent"));
  EXPECT_EQ(erase_tree_count, 1);
}

TEST_F(InodeCacheTest, Update_NonExistentReturnsFalse) {
  auto& cache = InodeCache::instance();
  auto result = cache.update("/nonexistent", [](Inode&) {}, false);
  EXPECT_FALSE(result);
}

TEST_F(InodeCacheTest, ReturnedInodeIsAnIndependentMetadataSnapshot) {
  auto& cache = InodeCache::instance();
  auto inode = make_inode("/weather.grib", "/");
  inode.size = 1024;
  inode.mode = 0600;
  inode.uid = 42;
  inode.gid = 84;
  inode.metadata = R"({"shortName":"t","level":850})";
  ASSERT_TRUE(cache.put(inode, true));
  auto snapshot = cache.get(inode.path);
  ASSERT_TRUE(snapshot);
  EXPECT_EQ(snapshot->row_time, inode.row_time);
  EXPECT_EQ(snapshot->size, 1024);
  EXPECT_EQ(snapshot->mode, 0600);
  EXPECT_EQ(snapshot->uid, 42);
  EXPECT_EQ(snapshot->gid, 84);
  EXPECT_EQ(snapshot->metadata, inode.metadata);
  snapshot->size = 0;
  snapshot->metadata.clear();
  ASSERT_TRUE(cache.flush(inode.path));
  EXPECT_EQ(fake_db_.at(inode.path).size, 1024);
  EXPECT_EQ(fake_db_.at(inode.path).metadata, inode.metadata);
}

TEST_F(InodeCacheTest, DatabaseLoadIsCachedAndMissDoesNotHideLaterCreation) {
  auto& cache = InodeCache::instance();
  EXPECT_FALSE(cache.get("/file"));
  fake_db_["/file"] = make_inode("/file", "/");
  ASSERT_TRUE(cache.get("/file"));
  ASSERT_TRUE(cache.get("/file"));
  EXPECT_EQ(load_count, 2);
}

TEST_F(InodeCacheTest, DatabaseLoadDoesNotOverwriteEntryCreatedDuringLoad) {
  auto& cache = InodeCache::instance();
  auto stored = make_inode("/file", "/");
  stored.size = 10;
  hooks_.load_by_path = [&cache, stored](const std::string&) {
    auto newer = stored;
    newer.size = 20;
    cache.put(newer, true);
    return std::optional<Inode>(stored);
  };
  cache.configure(hooks_, 10, std::chrono::milliseconds(500));
  auto loaded = cache.get("/file");
  ASSERT_TRUE(loaded);
  EXPECT_EQ(loaded->size, 20);
  ASSERT_TRUE(cache.flush("/file"));
  EXPECT_EQ(fake_db_.at("/file").size, 20);
}

TEST_F(InodeCacheTest, FlushFailureRetainsDirtyDataForRetry) {
  auto& cache = InodeCache::instance();
  auto inode = make_inode("/file", "/");
  inode.size = 123;
  cache.put(inode, true);
  failing_upserts.insert("/file");
  EXPECT_FALSE(cache.flush("/file"));
  EXPECT_EQ(fake_db_.count("/file"), 0u);
  ASSERT_TRUE(cache.get("/file"));
  failing_upserts.clear();
  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(fake_db_.at("/file").size, 123);
  EXPECT_EQ(upsert_count, 2);
  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 2);
}

TEST_F(InodeCacheTest, FlushAllContinuesAfterFailureAndRetriesOnlyFailedEntries) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/good", "/"), true);
  cache.put(make_inode("/failed", "/"), true);
  cache.put(make_inode("/clean", "/"), false);
  failing_upserts.insert("/failed");
  EXPECT_FALSE(cache.flush_all());
  EXPECT_EQ(upsert_count, 2);
  EXPECT_EQ(fake_db_.count("/good"), 1u);
  EXPECT_EQ(fake_db_.count("/failed"), 0u);
  EXPECT_EQ(fake_db_.count("/clean"), 0u);
  failing_upserts.clear();
  EXPECT_TRUE(cache.flush_all());
  EXPECT_EQ(upsert_count, 3);
  EXPECT_EQ(fake_db_.count("/failed"), 1u);
  EXPECT_TRUE(cache.flush_all());
  EXPECT_EQ(upsert_count, 3);
}

TEST_F(InodeCacheTest, UpdateDuringFlushRemainsDirtyUntilItsOwnWriteSucceeds) {
  auto& cache = InodeCache::instance();
  auto inode = make_inode("/file", "/");
  inode.size = 1;
  cache.put(inode, true);
  std::vector<long long> persisted_sizes;
  hooks_.upsert = [&cache, &persisted_sizes](const Inode& current) {
    persisted_sizes.push_back(current.size);
    if (persisted_sizes.size() == 1)
      cache.update(current.path, [](Inode& entry) { entry.size = 2; }, true);
    return true;
  };
  cache.configure(hooks_, 10, std::chrono::milliseconds(500));
  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_TRUE(cache.flush("/file"));
  EXPECT_EQ(persisted_sizes, (std::vector<long long>{1, 2}));
}

TEST_F(InodeCacheTest, UpdateDuringFlushAllRetainsTheNewDirtyGeneration) {
  auto& cache = InodeCache::instance();
  auto inode = make_inode("/file", "/");
  inode.size = 1;
  cache.put(inode, true);
  std::vector<long long> persisted_sizes;
  hooks_.upsert = [&cache, &persisted_sizes](const Inode& current) {
    persisted_sizes.push_back(current.size);
    if (persisted_sizes.size() == 1)
      cache.update(current.path, [](Inode& entry) { entry.size = 2; }, true);
    return true;
  };
  cache.configure(hooks_, 10, std::chrono::milliseconds(500));
  EXPECT_TRUE(cache.flush_all());
  EXPECT_TRUE(cache.flush_all());
  EXPECT_TRUE(cache.flush_all());
  EXPECT_EQ(persisted_sizes, (std::vector<long long>{1, 2}));
}

TEST_F(InodeCacheTest, FailedErasePreservesCachedInodeAndDirectoryListing) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir/file"] = make_inode("/dir/file", "/dir");
  ASSERT_EQ(cache.list_children("/dir").size(), 1u);
  erase_succeeds = false;
  EXPECT_FALSE(cache.erase("/dir/file", 987));
  EXPECT_EQ(erased_row_time, std::optional<long long>(987));
  ASSERT_TRUE(cache.get("/dir/file"));
  EXPECT_EQ(cache.list_children("/dir").size(), 1u);
  EXPECT_EQ(children_load_count, 1);
  erase_succeeds = true;
  EXPECT_TRUE(cache.erase("/dir/file"));
  EXPECT_EQ(erased_row_time, std::nullopt);
  EXPECT_TRUE(cache.list_children("/dir").empty());
  EXPECT_EQ(children_load_count, 2);
}

TEST_F(InodeCacheTest, FailedSubtreeErasePreservesAllEntries) {
  auto& cache = InodeCache::instance();
  for (const auto& path : {"/dir", "/dir/a", "/dir/sub/b"}) {
    auto inode = make_inode(path, parent_of(path));
    fake_db_[path] = inode;
    cache.put(inode, false);
  }
  erase_succeeds = false;
  EXPECT_FALSE(cache.erase_subtree("/dir"));
  for (const auto& path : {"/dir", "/dir/a", "/dir/sub/b"}) {
    EXPECT_TRUE(cache.get(path));
    EXPECT_EQ(fake_db_.count(path), 1u);
  }
  EXPECT_EQ(load_count, 0);
}

TEST_F(InodeCacheTest, SubtreeEraseDoesNotRemovePathsWithOnlyAMatchingPrefix) {
  auto& cache = InodeCache::instance();
  for (const auto& path : {"/dir", "/dir/a", "/dir/sub/b", "/directory/a", "/dir2"}) {
    auto inode = make_inode(path, parent_of(path));
    fake_db_[path] = inode;
    cache.put(inode, false);
  }
  ASSERT_TRUE(cache.erase_subtree("/dir"));
  for (const auto& path : {"/dir", "/dir/a", "/dir/sub/b"})
    EXPECT_FALSE(cache.get(path));
  for (const auto& path : {"/directory/a", "/dir2"}) {
    EXPECT_TRUE(cache.get(path));
    EXPECT_EQ(fake_db_.count(path), 1u);
  }
}

TEST_F(InodeCacheTest, EmptyDirectoryListingIsCachedAndPutInvalidatesIt) {
  auto& cache = InodeCache::instance();
  EXPECT_TRUE(cache.list_children("/dir").empty());
  EXPECT_TRUE(cache.list_children("/dir").empty());
  EXPECT_EQ(children_load_count, 1);
  cache.put(make_inode("/dir/new", "/dir"), true);
  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1u);
  EXPECT_EQ(children.front().path, "/dir/new");
  EXPECT_EQ(children_load_count, 2);
}

TEST_F(InodeCacheTest, ListingUsesDirtyMetadataInsteadOfStaleDatabaseMetadata) {
  auto& cache = InodeCache::instance();
  auto stored = make_inode("/dir/file", "/dir");
  stored.size = 10;
  fake_db_[stored.path] = stored;
  auto newer = stored;
  newer.size = 99;
  newer.metadata = R"({"shortName":"u"})";
  cache.put(newer, true);
  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1u);
  EXPECT_EQ(children.front().size, 99);
  EXPECT_EQ(children.front().metadata, newer.metadata);
  ASSERT_TRUE(cache.flush("/dir/file"));
  EXPECT_EQ(fake_db_.at("/dir/file").size, 99);
}

TEST_F(InodeCacheTest, DirectoryListingContainsOnlyDirectChildren) {
  auto& cache = InodeCache::instance();
  for (const auto& path : {"/dir/a", "/dir/sub", "/dir/sub/nested", "/other/file"})
    fake_db_[path] = make_inode(path, parent_of(path));
  cache.put(make_inode("/dir/local", "/dir"), true);
  cache.put(make_inode("/dir/sub/local", "/dir/sub"), true);
  auto children = cache.list_children("/dir");
  std::set<std::string> paths;
  for (const auto& child : children)
    paths.insert(child.path);
  EXPECT_EQ(paths, (std::set<std::string>{"/dir/a", "/dir/local", "/dir/sub"}));
}

TEST_F(InodeCacheTest, UpdateRefreshesCachedDirectoryListing) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir/file"] = make_inode("/dir/file", "/dir");
  ASSERT_EQ(cache.list_children("/dir").size(), 1u);
  ASSERT_TRUE(cache.update("/dir/file", [](Inode& inode) { inode.size = 42; }, true));
  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1u);
  EXPECT_EQ(children.front().size, 42);
  EXPECT_EQ(children_load_count, 2);
}

TEST_F(InodeCacheTest, DiscardInvalidatesListingAndReloadsStoredVersionWithoutWriting) {
  auto& cache = InodeCache::instance();
  auto stored = make_inode("/dir/file", "/dir");
  stored.size = 1;
  fake_db_[stored.path] = stored;
  auto changed = stored;
  changed.size = 2;
  cache.put(changed, true);
  const auto before_discard = cache.list_children("/dir");
  ASSERT_EQ(before_discard.size(), 1u);
  ASSERT_EQ(before_discard.front().size, 2);
  cache.discard(stored.path);
  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1u);
  EXPECT_EQ(children.front().size, 1);
  EXPECT_EQ(children_load_count, 2);
  EXPECT_EQ(upsert_count, 0);
  EXPECT_EQ(erase_count, 0);
}

TEST_F(InodeCacheTest, NonDirtyUpdateDoesNotClearAnExistingDirtyFlag) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), true);
  ASSERT_TRUE(cache.update("/file", [](Inode& inode) { inode.size = 5; }, false));
  ASSERT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 1);
  EXPECT_EQ(fake_db_.at("/file").size, 5);
}

TEST_F(InodeCacheTest, NonDirtyUpdateOnCleanEntryDoesNotWriteBack) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/file", "/"), false);
  ASSERT_TRUE(cache.update("/file", [](Inode& inode) { inode.size = 5; }, false));
  ASSERT_TRUE(cache.flush("/file"));
  EXPECT_EQ(upsert_count, 0);
  auto inode = cache.get("/file");
  ASSERT_TRUE(inode);
  EXPECT_EQ(inode->size, 5);
}

TEST_F(InodeCacheTest, MissingUpdateDoesNotInvokeMutator) {
  bool invoked = false;
  EXPECT_FALSE(InodeCache::instance().update("/missing", [&](Inode&) {
    invoked = true;
  }, true));
  EXPECT_FALSE(invoked);
  EXPECT_EQ(upsert_count, 0);
}

TEST_F(InodeCacheTest, ClearDropsDirtyEntriesAndDirectoryListingsWithoutDatabaseWrites) {
  auto& cache = InodeCache::instance();
  fake_db_["/dir/stored"] = make_inode("/dir/stored", "/dir");
  cache.put(make_inode("/dir/dirty", "/dir"), true);
  ASSERT_EQ(cache.list_children("/dir").size(), 2u);
  cache.clear();
  EXPECT_TRUE(cache.flush_all());
  auto children = cache.list_children("/dir");
  ASSERT_EQ(children.size(), 1u);
  EXPECT_EQ(children.front().path, "/dir/stored");
  EXPECT_EQ(children_load_count, 2);
  EXPECT_EQ(upsert_count, 0);
  EXPECT_EQ(erase_count, 0);
  EXPECT_EQ(erase_tree_count, 0);
}

TEST_F(InodeCacheTest, DirtyEntriesSurviveCapacityPressureUntilFlushed) {
  auto& cache = InodeCache::instance();
  cache.configure(hooks_, 2, std::chrono::milliseconds(500));
  for (int i = 0; i < 5; ++i)
    cache.put(make_inode("/dirty" + std::to_string(i), "/"), true);
  for (int i = 0; i < 5; ++i)
    EXPECT_TRUE(cache.get("/dirty" + std::to_string(i)));
  EXPECT_EQ(load_count, 0);
  ASSERT_TRUE(cache.flush_all());
  EXPECT_EQ(upsert_count, 5);
  EXPECT_EQ(fake_db_.size(), 5u);
}

TEST_F(InodeCacheTest, ConcurrentUpdatesToSameInodeDoNotLoseIncrements) {
  auto& cache = InodeCache::instance();
  cache.put(make_inode("/counter", "/"), true);
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int worker = 0; worker < 4; ++worker) {
    threads.emplace_back([&cache, &failures] {
      for (int iteration = 0; iteration < 250; ++iteration)
        if (!cache.update("/counter", [](Inode& inode) { ++inode.size; }, true))
          ++failures;
    });
  }
  for (auto& thread : threads)
    thread.join();
  EXPECT_EQ(failures, 0);
  auto inode = cache.get("/counter");
  ASSERT_TRUE(inode);
  EXPECT_EQ(inode->size, 1000);
  ASSERT_TRUE(cache.flush("/counter"));
  EXPECT_EQ(fake_db_.at("/counter").size, 1000);
}

TEST_F(InodeCacheTest, MissingDatabaseHooksFailReadAndWriteBackWithoutLosingDirtyEntry) {
  auto& cache = InodeCache::instance();
  cache.configure({}, 10, std::chrono::milliseconds(500));
  EXPECT_FALSE(cache.get("/missing"));
  cache.put(make_inode("/dirty", "/"), true);
  EXPECT_FALSE(cache.flush("/dirty"));
  EXPECT_FALSE(cache.flush_all());
  cache.configure(hooks_, 10, std::chrono::milliseconds(500));
  ASSERT_TRUE(cache.flush("/dirty"));
  EXPECT_EQ(fake_db_.count("/dirty"), 1u);
}

TEST_F(InodeCacheTest, MissingChildrenLoaderReturnsEmptyAndDiscardingMissingPathIsHarmless) {
  auto& cache = InodeCache::instance();
  cache.configure({}, 10, std::chrono::milliseconds(0));
  EXPECT_TRUE(cache.list_children("/missing").empty());
  cache.discard("/missing");
  cache.put(make_inode("/retained", "/"), false);
  cache.discard("/another-missing");
  const auto retained = cache.get("/retained");
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->path, "/retained");
}
