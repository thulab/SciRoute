#include "iotdbfs/storage/NativeFileStore.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>

namespace iotdbfs {
namespace {
namespace fs = std::filesystem;
class NativeFileStoreTest : public ::testing::Test {
protected:
  std::string root;
  std::unique_ptr<NativeFileStore> store;
  void SetUp() override {
    auto pattern = (fs::canonical(fs::temp_directory_path()) / "iotdbfs-native-XXXXXX").string();
    std::vector<char> path(pattern.begin(), pattern.end()); path.push_back('\0');
    auto* made = ::mkdtemp(path.data()); ASSERT_NE(made, nullptr);
    root = made; store = std::make_unique<NativeFileStore>(root);
  }
  void TearDown() override { store.reset(); fs::remove_all(root); }
  NativeFileLocation identity(std::size_t size) {
    NativeFileLocation l;
    l.namespace_identity = {"123e4567-e89b-42d3-a456-426614174000",
                            "123e4567-e89b-42d3-a456-426614174001"};
    l.key = {"/hdf5/data.h5", 42}; l.descriptor_id = std::string(64, 'a');
    l.seal_id = std::string(64, 'b'); l.payload_root = std::string(64, 'c');
    l.size = size; return l;
  }
  NativeFileStore::Reader reader(const StructuredBlobBytes& bytes) {
    return [&bytes](std::uint64_t offset, std::uint64_t length) {
      return StructuredBlobResult<StructuredBlobBytes>::success(
        {bytes.begin() + offset, bytes.begin() + offset + length});
    };
  }
};

TEST_F(NativeFileStoreTest, InstallStreamsAndPublishesOnlyCompleteImmutableFile) {
  StructuredBlobBytes bytes(1024 * 1024 + 37);
  for (std::size_t i = 0; i < bytes.size(); ++i) bytes[i] = i % 251;
  std::size_t calls = 0;
  auto installed = store->install(identity(bytes.size()), [&](auto offset, auto length) {
    ++calls; EXPECT_LE(length, 1024U * 1024U);
    // Before the callback completes, only an unreferenced temporary is present.
    for (const auto& entry : fs::directory_iterator(root))
      EXPECT_EQ(entry.path().filename().string().find(".install-"), 0U);
    return reader(bytes)(offset, length);
  });
  ASSERT_TRUE(installed.ok()) << installed.status().message(); EXPECT_EQ(calls, 2U);
  const auto& l = installed.value();
  EXPECT_EQ(l.relative_path, l.sha256 + ".blob");
  struct stat s{}; ASSERT_EQ(::stat((root + "/" + l.relative_path).c_str(), &s), 0);
  EXPECT_EQ(s.st_mode & 07777, 0444); EXPECT_EQ(s.st_nlink, 1);
  auto opened = store->open(l); ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto tail = opened.value()->read(1024 * 1024 - 1, 38); ASSERT_TRUE(tail.ok());
  EXPECT_EQ(tail.value(), (StructuredBlobBytes(bytes.end() - 38, bytes.end())));
  EXPECT_FALSE(opened.value()->read(bytes.size(), 1).ok());
  auto empty = opened.value()->read(bytes.size(), 0); ASSERT_TRUE(empty.ok()); EXPECT_TRUE(empty.value().empty());
}

TEST_F(NativeFileStoreTest, CodecBindsExactIdentityAndRejectsMalformedLocations) {
  StructuredBlobBytes bytes{1, 2, 3};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto encoded = encode_native_file_location(installed.value());
  auto decoded = decode_native_file_location(encoded);
  EXPECT_EQ(encode_native_file_location(decoded), encoded);
  EXPECT_EQ(decoded.namespace_identity, installed.value().namespace_identity);
  EXPECT_EQ(decoded.key, installed.value().key);
  EXPECT_EQ(decoded.payload_root, installed.value().payload_root);
  auto trailing = encoded; trailing.push_back(0);
  EXPECT_THROW(decode_native_file_location(trailing), versioning::Error);
  encoded.resize(encoded.size() - 1);
  EXPECT_THROW(decode_native_file_location(encoded), versioning::Error);
  decoded.relative_path = "../" + decoded.relative_path;
  EXPECT_THROW(encode_native_file_location(decoded), versioning::Error);
  EXPECT_FALSE(store->open(decoded).ok());
  auto key = native_file_location_key(decoded.key);
  EXPECT_EQ(key.front(), '\1'); EXPECT_TRUE(is_native_file_location_key(key));
  EXPECT_FALSE(is_native_file_location_key(decoded.key.path));
  EXPECT_NE(key, native_file_location_key({decoded.key.path, decoded.key.version + 1}));
}

TEST_F(NativeFileStoreTest, ReuseBindsExactLocatorWithoutKeepingPreviousGuard) {
  StructuredBlobBytes bytes{1, 2, 3};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  bool old_allowed = true;
  auto old = opened.value()->with_read_guard([&] {
    return old_allowed ? StructuredBlobStatus::success() :
      StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "old request revoked");
  });
  auto key = store->cache_identity(installed.value()); ASSERT_TRUE(key.ok());
  EXPECT_EQ(key.value(), store->cache_identity(installed.value()).value());
  old_allowed = false;
  EXPECT_FALSE(old->validate().ok());
  auto reused = store->reuse(installed.value(), old->cache_descriptor()); ASSERT_TRUE(reused.ok());
  bool fresh_allowed = true;
  auto fresh = reused.value()->with_read_guard([&] {
    return fresh_allowed ? StructuredBlobStatus::success() :
      StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "new request revoked");
  });
  auto read = fresh->read(0, bytes.size()); ASSERT_TRUE(read.ok()); EXPECT_EQ(read.value(), bytes);
  fresh_allowed = false; EXPECT_FALSE(fresh->read(0, 1).ok());
  auto changed = installed.value(); ++changed.key.version;
  EXPECT_NE(key.value(), store->cache_identity(changed).value());
  EXPECT_FALSE(store->reuse(changed, old->cache_descriptor()).ok());
  EXPECT_FALSE(store->reuse(installed.value(), {}).ok());
}

TEST_F(NativeFileStoreTest, ReuseRejectsAnotherStoreAndChangedDirectoryEntry) {
  StructuredBlobBytes bytes{4, 5, 6};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  auto other_root = root + "/other"; ASSERT_TRUE(fs::create_directory(other_root));
  ASSERT_EQ(::chmod(other_root.c_str(), 0700), 0);
  NativeFileStore other(other_root);
  EXPECT_NE(store->cache_identity(installed.value()).value(), other.cache_identity(installed.value()).value());
  EXPECT_FALSE(other.reuse(installed.value(), opened.value()->cache_descriptor()).ok());
  auto path = root + "/" + installed.value().relative_path;
  ASSERT_EQ(::rename(path.c_str(), (path + ".old").c_str()), 0);
  { std::ofstream out(path, std::ios::binary); out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); }
  ASSERT_EQ(::chmod(path.c_str(), 0444), 0);
  EXPECT_FALSE(store->reuse(installed.value(), opened.value()->cache_descriptor()).ok());
}

TEST_F(NativeFileStoreTest, CachedDescriptorRetainsNoRequestGuard) {
  StructuredBlobBytes bytes{2, 3};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  auto proof = std::make_shared<int>(1); std::weak_ptr<int> lifetime = proof;
  auto capability = opened.value()->with_read_guard([proof] {
    return *proof ? StructuredBlobStatus::success() :
      StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "revoked");
  });
  auto descriptor = capability->cache_descriptor();
  capability.reset(); proof.reset(); EXPECT_TRUE(lifetime.expired());
  auto rebound = store->reuse(installed.value(), descriptor);
  ASSERT_TRUE(rebound.ok()); EXPECT_TRUE(rebound.value()->validate().ok());
}

TEST_F(NativeFileStoreTest, DedupDoesNotChangeExistingLocatorAndVerifiesExistingContent) {
  StructuredBlobBytes bytes{10, 20, 30};
  auto first = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(first.ok());
  auto next = identity(bytes.size()); ++next.key.version;
  auto second = store->install(next, reader(bytes)); ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(first.value().inode, second.value().inode);
  EXPECT_EQ(first.value().ctime_ns, second.value().ctime_ns);
  EXPECT_TRUE(store->open(first.value()).ok());
  auto path = root + "/" + first.value().relative_path;
  ASSERT_EQ(::chmod(path.c_str(), 0644), 0);
  { std::ofstream out(path, std::ios::binary); out << "bad"; }
  ASSERT_EQ(::chmod(path.c_str(), 0444), 0);
  EXPECT_FALSE(store->install(next, reader(bytes)).ok());
}

TEST_F(NativeFileStoreTest, FileSizeIsIndependentOfPerRequestReadBudget) {
  // Metadata-only check: large files may serve small, bounded native ROIs.
  auto location = identity(kMaxSharedPayloadReadBytes + 1);
  location.sha256 = std::string(64, 'd');
  location.relative_path = location.sha256 + ".blob";
  auto decoded = decode_native_file_location(encode_native_file_location(location));
  EXPECT_EQ(decoded.size, kMaxSharedPayloadReadBytes + 1);
}

TEST_F(NativeFileStoreTest, FailedOrShortInstallationLeavesNoPublishedFile) {
  auto short_read = store->install(identity(9), [](auto, auto) {
    return StructuredBlobResult<StructuredBlobBytes>::success({1});
  });
  EXPECT_FALSE(short_read.ok()); EXPECT_TRUE(fs::is_empty(root));
  auto failed = store->install(identity(9), [](auto, auto) {
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "source missing");
  });
  EXPECT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), StructuredBlobError::NotFound);
  EXPECT_TRUE(fs::is_empty(root));
  StructuredBlobBytes bytes{1, 2, 3}; auto wrong = identity(bytes.size()); wrong.sha256 = std::string(64, '0');
  EXPECT_FALSE(store->install(wrong, reader(bytes)).ok()); EXPECT_TRUE(fs::is_empty(root));
}

TEST_F(NativeFileStoreTest, MissingReplacementAndSymlinkRejectBothOpenAndPinnedReads) {
  StructuredBlobBytes bytes{7, 8, 9};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  auto path = root + "/" + installed.value().relative_path;
  ASSERT_EQ(::rename(path.c_str(), (path + ".old").c_str()), 0);
  EXPECT_FALSE(opened.value()->validate().ok());
  EXPECT_FALSE(opened.value()->read(0, 1).ok());
  EXPECT_FALSE(store->open(installed.value()).ok());
  ASSERT_EQ(::symlink((path + ".old").c_str(), path.c_str()), 0);
  EXPECT_FALSE(store->open(installed.value()).ok());
  EXPECT_FALSE(opened.value()->read(0, 1).ok());
  EXPECT_FALSE(store->install(identity(bytes.size()), reader(bytes)).ok());
}

TEST_F(NativeFileStoreTest, InPlaceChangesInvalidatePinnedHandleWithoutPerQueryHashing) {
  StructuredBlobBytes bytes{1, 2, 3, 4};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  auto path = root + "/" + installed.value().relative_path;
  ASSERT_EQ(::chmod(path.c_str(), 0644), 0);
  { std::ofstream out(path, std::ios::binary); out << "changed"; }
  ASSERT_EQ(::chmod(path.c_str(), 0444), 0);
  EXPECT_FALSE(opened.value()->validate().ok()); EXPECT_FALSE(opened.value()->read(0, 1).ok());
  EXPECT_FALSE(store->open(installed.value()).ok());
}

TEST_F(NativeFileStoreTest, RootRejectsRelativeTraversalSymlinksAndPublicPermissions) {
  EXPECT_THROW(NativeFileStore("relative"), versioning::Error);
  EXPECT_THROW(NativeFileStore(root + "/../escape"), versioning::Error);
  auto alias = root + "/alias"; ASSERT_EQ(::symlink(root.c_str(), alias.c_str()), 0);
  EXPECT_THROW({ NativeFileStore invalid(alias); }, versioning::Error);
  ASSERT_EQ(::chmod(root.c_str(), 0755), 0);
  EXPECT_THROW({ NativeFileStore invalid(root); }, versioning::Error);
  ASSERT_EQ(::chmod(root.c_str(), 0700), 0);
}

TEST_F(NativeFileStoreTest, HandlePinsDescriptorsAfterStoreIsDestroyed) {
  StructuredBlobBytes bytes{4, 5, 6};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok()); store.reset();
  auto read = opened.value()->read(0, bytes.size()); ASSERT_TRUE(read.ok()); EXPECT_EQ(read.value(), bytes);
}

TEST_F(NativeFileStoreTest, LiveReadGuardsCannotBeRemovedOrWeakenedByAdditionalGuards) {
  StructuredBlobBytes bytes{4, 5, 6};
  auto installed = store->install(identity(bytes.size()), reader(bytes)); ASSERT_TRUE(installed.ok());
  auto opened = store->open(installed.value()); ASSERT_TRUE(opened.ok());
  bool available = true;
  auto guarded = opened.value()->with_read_guard([&] {
    return available ? StructuredBlobStatus::success() :
      StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "owner frozen");
  });
  auto empty_added = guarded->with_read_guard({});
  auto permissive_added = guarded->with_read_guard([] { return StructuredBlobStatus::success(); });
  EXPECT_TRUE(guarded->read(0, 3).ok()); EXPECT_TRUE(empty_added->read(0, 3).ok());
  available = false;
  EXPECT_FALSE(guarded->validate().ok());
  auto rejected = guarded->read(0, 3); EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().message(), "owner frozen");
  EXPECT_FALSE(empty_added->read(0, 3).ok()); EXPECT_FALSE(permissive_added->read(0, 3).ok());
  // Standalone store capabilities have no owner guard until explicitly wrapped.
  EXPECT_TRUE(opened.value()->read(0, 3).ok());
}
} // namespace
} // namespace iotdbfs
