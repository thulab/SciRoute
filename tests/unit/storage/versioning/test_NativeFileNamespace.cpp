#include "iotdbfs/storage/NativeFileStore.h"
#include "iotdbfs/storage/versioning/NamespaceSelection.h"
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/PathUtils.h"

#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <tuple>

namespace iotdbfs::versioning {
namespace {

class NativeNamespaceStore final : public Store {
public:
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  std::optional<Bytes> head;
  std::map<Key, Bytes> objects;
  static Key key(const NamespaceIdentity& ns, const ObjectRef& ref) {
    return {ns.namespace_id, ns.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override { return head; }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override {
    const auto found = objects.find(key(ns, ref));
    if (found == objects.end()) throw Error(ErrorCode::Missing, "missing native namespace fixture object");
    return found->second;
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    std::vector<Bytes> result;
    for (const auto& request : requests) {
      auto bytes = get_object(ns, request.ref);
      if (bytes.size() != request.size) throw Error(ErrorCode::Corrupt, "native fixture object size differs");
      result.push_back(std::move(bytes));
    }
    return result;
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& bytes) override {
    if (content_id(ref.type, ref.protocol, bytes) != ref.id)
      throw Error(ErrorCode::Corrupt, "native namespace fixture object identity differs");
    const auto inserted = objects.emplace(key(ns, ref), bytes);
    if (!inserted.second && inserted.first->second != bytes)
      throw Error(ErrorCode::Conflict, "native namespace fixture immutable replacement");
  }
  HeadWriteResult write_head(const Bytes& bytes) override {
    head = bytes;
    return HeadWriteResult::Acknowledged;
  }
};

class NativeNamespaceJournal final : public Journal {
public:
  std::optional<JournalState> state;
  std::optional<JournalState> load() override { return state; }
  void save(const JournalState& next) override { state = next; }
};

class NativeFileNamespaceTest : public ::testing::Test {
protected:
  const NamespaceIdentity identity{"123e4567-e89b-42d3-a456-426614174000",
                                   "123e4567-e89b-42d3-a456-426614174001"};
  std::shared_ptr<NativeNamespaceStore> store = std::make_shared<NativeNamespaceStore>();
  std::shared_ptr<NativeNamespaceJournal> journal = std::make_shared<NativeNamespaceJournal>();
  Owner owner{store, journal};
  NamespaceRecord file;

  void SetUp() override {
    NamespaceRecord root;
    root.inode.path = root.inode.parent = "/";
    root.inode.name = "/";
    root.inode.row_time = 1;
    root.inode.is_dir = true;
    root.inode.mode = 0750;
    root.inode.uid = 1001; root.inode.gid = 40077; root.inode.nlink = 1;
    root.inode.ctime = root.inode.mtime = root.inode.atime = 1;
    root.inode.metadata = "{}";
    root.manifest.key = {"/", 1};
    root.manifest.published = true;
    root.manifest.phase = StructuredBlobPhase::Published;

    file.inode = root.inode;
    file.inode.path = "/science.h5"; file.inode.parent = "/"; file.inode.name = "science.h5";
    file.inode.row_time = 10; file.inode.is_dir = false; file.inode.mode = 0640;
    file.inode.size = 16; file.inode.format_id = "hdf5";
    file.manifest.key = {file.inode.path, file.inode.row_time};
    file.manifest.format_id = "hdf5";
    file.manifest.payload_size = 16; file.manifest.layout_size = 8;
    file.manifest.published = true; file.manifest.phase = StructuredBlobPhase::Published;
    file.descriptor_id = std::string(64, 'a'); file.seal_id = std::string(64, 'b');

    std::vector<Owner::Seed> seeds{{Table::Current, content_profile_key(), content_profile()}};
    for (const auto& record : {root, file}) {
      const auto bytes = encode_namespace_record(record);
      const auto exact = exact_key(record.inode.path, record.inode.row_time);
      seeds.emplace_back(Table::Exact, exact, bytes);
      seeds.emplace_back(Table::Permission, exact, bytes);
      seeds.emplace_back(Table::Liveness, exact, Bytes{1});
      seeds.emplace_back(Table::Current, record.inode.path, bytes);
      seeds.emplace_back(Table::Current, record.inode.path + std::string(1, '\0') + record.inode.format_id, bytes);
    }
    owner.initialize(identity, seeds);
  }

  NativeFileLocation location() const {
    NativeFileLocation value;
    value.namespace_identity = identity; value.key = file.manifest.key;
    value.descriptor_id = file.descriptor_id; value.seal_id = file.seal_id;
    value.payload_root = std::string(64, 'c'); value.sha256 = std::string(64, 'd');
    value.relative_path = value.sha256 + ".blob"; value.size = 16;
    value.device = 1; value.inode = 2; value.mtime_ns = 3; value.ctime_ns = 4;
    return value;
  }

  void put(const std::string& key, const Bytes& bytes) {
    auto tx = owner.begin("native-namespace-fixture");
    tx->put(Table::Current, key, bytes); tx->commit();
  }

  SourceSelector selector(VersionSelectionKind versions = VersionSelectionKind::Latest) const {
    SourceSelector value;
    value.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
    value.versions.kind = versions;
    return value;
  }

  void validate(Owner& checked) {
    const auto view = checked.view();
    validate_current_index(checked, view, checked.scan(view, Table::Exact));
  }
};

TEST_F(NativeFileNamespaceTest, LocatorNeverBecomesCandidateOrPermissionAncestor) {
  const auto value = location();
  put(native_file_location_key(value.key), encode_native_file_location(value));
  for (const auto versions : {VersionSelectionKind::Latest, VersionSelectionKind::All}) {
    const auto selected = select_namespace(owner, owner.view(), selector(versions), {});
    ASSERT_EQ(selected.sources.size(), 1u);
    EXPECT_EQ(selected.sources.begin()->first, file.manifest.key);
    ASSERT_EQ(selected.parents.size(), 1u);
    ASSERT_TRUE(selected.parents.at("/").has_value());
    EXPECT_EQ(selected.parents.at("/")->inode.mode, 0750);
    EXPECT_EQ(selected.parents.at("/")->inode.uid, 1001);
  }
  EXPECT_NO_THROW(validate(owner));
}

TEST_F(NativeFileNamespaceTest, RecoveryKeepsOrdinaryCurrentMetadataAndAcceptsLocator) {
  const auto value = location();
  put(native_file_location_key(value.key), encode_native_file_location(value));
  Owner recovered(store, journal);
  ASSERT_NO_THROW(recovered.recover());
  EXPECT_NO_THROW(validate(recovered));
  const auto selected = select_namespace(recovered, recovered.view(), selector(), {});
  ASSERT_EQ(selected.sources.size(), 1u);
  EXPECT_EQ(selected.sources.begin()->second.inode.mode, file.inode.mode);
  EXPECT_EQ(selected.sources.begin()->second.descriptor_id, file.descriptor_id);
}

TEST_F(NativeFileNamespaceTest, ForeignNamespaceLocatorIsRejected) {
  auto value = location();
  value.namespace_identity.incarnation = "123e4567-e89b-42d3-a456-426614174002";
  put(native_file_location_key(value.key), encode_native_file_location(value));
  EXPECT_THROW(select_namespace(owner, owner.view(), selector(), {}), Error);
  EXPECT_THROW(validate(owner), Error);
}

TEST_F(NativeFileNamespaceTest, ReservedKeyMustMatchEncodedExactVersion) {
  const auto value = location();
  put(native_file_location_key({value.key.path, value.key.version + 1}), encode_native_file_location(value));
  EXPECT_THROW(select_namespace(owner, owner.view(), selector(), {}), Error);
  EXPECT_THROW(validate(owner), Error);
}

TEST_F(NativeFileNamespaceTest, MalformedReservedRecordAndUnknownExtensionAreRejected) {
  put(native_file_location_key(file.manifest.key), Bytes{1, 2, 3});
  EXPECT_THROW(select_namespace(owner, owner.view(), selector(), {}), Error);
  EXPECT_THROW(validate(owner), Error);
  auto tx = owner.begin("replace-malformed-fixture");
  tx->erase(Table::Current, native_file_location_key(file.manifest.key));
  tx->put(Table::Current, std::string(1, '\1') + "unknown-extension", Bytes{1});
  tx->commit();
  EXPECT_THROW(select_namespace(owner, owner.view(), selector(), {}), Error);
  EXPECT_THROW(validate(owner), Error);
}

TEST_F(NativeFileNamespaceTest, RemovedVersionOrphanLocatorIsNotRetargetedOrSelected) {
  auto value = location();
  value.key = {"/removed.h5", 9};
  put(native_file_location_key(value.key), encode_native_file_location(value));
  EXPECT_NO_THROW(validate(owner));
  const auto selected = select_namespace(owner, owner.view(), selector(), {});
  ASSERT_EQ(selected.sources.size(), 1u);
  EXPECT_EQ(selected.sources.begin()->first, file.manifest.key);
  EXPECT_EQ(owner.get(owner.view(), Table::Current, native_file_location_key(value.key)),
    std::optional<Bytes>(encode_native_file_location(value)));
}

TEST_F(NativeFileNamespaceTest, LocatorCannotHideMissingOrdinaryLatestIndex) {
  const auto value = location();
  put(native_file_location_key(value.key), encode_native_file_location(value));
  auto tx = owner.begin("remove-current-index-fixture");
  tx->erase(Table::Current, file.inode.path); tx->commit();
  EXPECT_THROW(validate(owner), Error);
}

} // namespace
} // namespace iotdbfs::versioning
