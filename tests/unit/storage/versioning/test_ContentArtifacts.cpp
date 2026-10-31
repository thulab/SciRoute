#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "iotdbfs/DatasetSubstructureIndex.h"

#include <gtest/gtest.h>
#include <openssl/sha.h>
#include <algorithm>
#include <iomanip>
#include <map>
#include <sstream>
#include <tuple>

namespace iotdbfs::versioning {
namespace {
const NamespaceIdentity ns{"123e4567-e89b-42d3-a456-426614174000",
                           "123e4567-e89b-42d3-a456-426614174001"};
const NamespaceIdentity foreign{"123e4567-e89b-42d3-a456-426614174002",
                                "123e4567-e89b-42d3-a456-426614174003"};
const std::string layout_protocol = "TEST-LAYOUT/1\n";
std::string sha(const Bytes& bytes) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(bytes.data(), bytes.size(), digest);
  std::ostringstream out;
  for (auto b : digest) out << std::hex << std::setfill('0') << std::setw(2) << unsigned(b);
  return out.str();
}
Bytes layout_bytes(std::size_t size = 128) {
  Bytes result(size);
  for (std::size_t i = 0; i < size; ++i) result[i] = (i * 31 + i / kContentChunkBytes) % 251;
  std::copy(layout_protocol.begin(), layout_protocol.end(), result.begin());
  return result;
}
class ArtifactStore final : public Store {
public:
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  std::map<Key, Bytes> objects;
  std::optional<Bytes> head;
  std::map<std::string, std::size_t> reads;
  std::size_t batches{0};
  static Key key(const NamespaceIdentity& identity, const ObjectRef& ref) {
    return {identity.namespace_id, identity.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override { return head; }
  Bytes get_object(const NamespaceIdentity& identity, const ObjectRef& ref) override {
    ++reads[ref.type];
    auto it = objects.find(key(identity, ref));
    if (it == objects.end()) throw Error(ErrorCode::Missing, "missing test object");
    return it->second;
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& identity,
      const std::vector<ObjectReadRequest>& requests) override {
    ++batches;
    std::vector<Bytes> result;
    for (const auto& request : requests) {
      auto value = get_object(identity, request.ref);
      if (value.size() != request.size) throw Error(ErrorCode::Corrupt, "batch object size mismatch");
      result.push_back(std::move(value));
    }
    return result;
  }
  void put_object(const NamespaceIdentity& identity, const ObjectRef& ref, const Bytes& bytes) override {
    if (content_id(ref.type, ref.protocol, bytes) != ref.id)
      throw Error(ErrorCode::Corrupt, "bad test object digest");
    auto inserted = objects.emplace(key(identity, ref), bytes);
    if (!inserted.second && inserted.first->second != bytes)
      throw Error(ErrorCode::Conflict, "immutable object changed");
  }
  HeadWriteResult write_head(const Bytes& bytes) override {
    head = bytes; return HeadWriteResult::Acknowledged;
  }
};
class ArtifactJournal final : public Journal {
public:
  std::optional<JournalState> state;
  std::optional<JournalState> load() override { return state; }
  void save(const JournalState& value) override { state = value; }
};
class ContentArtifactsTest : public ::testing::Test {
protected:
  std::shared_ptr<ArtifactStore> store = std::make_shared<ArtifactStore>();
  std::shared_ptr<ArtifactJournal> journal = std::make_shared<ArtifactJournal>();
  std::shared_ptr<Owner> owner = std::make_shared<Owner>(store, journal);
  std::size_t validations{0};
  bool validator_failure{false};
  LayoutValidator validator() {
    return [this](const std::string& format, const Bytes& bytes, std::uint64_t size) {
      ++validations;
      if (validator_failure || format != "fake" || size != 4096 ||
          bytes.size() < layout_protocol.size() ||
          !std::equal(layout_protocol.begin(), layout_protocol.end(), bytes.begin()))
        return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument, "invalid fixture layout");
      return StructuredBlobStatus::success();
    };
  }
  std::unique_ptr<ContentArtifacts> content;
  void SetUp() override {
    owner->initialize(ns, {{Table::Current, content_profile_key(), content_profile()}});
    content = std::make_unique<ContentArtifacts>(owner, validator());
  }
  StructuredBlobManifest manifest(const Bytes& layout, std::int64_t version = 1) {
    StructuredBlobManifest result;
    result.key = {"/science/a.fake", version}; result.format_id = "fake";
    result.payload_size = 4096; result.layout_size = layout.size();
    result.published = true; result.phase = StructuredBlobPhase::Published;
    result.metadata["description"] = MetadataValue("shared metadata");
    return result;
  }
  DatasetCatalogArtifact catalog_for(const Bytes& layout) {
    DatasetCatalogArtifact result;
    result.format_id = "fake"; result.layout_protocol = layout_protocol;
    result.layout_digest = sha(layout);
    DatasetCatalogRecord row; row.object_id = "dataset-1"; row.canonical_path = "/values";
    row.attributes.push_back({"units", DatasetAttributeState::Complete,
      DatasetAttributeValue(std::string("K")), {}, {}});
    result.records.push_back(std::move(row)); return result;
  }
  ContentBinding build(StructuredBlobManifest m, const Bytes& layout,
      std::optional<DatasetCatalogArtifact> catalog = {}) {
    if (catalog) {
      auto status = seal_dataset_substructure_index(m, *catalog);
      if (!status.ok()) throw std::runtime_error(status.message());
    }
    auto tx = owner->begin("test-content");
    const auto root = payload_root(*tx, m.payload_size);
    auto binding = content->build(*tx, m, layout, catalog, root);
    tx->commit(); return binding;
  }
  ContentId payload_root(Transaction& tx, std::uint64_t size) {
    PayloadArtifacts payload(owner);
    return payload.build(tx, size, [](std::uint64_t, std::uint64_t length) {
      return Bytes(length, 0x37);
    });
  }
  std::shared_ptr<const VerifiedFileContent> verify(const ContentBinding& binding,
      const StructuredBlobManifest& m, bool deep = false, std::uint64_t* budget = nullptr) {
    return content->verify(binding, m.key, m.format_id, m.payload_size, m.layout_size, deep, budget);
  }
  ArtifactStore::Key object_key(const std::string& type, const ContentId& id) {
    return ArtifactStore::key(ns, {type, "iotdbfs-" + type + (type == "file-descriptor" ? "/2" : "/1"), id});
  }
  ContentBinding forged_graph(FileDescriptor descriptor, const FormatProof& proof, VersionSeal seal) {
    auto write = [&](const std::string& type, const Bytes& bytes) {
      const auto ref = object_ref(type, bytes);
      store->put_object(ns, ref, bytes);
      return ref.id;
    };
    descriptor.proof_id = write("format-proof", encode_format_proof(proof));
    seal.proof_id = descriptor.proof_id;
    seal.descriptor_id = write("file-descriptor", encode_file_descriptor(descriptor));
    return {seal.descriptor_id, write("version-seal", encode_version_seal(seal))};
  }
};

TEST_F(ContentArtifactsTest, GenesisProfileIsPresentAndSurvivesRecovery) {
  const auto profile = owner->get(owner->view(), Table::Current, content_profile_key());
  ASSERT_TRUE(profile); EXPECT_EQ(*profile, content_profile());
  Owner reopened(store, journal); reopened.recover();
  EXPECT_EQ(reopened.get(reopened.view(), Table::Current, content_profile_key()), profile);
  EXPECT_EQ(owner->view().head.sequence, 0u);
}

TEST_F(ContentArtifactsTest, SharedDescriptorsProofsAndParsesKeepPerVersionSeals) {
  const auto layout = layout_bytes(); auto one = manifest(layout); auto two = manifest(layout, 2);
  two.key.path = "/other/renamed.fake";
  const auto catalog = catalog_for(layout);
  const auto first = build(one, layout, catalog); const auto second = build(two, layout, catalog);
  EXPECT_EQ(first.descriptor_id, second.descriptor_id); EXPECT_NE(first.seal_id, second.seal_id);
  EXPECT_EQ(validations, 1u);
  content->clear_cache();
  const auto a = verify(first, one); const auto counters = content->stats();
  const auto b = verify(second, two); const auto warm = content->stats();
  EXPECT_EQ(a->descriptor.proof_id, b->descriptor.proof_id);
  EXPECT_EQ(counters.metadata_parses, warm.metadata_parses);
  EXPECT_EQ(counters.catalog_parses, warm.catalog_parses);
  EXPECT_EQ(warm.metadata_parses, 1u); EXPECT_EQ(warm.catalog_parses, 1u);
  EXPECT_EQ(content->layout_identity(*a), content->layout_identity(*b));
  ASSERT_TRUE(content->catalog(*a)); EXPECT_EQ(content->catalog(*a), content->catalog(*b));
  EXPECT_THROW(verify(first, two), Error);
  EXPECT_NE(std::get<std::string>(a->metadata.at("__iotdbfs.dataset_index.sha256").value()),
            std::get<std::string>(b->metadata.at("__iotdbfs.dataset_index.sha256").value()));
}

TEST_F(ContentArtifactsTest, LayoutRangeReadsCrossChunkAndIndexFanoutBoundaries) {
  const auto layout = layout_bytes(kContentChunkBytes * kPageEntries + 17);
  const auto m = manifest(layout); const auto binding = build(m, layout);
  const auto file = verify(binding, m); content->clear_cache(); store->reads.clear();
  const auto offset = kContentChunkBytes * kPageEntries - 3;
  EXPECT_EQ(content->read_layout(*file, offset, 20), Bytes(layout.begin() + offset, layout.end()));
  EXPECT_EQ(store->reads["layout-object"], 1u);
  EXPECT_EQ(store->reads["layout-index"], 3u); // root + two leaves
  EXPECT_EQ(store->reads["layout-chunk"], 2u);
  const auto reads = content->stats().object_reads;
  EXPECT_EQ(content->read_layout(*file, offset, 20), Bytes(layout.begin() + offset, layout.end()));
  EXPECT_EQ(content->stats().object_reads, reads);
  EXPECT_EQ(content->read_layout(*file, layout.size(), 0), Bytes{});
  EXPECT_THROW(content->read_layout(*file, layout.size(), 1), Error);
}

TEST_F(ContentArtifactsTest, MissingAndCorruptObjectsNeverBecomeSuccessfulCacheEntries) {
  const auto layout = layout_bytes(); const auto m = manifest(layout); const auto binding = build(m, layout);
  content->clear_cache(); const auto key = object_key("file-descriptor", binding.descriptor_id);
  store->reads.clear();
  auto bytes = store->objects.at(key); store->objects.erase(key);
  EXPECT_THROW(verify(binding, m), Error);
  EXPECT_THROW(verify(binding, m), Error);
  EXPECT_EQ(store->reads["file-descriptor"], 2u);
  auto corrupt = bytes; corrupt.back() ^= 1; store->objects[key] = corrupt;
  EXPECT_THROW(verify(binding, m), Error);
  store->objects[key] = bytes;
  EXPECT_NO_THROW(verify(binding, m));
}

TEST_F(ContentArtifactsTest, WrongNamespacePathFormatAndSizesAreRejectedIncludingWarmHits) {
  const auto layout = layout_bytes(); const auto m = manifest(layout); const auto binding = build(m, layout);
  const auto file = verify(binding, m);
  for (int field = 0; field != 5; ++field) {
    auto other = m;
    if (field == 0) other.key.path = "/foreign";
    if (field == 1) ++other.key.version;
    if (field == 2) other.format_id = "other";
    if (field == 3) ++other.payload_size;
    if (field == 4) ++other.layout_size;
    EXPECT_THROW(verify(binding, other), Error);
  }
  auto other_store = std::make_shared<ArtifactStore>(); auto other_journal = std::make_shared<ArtifactJournal>();
  auto other_owner = std::make_shared<Owner>(other_store, other_journal); other_owner->initialize(foreign);
  ContentArtifacts other(other_owner, validator());
  EXPECT_THROW(other.verify(binding, m.key, m.format_id, m.payload_size, m.layout_size), Error);
  EXPECT_THROW(other.read_layout(*file, 0, 1), Error);
  EXPECT_THROW(other.catalog(*file), Error);
  EXPECT_THROW(other.layout_identity(*file), Error);
}

TEST_F(ContentArtifactsTest, FailedLayoutValidationIsRetriedAndNotCached) {
  const auto layout = layout_bytes(); const auto m = manifest(layout);
  validator_failure = true;
  for (int attempt = 0; attempt != 2; ++attempt) {
    auto tx = owner->begin("rejected-layout");
    EXPECT_THROW(content->build(*tx, m, layout, {}, payload_root(*tx, m.payload_size)), Error);
    tx->abort();
  }
  EXPECT_EQ(validations, 2u); EXPECT_EQ(owner->view().head.sequence, 0u);
  validator_failure = false; EXPECT_NO_THROW(build(m, layout)); EXPECT_EQ(validations, 3u);
}

TEST_F(ContentArtifactsTest, CacheHitsStillChargeCallerBudgetAndClearDropsParsedResults) {
  const auto layout = layout_bytes(); const auto m = manifest(layout);
  const auto binding = build(m, layout, catalog_for(layout));
  verify(binding, m);
  std::uint64_t budget = kMetadataBytes;
  EXPECT_THROW(verify(binding, m, false, &budget), Error);
  EXPECT_EQ(budget, kMetadataBytes);
  const auto before = content->stats(); EXPECT_GT(before.cache_entries, 0u);
  content->clear_cache(); EXPECT_EQ(content->stats().cache_entries, 0u); EXPECT_EQ(content->stats().cache_bytes, 0u);
  verify(binding, m); const auto after = content->stats();
  EXPECT_GT(after.object_reads, before.object_reads);
  EXPECT_EQ(after.metadata_parses, before.metadata_parses + 1);
  EXPECT_EQ(after.catalog_parses, before.catalog_parses + 1);
}

TEST_F(ContentArtifactsTest, SmallUnifiedLruIsBoundedAndEvictsWithoutChangingResults) {
  content = std::make_unique<ContentArtifacts>(owner, validator(), 8192);
  const auto layout = layout_bytes(kContentChunkBytes + 10); const auto m = manifest(layout);
  const auto binding = build(m, layout); const auto file = verify(binding, m);
  EXPECT_EQ(content->read_layout(*file, 0, layout.size()), layout);
  EXPECT_LE(content->stats().cache_bytes, 8192u); EXPECT_GT(content->stats().evictions, 0u);
  EXPECT_EQ(content->read_layout(*file, kContentChunkBytes - 2, 5),
    Bytes(layout.begin() + kContentChunkBytes - 2, layout.begin() + kContentChunkBytes + 3));
  EXPECT_LE(content->stats().cache_bytes, 8192u);
}

TEST_F(ContentArtifactsTest, DeepVerificationDetectsCorruptionHiddenByWarmCache) {
  const auto layout = layout_bytes(); const auto m = manifest(layout); const auto binding = build(m, layout);
  auto file = verify(binding, m); ASSERT_EQ(content->read_layout(*file, 0, layout.size()), layout);
  auto chunk = std::find_if(store->objects.begin(), store->objects.end(), [](const auto& item) {
    return std::get<2>(item.first) == "layout-chunk";
  });
  ASSERT_NE(chunk, store->objects.end()); chunk->second.back() ^= 1;
  EXPECT_THROW(verify(binding, m, true), Error);
  content->clear_cache(); EXPECT_THROW(content->read_layout(*file, 0, layout.size()), Error);
}

TEST_F(ContentArtifactsTest, BytePassThroughHasNoFormatEvidenceAndRejectsNonemptyLayout) {
  StructuredBlobManifest m; m.key = {"/plain", 1}; m.payload_size = 4;
  const auto binding = build(m, {}); const auto file = verify(binding, m, true);
  EXPECT_TRUE(file->descriptor.format.empty()); EXPECT_FALSE(content->catalog(*file));
  EXPECT_TRUE(content->read_layout(*file, 0, 0).empty());
  m.layout_size = layout_protocol.size(); Bytes layout(layout_protocol.begin(), layout_protocol.end());
  auto tx = owner->begin("bad-plain");
  EXPECT_THROW(content->build(*tx, m, layout, {}, payload_root(*tx, m.payload_size)), Error);
  tx->abort();
}

TEST_F(ContentArtifactsTest, WarmCatalogStillChecksClaimedProofByteCount) {
  const auto layout = layout_bytes(); const auto m = manifest(layout);
  const auto binding = build(m, layout, catalog_for(layout));
  const auto file = verify(binding, m);
  ASSERT_TRUE(content->catalog(*file));
  const auto parses = content->stats().catalog_parses;
  auto forged = *file;
  ++forged.proof.catalog_size;
  EXPECT_THROW(content->catalog(forged), Error);
  EXPECT_EQ(content->stats().catalog_parses, parses);
  EXPECT_NO_THROW(content->catalog(*file));
}

TEST_F(ContentArtifactsTest, DeepVerificationBindsProofProtocolToActualLayoutWithoutCatalog) {
  const auto layout = layout_bytes(); const auto m = manifest(layout);
  const auto binding = build(m, layout); const auto file = verify(binding, m);
  ASSERT_TRUE(file->proof.catalog_id.empty());
  auto proof = file->proof;
  proof.layout_protocol = "FORGED-LAYOUT/99\n";
  const auto forged = forged_graph(file->descriptor, proof, file->seal);
  // The graph's IDs and lengths are internally consistent. Deep recovery must
  // additionally compare the actual protocol bytes, even without a catalog.
  EXPECT_NO_THROW(verify(forged, m));
  EXPECT_THROW(verify(forged, m, true), Error);
}

TEST_F(ContentArtifactsTest, ForgedBytePassThroughCannotCarrySharedFormatEvidence) {
  // Empty layout isolates the metadata invariant from the independent rule
  // that byte pass-through cannot have a layout.
  content = std::make_unique<ContentArtifacts>(owner,
    [](const std::string&, const Bytes&, std::uint64_t) { return StructuredBlobStatus::success(); });
  const Bytes empty_layout;
  auto m = manifest(empty_layout);
  const auto binding = build(m, empty_layout); const auto file = verify(binding, m);
  ASSERT_FALSE(file->metadata.empty());
  auto descriptor = file->descriptor; auto proof = file->proof; auto seal = file->seal;
  descriptor.format.clear(); proof.format.clear(); seal.format.clear();
  const auto forged = forged_graph(descriptor, proof, seal);
  m.format_id.reset();
  EXPECT_THROW(verify(forged, m), Error);

  const auto layout = layout_bytes(); auto indexed = manifest(layout, 2);
  const auto indexed_binding = build(indexed, layout, catalog_for(layout));
  const auto indexed_file = verify(indexed_binding, indexed);
  descriptor = indexed_file->descriptor; proof = indexed_file->proof; seal = indexed_file->seal;
  descriptor.format.clear(); proof.format.clear(); seal.format.clear();
  const auto forged_catalog = forged_graph(descriptor, proof, seal);
  indexed.format_id.reset();
  EXPECT_THROW(verify(forged_catalog, indexed), Error);
}

TEST(ContentArtifactsCodec, CanonicalRoundTripsRejectProtocolRulesIdsAndTrailingBytes) {
  FileDescriptor d;
  d.format = "fake"; d.payload_size = 4096; d.layout_size = 20;
  d.metadata_id = std::string(64, 'a'); d.layout_id = std::string(64, 'b');
  d.proof_id = std::string(64, 'c'); d.payload_root = std::string(64, 'd');
  const auto descriptor = encode_file_descriptor(d);
  EXPECT_EQ(encode_file_descriptor(decode_file_descriptor(descriptor)), descriptor);
  FormatProof p; p.format = "fake"; p.layout_protocol = layout_protocol; p.layout_size = 20;
  p.layout_id = std::string(64, 'a'); p.layout_digest = std::string(64, 'b'); p.payload_size = 4096;
  const auto proof = encode_format_proof(p);
  EXPECT_EQ(encode_format_proof(decode_format_proof(proof)), proof);
  VersionSeal s{ns, {"/science/a", 42}, "fake", 4096, std::string(64, 'c'), std::string(64, 'd'), {}};
  const auto seal = encode_version_seal(s);
  EXPECT_EQ(encode_version_seal(decode_version_seal(seal)), seal);
  auto bad = descriptor; bad.push_back(0); EXPECT_THROW(decode_file_descriptor(bad), Error);
  bad = proof; bad.pop_back(); EXPECT_THROW(decode_format_proof(bad), Error);
  bad = seal; bad[4] ^= 1; EXPECT_THROW(decode_version_seal(bad), Error);
  p.rules = "iotdbfs-content-build/1"; EXPECT_THROW(encode_format_proof(p), Error);
  d.metadata_id = std::string(64, 'A'); EXPECT_THROW(encode_file_descriptor(d), Error);
  s.key.path = "/science/../a"; EXPECT_THROW(encode_version_seal(s), Error);
}
} // namespace
} // namespace iotdbfs::versioning
