#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include <map>
#include <tuple>
#include <future>

namespace iotdbfs::versioning {
namespace {
constexpr std::uint64_t block_bytes = 1024 * 1024;
const NamespaceIdentity identity{"123e4567-e89b-42d3-a456-426614174000",
                                 "123e4567-e89b-42d3-a456-426614174001"};

class PayloadStore final : public Store {
public:
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  std::map<Key, Bytes> objects;
  std::optional<Bytes> head;
  std::vector<std::vector<ObjectReadRequest>> batches;
  std::map<std::string, unsigned> scalar_reads;
  bool short_batch{false}, missing_batch_value{false};
  std::string fail_put_type, fail_read_type;
  unsigned successful_puts_before_failure{0};
  static Key key(const NamespaceIdentity& ns, const ObjectRef& ref) {
    return {ns.namespace_id, ns.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override { return head; }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override {
    ++scalar_reads[ref.type];
    if (ref.type == fail_read_type)
      throw Error(ErrorCode::Unavailable, "injected artifact readback failure");
    auto found = objects.find(key(ns, ref));
    if (found == objects.end()) throw Error(ErrorCode::Missing, "test object absent");
    return found->second;
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    batches.push_back(requests);
    std::vector<Bytes> values;
    for (const auto& request : requests) {
      auto value = get_object(ns, request.ref);
      if (value.size() != request.size) throw Error(ErrorCode::Corrupt, "test size mismatch");
      values.push_back(std::move(value));
    }
    if (short_batch && !values.empty() && !values.back().empty()) values.back().pop_back();
    if (missing_batch_value && !values.empty()) values.pop_back();
    return values;
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& value) override {
    if (ref.type == fail_put_type) {
      if (!successful_puts_before_failure)
        throw Error(ErrorCode::Unavailable, "injected artifact write failure");
      --successful_puts_before_failure;
    }
    if (content_id(ref.type, ref.protocol, value) != ref.id)
      throw Error(ErrorCode::Corrupt, "test digest mismatch");
    auto inserted = objects.emplace(key(ns, ref), value);
    if (!inserted.second && inserted.first->second != value)
      throw Error(ErrorCode::Conflict, "test immutable replacement");
  }
  HeadWriteResult write_head(const Bytes& value) override {
    head = value;
    return HeadWriteResult::Acknowledged;
  }
  std::size_t count(const std::string& type) const {
    return std::count_if(objects.begin(), objects.end(), [&](const auto& object) {
      return std::get<2>(object.first) == type;
    });
  }
};
class PayloadJournal final : public Journal {
public:
  std::optional<JournalState> state;
  std::optional<JournalState> load() override { return state; }
  void save(const JournalState& value) override { state = value; }
};

Bytes fixture_bytes(std::size_t size) {
  Bytes value(size);
  for (std::size_t offset = 0; offset < size; offset += block_bytes)
    std::fill(value.begin() + offset, value.begin() + std::min<std::size_t>(size, offset + block_bytes),
              static_cast<std::uint8_t>(11 + offset / block_bytes));
  return value;
}
Bytes slice(const Bytes& value, std::uint64_t offset, std::uint64_t length) {
  return Bytes(value.begin() + offset, value.begin() + offset + length);
}
class PayloadArtifactsTest : public ::testing::Test {
protected:
  std::shared_ptr<PayloadStore> store = std::make_shared<PayloadStore>();
  std::shared_ptr<PayloadJournal> journal = std::make_shared<PayloadJournal>();
  std::shared_ptr<Owner> owner = std::make_shared<Owner>(store, journal, [] {
    OwnerConfig config;config.payload_cache_bytes=0;return config;
  }()); // Mapping/integrity tests below isolate physical reads from residency.
  std::unique_ptr<PayloadArtifacts> payload;
  void SetUp() override {
    owner->initialize(identity);
    payload = std::make_unique<PayloadArtifacts>(owner);
  }
  ContentId build(const Bytes& value) {
    auto tx = owner->begin("payload-fixture");
    auto root = payload->build(*tx, value.size(), [&](std::uint64_t offset, std::uint64_t length) {
      EXPECT_LE(length, block_bytes);
      EXPECT_LE(offset, value.size());
      EXPECT_LE(length, value.size() - offset);
      return slice(value, offset, length);
    });
    tx->commit();
    return root;
  }
  void cold() {
    payload = std::make_unique<PayloadArtifacts>(owner);
    owner->clear_read_cache();
    store->batches.clear();
    store->scalar_reads.clear();
  }
  ContentId forged_map(const ExtentPage& page, std::uint64_t size) {
    const auto bytes = encode_extent_page(page);
    const auto ref = object_ref("extent-page", bytes);
    store->put_object(identity, ref, bytes);
    PayloadMap map{size, page.extent_count, static_cast<std::uint8_t>(page.level + 1), bytes.size(), ref.id};
    const auto encoded = encode_payload_map(map);
    const auto root = object_ref("payload-map", encoded);
    store->put_object(identity, root, encoded);
    return root.id;
  }
};

TEST_F(PayloadArtifactsTest, EmptyAndFixedBlockBoundaryFilesRoundTripWithoutPadding) {
  for (std::size_t size : {std::size_t(0), std::size_t(1), std::size_t(block_bytes - 1),
       std::size_t(block_bytes), std::size_t(block_bytes + 1), std::size_t(3 * block_bytes + 17)}) {
    SCOPED_TRACE(static_cast<int>(size));
    const auto value = fixture_bytes(size);
    const auto root = build(value);
    EXPECT_NO_THROW(payload->verify(root, size, true));
    if (size) {
      auto result = payload->read_ranges(root, size, {{0, size}});
      ASSERT_EQ(result.size(), 1u);
      EXPECT_EQ(result[0], value);
      EXPECT_EQ(payload->read_ranges(root, size, {{size - 1, 1}})[0], slice(value, size - 1, 1));
    } else {
      EXPECT_EQ(store->count("payload-block"), 0u);
    }
  }
}

TEST_F(PayloadArtifactsTest, IdenticalFilesShareMapAndOneChangedBlockKeepsAllOtherBlocks) {
  auto value = fixture_bytes(3 * block_bytes + 17);
  const auto first = build(value);
  ASSERT_EQ(store->count("payload-block"), 4u);
  const auto second = build(value);
  EXPECT_EQ(first, second);
  EXPECT_EQ(store->count("payload-block"), 4u);
  value[block_bytes + 37] ^= 1;
  const auto changed = build(value);
  EXPECT_NE(changed, first);
  EXPECT_EQ(store->count("payload-block"), 5u);
  EXPECT_EQ(payload->read_ranges(changed, value.size(), {{0, value.size()}})[0], value);
  const auto original = fixture_bytes(value.size());
  EXPECT_EQ(payload->read_ranges(first, original.size(), {{0, original.size()}})[0], original);
}

TEST_F(PayloadArtifactsTest, UnsortedOverlappingAndRepeatedRangesKeepCallerOrderAndBatchUniqueBlocks) {
  const auto value = fixture_bytes(3 * block_bytes + 17);
  const auto root = build(value);
  cold();
  const StructuredBlobPayloadRanges ranges{{2 * block_bytes + 9, 20}, {block_bytes - 3, 9},
    {0, 7}, {block_bytes - 3, 9}, {value.size() - 3, 3}};
  const auto result = payload->read_ranges(root, value.size(), ranges);
  ASSERT_EQ(result.size(), ranges.size());
  for (std::size_t i = 0; i < ranges.size(); ++i)
    EXPECT_EQ(result[i], slice(value, ranges[i].offset, ranges[i].length));
  std::vector<ContentId> blocks;
  for (const auto& batch : store->batches) {
    std::uint64_t bytes = 0;
    EXPECT_LE(batch.size(), 64u);
    for (const auto& request : batch) {
      bytes += request.size;
      if (request.ref.type == "payload-block") blocks.push_back(request.ref.id);
    }
    EXPECT_LE(bytes, 8 * block_bytes);
  }
  ASSERT_EQ(blocks.size(), 4u);
  std::sort(blocks.begin(), blocks.end());
  EXPECT_EQ(std::unique(blocks.begin(), blocks.end()), blocks.end());
}

TEST_F(PayloadArtifactsTest, InvalidAndOverBudgetBatchesFailBeforeReadingBlocks) {
  const auto value = fixture_bytes(9 * block_bytes);
  const auto root = build(value);
  cold();
  for (const auto& ranges : std::vector<StructuredBlobPayloadRanges>{ {}, {{0, 0}},
      {{value.size(), 1}}, {{value.size() + 1, 1}},
      {{std::numeric_limits<std::uint64_t>::max(), 2}}, {{0, 8 * block_bytes + 1}},
      StructuredBlobPayloadRanges(65, {0, 1}) }) {
    EXPECT_THROW(payload->read_ranges(root, value.size(), ranges), Error);
  }
  for (const auto& batch : store->batches)
    for (const auto& request : batch) EXPECT_NE(request.ref.type, "payload-block");
  const auto result = payload->read_ranges(root, value.size(), {{0, 8 * block_bytes}});
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], slice(value, 0, 8 * block_bytes));
}

TEST_F(PayloadArtifactsTest, MissingCorruptAndShortBlocksNeverBecomeSuccessfulReads) {
  const auto value = fixture_bytes(2 * block_bytes + 17);
  const auto root = build(value);
  auto block = std::find_if(store->objects.begin(), store->objects.end(), [](const auto& object) {
    return std::get<2>(object.first) == "payload-block" && object.second.size() == 17;
  });
  ASSERT_NE(block, store->objects.end());
  const auto key = block->first;
  const auto original = block->second;
  store->objects.erase(block);
  cold();
  EXPECT_THROW(payload->read_ranges(root, value.size(), {{value.size() - 1, 1}}), Error);
  store->objects.emplace(key, original);
  store->objects.at(key).back() ^= 1;
  cold();
  EXPECT_THROW(payload->read_ranges(root, value.size(), {{value.size() - 1, 1}}), Error);
  EXPECT_THROW(payload->verify(root, value.size(), true), Error);
  store->objects.at(key) = original;
  cold();
  store->short_batch = true;
  EXPECT_THROW(payload->read_ranges(root, value.size(), {{value.size() - 1, 1}}), Error);
  store->short_batch = false;
  store->missing_batch_value = true;
  cold();
  EXPECT_THROW(payload->read_ranges(root, value.size(), {{value.size() - 1, 1}}), Error);
  store->missing_batch_value = false;
  cold();
  EXPECT_EQ(payload->read_ranges(root, value.size(), {{value.size() - 1, 1}})[0], slice(value, value.size() - 1, 1));
}

TEST_F(PayloadArtifactsTest, DirectMapLookupNeverReadsHistoricalCommitsOrUnrelatedBlocks) {
  const auto value = fixture_bytes(3 * block_bytes + 17);
  const auto root = build(value);
  for (unsigned version = 0; version < 40; ++version) {
    auto tx = owner->begin("unrelated-history");
    tx->put(Table::Current, "/history", Bytes{static_cast<std::uint8_t>(version)});
    tx->commit();
  }
  cold();
  EXPECT_EQ(payload->read_ranges(root, value.size(), {{block_bytes + 1, 1}})[0], slice(value, block_bytes + 1, 1));
  EXPECT_EQ(store->scalar_reads["commit"], 0u);
  std::size_t block_reads = 0;
  for (const auto& batch : store->batches)
    for (const auto& request : batch) block_reads += request.ref.type == "payload-block";
  EXPECT_EQ(block_reads, 1u);
}

TEST(PayloadArtifactsCodec, CanonicalRoundTripsAndFixedBlockRulesRejectMalformedValues) {
  ExtentPage page;
  page.end = 17; page.extent_count = 1;
  page.extents.push_back({0, 17, 0, 17, std::string(64, 'a')});
  const auto encoded = encode_extent_page(page);
  EXPECT_EQ(encode_extent_page(decode_extent_page(encoded)), encoded);
  PayloadMap map{17, 1, 1, encoded.size(), std::string(64, 'b')};
  const auto root = encode_payload_map(map);
  EXPECT_EQ(root.size(), 93u);
  EXPECT_EQ(encode_payload_map(decode_payload_map(root)), root);
  auto trailing = root; trailing.push_back(0);
  EXPECT_THROW(decode_payload_map(trailing), Error);
  auto truncated = encoded; truncated.pop_back();
  EXPECT_THROW(decode_extent_page(truncated), Error);
  for (unsigned field = 0; field < 6; ++field) {
    auto bad = page;
    switch (field) {
      case 0: bad.extents[0].content_offset = 1; break;
      case 1: bad.extents[0].logical_offset = 1; break;
      case 2: bad.extents[0].content_size = 18; break;
      case 3: bad.extents[0].length = 0; break;
      case 4: bad.extents[0].content_id = std::string(64, 'A'); break;
      case 5: bad.extent_count = 2; break;
    }
    EXPECT_THROW(encode_extent_page(bad), Error);
  }
  page.extents[0].length = page.extents[0].content_size = block_bytes + 1;
  page.end = block_bytes + 1;
  EXPECT_THROW(encode_extent_page(page), Error);
  map.height = 9;
  EXPECT_THROW(encode_payload_map(map), Error);
  map.height = 1; map.payload_size = 0;
  EXPECT_THROW(encode_payload_map(map), Error);
}

TEST_F(PayloadArtifactsTest, MapSizeAndAuthenticatedChildBoundsCannotContradictEachOther) {
  const Bytes data(17, 9);
  const auto root = build(data);
  EXPECT_THROW(payload->verify(root, 18), Error);
  auto block = std::find_if(store->objects.begin(), store->objects.end(), [](const auto& object) {
    return std::get<2>(object.first) == "payload-block";
  });
  ASSERT_NE(block, store->objects.end());
  ExtentPage leaf;
  leaf.end = 17; leaf.extent_count = 1;
  leaf.extents.push_back({0, 17, 0, 17, std::get<4>(block->first)});
  const auto leaf_bytes = encode_extent_page(leaf);
  const auto leaf_ref = object_ref("extent-page", leaf_bytes);
  store->put_object(identity, leaf_ref, leaf_bytes);
  ExtentPage parent;
  parent.level = 1; parent.end = 18; parent.extent_count = 1;
  // The parent authenticates a plausible but false child endpoint. Merely
  // trusting the parent would permit an uncovered final byte in the file.
  parent.children.push_back({0, 18, 1, leaf_bytes.size(), leaf_ref.id});
  const auto wrong_bounds = forged_map(parent, 18);
  cold();
  EXPECT_THROW(payload->verify(wrong_bounds, 18, true), Error);
  EXPECT_THROW(payload->read_ranges(wrong_bounds, 18, {{17, 1}}), Error);
  parent.end = 17; parent.children[0].end = 17; parent.level = 2;
  const auto wrong_level = forged_map(parent, 17);
  cold();
  EXPECT_THROW(payload->verify(wrong_level, 17, true), Error);
  parent.level = 1; ++parent.children[0].encoded_size;
  const auto wrong_length = forged_map(parent, 17);
  cold();
  EXPECT_THROW(payload->verify(wrong_length, 17, true), Error);
}

TEST_F(PayloadArtifactsTest, PayloadBytesNeverConsumeMetadataLedgerButWarmMapStillChargesControlFees) {
  const auto value = fixture_bytes(3 * block_bytes + 17);
  const auto root = build(value);
  cold();
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 4096);
    EXPECT_EQ(payload->read_ranges(root, value.size(), {{0, value.size()}})[0], value);
    EXPECT_TRUE(request.status().ok());
    EXPECT_LT(request.budget()->consumed_bytes(), QueryMetadataBudget::maximum_bytes);
  }
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW(payload->read_ranges(root, value.size(), {{0, 1}}), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_FALSE(owner->frozen());
  }
  EXPECT_EQ(payload->read_ranges(root, value.size(), {{0, 1}})[0], slice(value, 0, 1));
}

TEST_F(PayloadArtifactsTest, OwnerBatchCombinesOnlyColdMetadataAndForceBypassesAllWarmEntries) {
  auto tx = owner->begin("opaque-map-objects");
  const auto a = tx->put_artifact("extent-page", Bytes{1, 2});
  const auto b = tx->put_artifact("extent-page", Bytes{3, 4, 5});
  tx->commit();
  owner->clear_read_cache(); store->batches.clear();
  const auto view = owner->view();
  std::uint64_t used = 0;
  EXPECT_EQ(owner->read_artifacts(view, {{a, 2}}, used)[0], (Bytes{1, 2}));
  store->batches.clear();
  const auto mixed = owner->read_artifacts(view, {{a, 2}, {b, 3}}, used);
  ASSERT_EQ(mixed.size(), 2u);
  EXPECT_EQ(mixed[0], (Bytes{1, 2})); EXPECT_EQ(mixed[1], (Bytes{3, 4, 5}));
  ASSERT_EQ(store->batches.size(), 1u);
  ASSERT_EQ(store->batches[0].size(), 1u);
  EXPECT_EQ(store->batches[0][0].ref.id, b.id);
  EXPECT_EQ(used, 7u);
  store->batches.clear();
  EXPECT_NO_THROW(owner->read_artifacts(view, {{a, 2}, {b, 3}}, used, false, true));
  ASSERT_EQ(store->batches.size(), 1u);
  EXPECT_EQ(store->batches[0].size(), 2u);
}

TEST_F(PayloadArtifactsTest, OwnerBatchFailureDoesNotPartiallyCacheAndRejectsForeignDuplicateOrFrozenReads) {
  auto tx = owner->begin("batch-failure-objects");
  const auto a = tx->put_artifact("extent-page", Bytes{1, 2});
  const auto b = tx->put_artifact("extent-page", Bytes{3, 4, 5});
  tx->commit();
  owner->clear_read_cache(); store->batches.clear();
  const auto view = owner->view();
  std::uint64_t used = 0;
  EXPECT_THROW(owner->read_artifacts(view, {{a, 2}, {a, 2}}, used), Error);
  auto foreign = view; foreign.head.identity.incarnation = "123e4567-e89b-42d3-a456-426614174002";
  EXPECT_THROW(owner->read_artifacts(foreign, {{a, 2}}, used), Error);
  Owner unopened(store, journal);
  EXPECT_THROW(unopened.read_artifacts(view, {{a, 2}}, used), Error);
  EXPECT_TRUE(store->batches.empty());
  store->short_batch = true;
  EXPECT_THROW(owner->read_artifacts(view, {{a, 2}, {b, 3}}, used), Error);
  store->short_batch = false;
  store->batches.clear();
  EXPECT_NO_THROW(owner->read_artifacts(view, {{a, 2}, {b, 3}}, used));
  ASSERT_EQ(store->batches.size(), 1u);
  EXPECT_EQ(store->batches[0].size(), 2u);
  EXPECT_THROW(owner->read_artifacts(view, {{a, 3}}, used), Error);
}

TEST_F(PayloadArtifactsTest, OwnerPayloadBlocksAreSeparatelyBoundedUncachedAndNeverMetadataArtifacts) {
  auto tx = owner->begin("payload-only-accounting");
  EXPECT_THROW(tx->put_payload_block({}), Error);
  EXPECT_THROW(tx->put_payload_block(Bytes(block_bytes + 1, 8)), Error);
  const Bytes block(block_bytes, 7);
  const auto ref = tx->put_payload_block(block);
  EXPECT_THROW(tx->put_artifact("payload-block", block), Error);
  tx->commit();
  const auto view = owner->view();
  std::uint64_t used = 0;
  EXPECT_THROW(owner->read_artifact(view, ref, used), Error);
  EXPECT_THROW(owner->read_artifacts(view, {{ref, block.size()}}, used), Error);
  store->batches.clear();
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes);
    for (unsigned i = 0; i < 2; ++i)
      EXPECT_EQ(owner->read_artifacts(view, {{ref, block.size()}}, used, true)[0], block);
    EXPECT_TRUE(request.status().ok());
    EXPECT_EQ(request.budget()->consumed_bytes(), QueryMetadataBudget::maximum_bytes);
  }
  EXPECT_EQ(used, 0u);
  EXPECT_EQ(store->batches.size(), 2u);
  const auto previous_head = owner->view().head;
  store->objects.at(PayloadStore::key(identity, ref))[0] ^= 1;
  auto conflicting = owner->begin("payload-immutable-conflict");
  EXPECT_THROW(conflicting->put_payload_block(block), Error);
  conflicting->abort();
  EXPECT_EQ(owner->view().head, previous_head);
}

TEST(PayloadArtifactsBudget, PayloadAndExtentBoundariesAccumulateIndependentlyAndFailureIsSticky) {
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata("metadata", 19);
    EXPECT_NO_THROW(require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes - 1,
                                                QueryMetadataBudget::maximum_extents - 1));
    EXPECT_NO_THROW(require_query_payload_demand(1, 1));
    EXPECT_EQ(request.budget()->consumed_bytes(), 19u);
    EXPECT_THROW(require_query_payload_demand(1, 0), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_THROW(require_query_metadata("metadata", 0), QueryMetadataBudgetExceeded);
    EXPECT_THROW(require_query_payload_demand(0, 0), QueryMetadataBudgetExceeded);
  }
  {
    ScopedQueryMetadataBudget request;
    EXPECT_NO_THROW(require_query_payload_demand(0, QueryMetadataBudget::maximum_extents));
    EXPECT_THROW(require_query_payload_demand(0, 1), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.budget()->consumed_bytes(), 0u);
    EXPECT_FALSE(request.status().ok());
  }
  {
    ScopedQueryMetadataBudget independent;
    EXPECT_NO_THROW(require_query_payload_demand(1, 1));
    EXPECT_TRUE(independent.status().ok());
  }
}

TEST(PayloadArtifactsBudget, ProducerTraceReplaysPayloadAndExtentFeesPerWaiterWithoutChargingMetadata) {
  auto producer = std::async(std::launch::async, [] {
    ScopedQueryMetadataProducer neutral;
    ScopedQueryMetadataTrace trace("");
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("source/map-control", 17);
    require_query_payload_demand(7, 3);
    return trace.release();
  });
  const auto fees = producer.get();
  ASSERT_EQ(fees.size(), 3u);
  {
    ScopedQueryMetadataBudget tight;
    require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes - 6, 0);
    EXPECT_THROW(replay_query_metadata_trace(fees), QueryMetadataBudgetExceeded);
    EXPECT_EQ(tight.status().code(), QueryError::ResourceExhausted);
  }
  {
    ScopedQueryMetadataBudget fresh;
    EXPECT_NO_THROW(replay_query_metadata_trace(fees));
    EXPECT_NO_THROW(replay_query_metadata_trace(fees)); // A repeated cache hit is the same demand.
    EXPECT_EQ(fresh.budget()->consumed_bytes(), 17u);
    EXPECT_NO_THROW(require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes - 7,
                                                QueryMetadataBudget::maximum_extents - 3));
    EXPECT_THROW(require_query_payload_demand(0, 1), QueryMetadataBudgetExceeded);
    EXPECT_EQ(fresh.budget()->consumed_bytes(), 17u);
  }
}

TEST_F(PayloadArtifactsTest, WarmRangeReadsEnforceCumulativePayloadAndLogicalExtentDemand) {
  const Bytes data(32, 7);
  const auto root = build(data);
  ASSERT_EQ(payload->read_ranges(root, data.size(), {{0, 1}})[0], (Bytes{7}));
  {
    ScopedQueryMetadataBudget request;
    // Even a one-byte output validates the full referenced encoded block.
    require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes - data.size(), 0);
    EXPECT_EQ(payload->read_ranges(root, data.size(), {{0, 1}})[0], (Bytes{7}));
    EXPECT_THROW(payload->read_ranges(root, data.size(), {{0, 1}}), QueryMetadataBudgetExceeded);
    EXPECT_FALSE(owner->frozen());
  }
  {
    ScopedQueryMetadataBudget request;
    require_query_payload_demand(0, QueryMetadataBudget::maximum_extents - 1);
    EXPECT_THROW(payload->read_ranges(root, data.size(), {{0, 1}, {0, 1}}), QueryMetadataBudgetExceeded);
    EXPECT_FALSE(request.status().ok());
  }
  {
    ScopedQueryMetadataBudget fresh;
    EXPECT_EQ(payload->read_ranges(root, data.size(), {{0, 1}, {0, 1}}).size(), 2u);
    EXPECT_TRUE(fresh.status().ok());
  }
}

TEST_F(PayloadArtifactsTest, HugeLogicalFileCanSeekPast65536ExtentsWithoutReadingItsWholeMap) {
  constexpr std::uint64_t count = 65537;
  const Bytes block(block_bytes, 77);
  auto tx = owner->begin("one-physical-block-for-large-logical-map");
  const auto ref = tx->put_payload_block(block);
  tx->commit();
  std::vector<PayloadChild> children;
  for (std::uint64_t first = 0; first < count; first += 256) {
    ExtentPage leaf;
    leaf.begin = first * block_bytes;
    const auto end = std::min(count, first + 256);
    leaf.end = end * block_bytes; leaf.extent_count = end - first;
    for (auto index = first; index < end; ++index)
      leaf.extents.push_back({index * block_bytes, block_bytes, 0, block_bytes, ref.id});
    auto encoded = encode_extent_page(leaf);
    auto page = object_ref("extent-page", encoded);
    store->put_object(identity, page, encoded);
    children.push_back({leaf.begin, leaf.end, leaf.extent_count, encoded.size(), page.id});
  }
  unsigned level = 0;
  while (children.size() > 1) {
    ++level;
    std::vector<PayloadChild> next;
    for (std::size_t first = 0; first < children.size(); first += 256) {
      const auto end = std::min(children.size(), first + 256);
      ExtentPage parent;
      parent.level = level; parent.begin = children[first].begin; parent.end = children[end - 1].end;
      parent.children.assign(children.begin() + first, children.begin() + end);
      for (const auto& child : parent.children) parent.extent_count += child.extent_count;
      auto encoded = encode_extent_page(parent);
      auto page = object_ref("extent-page", encoded);
      store->put_object(identity, page, encoded);
      next.push_back({parent.begin, parent.end, parent.extent_count, encoded.size(), page.id});
    }
    children = std::move(next);
  }
  const auto& top = children[0];
  const PayloadMap map{count * block_bytes, count, static_cast<std::uint8_t>(level + 1), top.encoded_size, top.page};
  const auto encoded = encode_payload_map(map);
  const auto root = object_ref("payload-map", encoded);
  store->put_object(identity, root, encoded);
  cold();
  EXPECT_NO_THROW(payload->verify(root.id, map.payload_size));
  const auto result = payload->read_ranges(root.id, map.payload_size, {{(count - 1) * block_bytes + 7, 1}});
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], (Bytes{77}));
  std::size_t page_reads = 0, payload_reads = 0;
  for (const auto& batch : store->batches) for (const auto& request : batch) {
    page_reads += request.ref.type == "extent-page";
    payload_reads += request.ref.type == "payload-block";
  }
  EXPECT_EQ(page_reads, 3u);
  EXPECT_EQ(payload_reads, 1u);
  EXPECT_EQ(store->count("payload-block"), 1u);
}

TEST_F(PayloadArtifactsTest, FailedPayloadPublicationKeepsOldHeadAndLeavesOnlyUnreachableObjects) {
  struct Fault { const char* type; bool readback; unsigned preceding_puts; };
  for (const auto& fault : {Fault{"payload-block", false, 1},
       Fault{"extent-page", false, 0}, Fault{"payload-map", false, 0},
       Fault{"payload-block", true, 0}, Fault{"extent-page", true, 0},
       Fault{"payload-map", true, 0}}) {
    SCOPED_TRACE(std::string(fault.type) + (fault.readback ? " readback" : " write"));
    auto local_store = std::make_shared<PayloadStore>();
    auto local_journal = std::make_shared<PayloadJournal>();
    auto local_owner = std::make_shared<Owner>(local_store, local_journal);
    local_owner->initialize(identity);
    PayloadArtifacts artifacts(local_owner);
    {
      auto tx = local_owner->begin("old-visible-root");
      tx->put(Table::Current, "/file", Bytes{'o', 'l', 'd'});
      tx->commit();
    }
    const auto old = local_owner->view();
    const auto old_wire = local_store->head;
    const auto old_journal_head = local_journal->state->committed;
    const auto objects_before = local_store->objects.size();
    if (fault.readback) local_store->fail_read_type = fault.type;
    else local_store->fail_put_type = fault.type;
    local_store->successful_puts_before_failure = fault.preceding_puts;
    {
      auto tx = local_owner->begin("unpublished-payload");
      tx->put(Table::Current, "/file", Bytes{'n', 'e', 'w'});
      EXPECT_THROW(artifacts.build(*tx, 2 * block_bytes,
        [](std::uint64_t offset, std::uint64_t length) {
          return Bytes(length, static_cast<std::uint8_t>(31 + offset / block_bytes));
        }), Error);
      EXPECT_EQ(local_store->head, old_wire);
      // The only publication boundary is commit; even staged directory changes stay private.
      EXPECT_EQ(local_owner->get(local_owner->view(), Table::Current, "/file"),
                std::optional<Bytes>(Bytes{'o', 'l', 'd'}));
    }
    local_store->fail_put_type.clear();
    local_store->fail_read_type.clear();
    EXPECT_GT(local_store->objects.size(), objects_before);
    EXPECT_EQ(local_owner->view().head, old.head);
    EXPECT_EQ(local_store->head, old_wire);
    EXPECT_EQ(local_journal->state->committed, old_journal_head);
    EXPECT_FALSE(local_owner->frozen());
    // A fresh owner must recover only the old graph, ignoring unreachable objects.
    auto recovered = std::make_shared<Owner>(local_store, local_journal);
    EXPECT_NO_THROW(recovered->recover());
    EXPECT_EQ(recovered->view().head, old.head);
    EXPECT_EQ(recovered->scan(recovered->view(), Table::Current).size(), 1u);
    auto next = local_owner->begin("after-failed-payload");
    next->put(Table::Current, "/after", Bytes{'o', 'k'});
    EXPECT_NO_THROW(next->commit());
    EXPECT_EQ(local_owner->view().head.sequence, old.head.sequence + 1);
    EXPECT_EQ(local_owner->get(local_owner->view(), Table::Current, "/file"),
              std::optional<Bytes>(Bytes{'o', 'l', 'd'}));
  }
}

TEST_F(PayloadArtifactsTest, ScatterReadOf128UniqueBlocksReservesOnlyOneBoundedBatchAndOutput) {
  // Persistent fixture bytes are outside the measured read working set; no source buffer is retained.
  constexpr std::uint64_t size = 128 * block_bytes;
  auto tx = owner->begin("many-distinct-blocks");
  const auto root = payload->build(*tx, size, [](std::uint64_t offset, std::uint64_t length) {
    return Bytes(length, static_cast<std::uint8_t>(1 + offset / block_bytes));
  });
  tx->commit();
  StructuredBlobPayloadRanges ranges;
  for (unsigned i = 0; i < 64; ++i) ranges.push_back({(2 * i + 1) * block_bytes - 1, 2});
  constexpr std::uint64_t output = 128, bound = 4 * 8 * block_bytes + output;
  struct Reservations { std::uint64_t live{0}, peak{0}, calls{0}, limit{0}; } accounting;
  auto reserve = [&](std::uint64_t bytes) -> std::shared_ptr<void> {
    if (bytes > accounting.limit - accounting.live)
      throw QueryStorageReadInterrupted(QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Storage,
                                                          "test temporary capacity exceeded"));
    accounting.live += bytes;
    accounting.peak = std::max(accounting.peak, accounting.live);
    ++accounting.calls;
    return std::shared_ptr<void>(new std::uint64_t(bytes), [&](void* token) {
      const auto held = *static_cast<std::uint64_t*>(token);
      EXPECT_GE(accounting.live, held);
      accounting.live -= held;
      delete static_cast<std::uint64_t*>(token);
    });
  };
  cold();
  accounting.limit = bound;
  {
    ScopedQueryStorageReadControl control([] {}, reserve);
    const auto result = payload->read_ranges(root, size, ranges);
    ASSERT_EQ(result.size(), 64u);
    for (unsigned i = 0; i < 64; ++i)
      EXPECT_EQ(result[i], (Bytes{static_cast<std::uint8_t>(2 * i + 1),
                                  static_cast<std::uint8_t>(2 * i + 2)}));
  }
  EXPECT_EQ(accounting.live, 0u);
  EXPECT_LE(accounting.peak, bound);
  EXPECT_GE(accounting.peak, 8 * block_bytes + output);
  EXPECT_GT(accounting.calls, 1u);
  std::size_t blocks = 0;
  for (const auto& batch : store->batches) {
    std::uint64_t batch_bytes = 0;
    for (const auto& request : batch) if (request.ref.type == "payload-block") {
      ++blocks; batch_bytes += request.size;
    }
    EXPECT_LE(batch_bytes, 8 * block_bytes);
  }
  EXPECT_EQ(blocks, 128u);
  // The full first group and output cannot fit one byte below the promised working-set bound.
  cold();
  accounting = {};
  accounting.limit = bound - 1;
  {
    ScopedQueryStorageReadControl control([] {}, reserve);
    try {
      (void)payload->read_ranges(root, size, ranges);
      FAIL() << "temporary-memory capacity must be enforced before block fetch";
    } catch (const QueryStorageReadInterrupted& error) {
      EXPECT_EQ(error.status().code(), QueryError::ResourceExhausted);
    }
  }
  EXPECT_EQ(accounting.live, 0u);
  EXPECT_FALSE(owner->frozen());
  for (const auto& batch : store->batches)
    for (const auto& request : batch) EXPECT_NE(request.ref.type, "payload-block");
}
} // namespace
} // namespace iotdbfs::versioning
