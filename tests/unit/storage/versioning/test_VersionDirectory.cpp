#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "../../TestSupport.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <tuple>
#include <atomic>
#include <condition_variable>
#include <future>
#include <thread>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace iotdbfs::versioning {
namespace {

const NamespaceIdentity identity{"123e4567-e89b-42d3-a456-426614174000",
                                 "123e4567-e89b-42d3-a456-426614174001"};
const NamespaceIdentity other_identity{"123e4567-e89b-42d3-a456-426614174002",
                                       "123e4567-e89b-42d3-a456-426614174003"};

Bytes bytes(const std::string& value) { return Bytes(value.begin(), value.end()); }

Bytes from_hex(const std::string& value) {
  Bytes result;
  for (std::size_t i = 0; i < value.size(); i += 2)
    result.push_back(static_cast<std::uint8_t>(std::stoul(value.substr(i, 2), nullptr, 16)));
  return result;
}

template <class Action>
void expect_error(ErrorCode expected, Action action) {
  try {
    action();
    FAIL() << "operation unexpectedly succeeded";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), expected) << error.what();
  }
}

// These maps stand in for durable state shared by independent Owner instances.
// They provide no CAS and do not share an Owner's in-memory current view.
class MemoryStore final : public Store {
public:
  enum class Write { Ack, UnknownCommitted, UnknownNotCommitted };
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  std::optional<Bytes> head;
  std::map<Key, Bytes> objects;
  Write write{Write::Ack};
  std::function<void(const ObjectRef&)> before_get;
  std::function<void(const ObjectRef&)> before_put;
  std::function<void()> before_head;
  bool fail_put{false};
  bool fail_read_head{false};
  unsigned head_writes{0}, reconciliations{0};
  unsigned object_batches{0};

  static Key key(const NamespaceIdentity& ns, const ObjectRef& ref) {
    return {ns.namespace_id, ns.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override {
    if (fail_read_head) throw Error(ErrorCode::Unavailable, "injected head read failure");
    return head;
  }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override {
    if (before_get) before_get(ref);
    const auto found = objects.find(key(ns, ref));
    if (found == objects.end()) throw Error(ErrorCode::Missing, "missing immutable object");
    return found->second;
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    ++object_batches;
    std::vector<Bytes> result;
    for (const auto& request : requests) {
      auto value = get_object(ns, request.ref);
      if (value.size() != request.size) throw Error(ErrorCode::Corrupt, "batch object size mismatch");
      result.push_back(std::move(value));
    }
    return result;
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& data) override {
    if (before_put) before_put(ref);
    if (fail_put) throw Error(ErrorCode::Unavailable, "injected object write failure");
    if (content_id(ref.type, ref.protocol, data) != ref.id)
      throw Error(ErrorCode::Corrupt, "object ID does not match supplied bytes");
    const auto inserted = objects.emplace(key(ns, ref), data);
    if (!inserted.second && inserted.first->second != data)
      throw Error(ErrorCode::Conflict, "immutable object replacement");
  }
  HeadWriteResult write_head(const Bytes& target) override {
    if (before_head) before_head();
    ++head_writes;
    if (write != Write::UnknownNotCommitted) head = target;
    return write == Write::Ack ? HeadWriteResult::Acknowledged : HeadWriteResult::Indeterminate;
  }
  void accept_reconciled_head() override { ++reconciliations; }
};

class MemoryJournal final : public Journal {
public:
  std::optional<JournalState> state;
  unsigned saves{0};
  unsigned fail_save_at{0};
  std::optional<JournalState> load() override { return state; }
  void save(const JournalState& value) override {
    ++saves;
    if (saves == fail_save_at) throw Error(ErrorCode::Unavailable, "injected journal failure");
    state = value;
  }
};

class VersionDirectoryOwner : public ::testing::Test {
protected:
  std::shared_ptr<MemoryStore> store = std::make_shared<MemoryStore>();
  std::shared_ptr<MemoryJournal> journal = std::make_shared<MemoryJournal>();
  std::unique_ptr<Owner> owner = std::make_unique<Owner>(store, journal);
  void SetUp() override { owner->initialize(identity); }
  void put_all(const std::string& key, const Bytes& value) {
    auto transaction = owner->begin("fixture-publish");
    for (auto table : {Table::Current, Table::Exact, Table::Liveness, Table::Permission})
      transaction->put(table, key, value);
    transaction->commit();
  }
};

TEST(VersionDirectoryCodec, ContentIdUsesFrozenIndependentSha256Framing) {
  // Expected digest is computed independently from the documented domain,
  // two u32-BE length-prefixed UTF-8 strings, u64-BE payload length and bytes.
  EXPECT_EQ(content_id("record", "iotdbfs-record/1", Bytes{0, 1, 255}),
            "234a530524e5b25380dca1712b440488c7e248758d80718c96e0ed22118a966f");
  const auto original = content_id("record", "iotdbfs-record/1", Bytes{0, 1, 255});
  EXPECT_NE(original, content_id("record2", "iotdbfs-record/1", Bytes{0, 1, 255}));
  EXPECT_NE(original, content_id("record", "iotdbfs-record/2", Bytes{0, 1, 255}));
  EXPECT_NE(original, content_id("record", "iotdbfs-record/1", Bytes{0, 1}));
  EXPECT_NE(original, content_id("record", "iotdbfs-record/1", Bytes{0, 1, 254}));
}

TEST(VersionDirectoryCodec, CanonicalIdentifiersRejectAlternateSpellings) {
  EXPECT_TRUE(valid_uuid(identity.namespace_id));
  EXPECT_TRUE(valid_uuid(random_uuid()));
  EXPECT_FALSE(valid_uuid("123E4567-E89B-42D3-A456-426614174000"));
  EXPECT_FALSE(valid_uuid("123e4567e89b42d3a456426614174000"));
  EXPECT_TRUE(valid_content_id(std::string(64, 'a')));
  EXPECT_FALSE(valid_content_id(std::string(64, 'A')));
  EXPECT_FALSE(valid_content_id(std::string(63, 'a')));
  EXPECT_FALSE(valid_content_id(std::string(65, 'a')));
  EXPECT_FALSE(valid_content_id(std::string(64, 'g')));
}

TEST(VersionDirectoryCodec, HeadGoldenFixesRawUuidBigEndianAndFramedDigest) {
  const Head head{identity, 0x0102030405060708ULL, std::string(64, 'a'), std::string(64, 'b')};
  const auto golden = from_hex(
    "56444831123e4567e89b42d3a456426614174000123e4567e89b42d3a4564266141740010102030405060708"
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    "755a4d1c71cd3865e3ba3239494cb17d4381a68ab7a54eb6870a6c147a6d73fb");
  EXPECT_EQ(encode_head(head), golden);
  EXPECT_EQ(golden.size(), 140u);
  EXPECT_EQ(decode_head(golden), head);
  EXPECT_EQ(encode_head(decode_head(golden)), golden);
  for (auto offset : {std::size_t{0}, std::size_t{20}, std::size_t{43}, std::size_t{108}, std::size_t{139}}) {
    auto bad = golden;
    bad[offset] ^= 1;
    EXPECT_THROW(decode_head(bad), Error);
  }
  auto truncated = golden;
  truncated.pop_back();
  EXPECT_THROW(decode_head(truncated), Error);
  auto trailing = golden;
  trailing.push_back(0);
  EXPECT_THROW(decode_head(trailing), Error);
  for (auto size : {kHeadBytes - 1, kHeadBytes, kHeadBytes + 1}) {
    auto oversized = golden;
    oversized.resize(size);
    EXPECT_THROW(decode_head(oversized), Error);
  }
}

TEST(VersionDirectoryCodec, RootPageAndCommitIndependentGoldens) {
  const Roots roots{{"", std::string(64, 'a'), "", std::string(64, 'b')}};
  const auto root_golden = from_hex(
    "564452310001aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "0001bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  EXPECT_EQ(encode_roots(roots), root_golden);
  EXPECT_EQ(decode_roots(root_golden), roots);
  const Page page{0, {{"/a", std::string(64, 'a')}, {"/b", std::string(64, 'b')}}};
  const auto page_golden = from_hex(
    "564450310000000002000000022f61aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "000000022f62bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  EXPECT_EQ(encode_page(page), page_golden);
  EXPECT_EQ(encode_page(decode_page(page_golden)), page_golden);
  const Commit commit{identity, 0x0102030405060708ULL, "", std::string(64, 'b'),
                      "123e4567-e89b-42d3-a456-426614174002", "publish", {}};
  const auto commit_golden = from_hex(
    "56444331123e4567e89b42d3a456426614174000123e4567e89b42d3a456426614174001010203040506070800"
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    "123e4567e89b42d3a456426614174002000000077075626c69736800000000");
  EXPECT_EQ(encode_commit(commit), commit_golden);
  EXPECT_EQ(encode_commit(decode_commit(commit_golden)), commit_golden);
  for (auto data : {root_golden, page_golden, commit_golden}) {
    auto trailing = data;
    trailing.push_back(0);
    data.pop_back();
    if (trailing[2] == 'R') {
      EXPECT_THROW(decode_roots(trailing), Error);
      EXPECT_THROW(decode_roots(data), Error);
    } else if (trailing[2] == 'P') {
      EXPECT_THROW(decode_page(trailing), Error);
      EXPECT_THROW(decode_page(data), Error);
    } else {
      EXPECT_THROW(decode_commit(trailing), Error);
      EXPECT_THROW(decode_commit(data), Error);
    }
  }
  auto noncanonical_optional = root_golden;
  noncanonical_optional[4] = 2;
  EXPECT_THROW(decode_roots(noncanonical_optional), Error);
}

TEST(VersionDirectoryCodec, PageByteEntryAndHeightLimitsAreIndependent) {
  Page page{0, {}};
  // Eight bounded keys make an exactly 64-KiB page without exceeding the
  // per-key cap. All lengths include magic, level, count, text lengths and IDs.
  for (unsigned i = 0; i < 8; ++i) {
    const auto size = i < 7 ? 8192u : 7895u;
    page.entries.push_back({std::string(1, static_cast<char>('a' + i)) + std::string(size - 1, 'x'),
                            std::string(64, 'a')});
  }
  EXPECT_EQ(encode_page(page).size(), kPageBytes);
  EXPECT_EQ(encode_page(decode_page(encode_page(page))).size(), kPageBytes);
  page.entries.back().key.pop_back();
  EXPECT_EQ(encode_page(page).size(), kPageBytes - 1);
  page.entries.back().key += "xx";
  expect_error(ErrorCode::Limit, [&] { encode_page(page); });

  page.entries.clear();
  for (unsigned i = 0; i < kPageEntries; ++i) {
    std::ostringstream key;
    key << std::setw(4) << std::setfill('0') << i;
    page.entries.push_back({key.str(), std::string(64, 'a')});
  }
  EXPECT_NO_THROW(encode_page(page));
  page.entries.push_back({"9999", std::string(64, 'a')});
  expect_error(ErrorCode::Limit, [&] { encode_page(page); });
  page.entries.pop_back();
  page.level = kTreeHeight - 1;
  EXPECT_NO_THROW(encode_page(page));
  page.level = kTreeHeight;
  expect_error(ErrorCode::Limit, [&] { encode_page(page); });
  page.level = 0;
  std::swap(page.entries[0], page.entries[1]);
  EXPECT_THROW(encode_page(page), Error);
  std::swap(page.entries[0], page.entries[1]);
  page.entries[1] = page.entries[0];
  EXPECT_THROW(encode_page(page), Error);
  EXPECT_THROW(encode_page(Page{}), Error);
}

TEST(VersionDirectoryCodec, CommitRejectsDuplicateOrUnsortedDependencies) {
  Commit commit{identity, 1, std::string(64, 'a'), std::string(64, 'b'),
                "123e4567-e89b-42d3-a456-426614174002", "rename", {"a", "b"}};
  const auto encoded = encode_commit(commit);
  EXPECT_EQ(decode_commit(encoded).dependencies, commit.dependencies);
  commit.dependencies = {"b", "a"};
  EXPECT_THROW(encode_commit(commit), Error);
  commit.dependencies = {"a", "a"};
  EXPECT_THROW(encode_commit(commit), Error);
  commit.dependencies.clear();
  commit.operation.resize(129, 'x');
  EXPECT_THROW(encode_commit(commit), Error);
}

TEST(VersionDirectoryCodec, ExactKeysPreserveLargeVersionAndRejectUnsafePaths) {
  for (auto version : {std::int64_t{0}, std::int64_t{255},
                       std::numeric_limits<std::int64_t>::max()}) {
    const auto decoded = decode_exact_key(exact_key("/science/气象.h5", version));
    EXPECT_EQ(decoded.first, "/science/气象.h5");
    EXPECT_EQ(decoded.second, version);
  }
  EXPECT_LT(exact_key("/a", 255), exact_key("/a", 256));
  for (const auto& path : {"relative", "/a/../b", "/a/./b", "/a//b"})
    EXPECT_THROW(exact_key(path, 1), Error);
  EXPECT_THROW(exact_key("/a", -1), Error);
  EXPECT_THROW(exact_key(std::string("/a\0hidden", 9), 1), Error);
  EXPECT_THROW(decode_exact_key("/a"), Error);
}

TEST_F(VersionDirectoryOwner, ExplicitInitializeAndFreshRecoverRetainOneCompleteHead) {
  EXPECT_FALSE(owner->frozen());
  ASSERT_TRUE(store->head);
  ASSERT_TRUE(journal->state);
  EXPECT_EQ(journal->state->identity, identity);
  EXPECT_EQ(journal->state->committed, store->head);
  EXPECT_FALSE(journal->state->pending);
  const auto original = owner->view();
  EXPECT_EQ(original.head.identity, identity);
  Owner recovered(store, journal);
  EXPECT_TRUE(recovered.frozen());
  recovered.recover();
  EXPECT_FALSE(recovered.frozen());
  EXPECT_EQ(recovered.view().head, original.head);
  EXPECT_EQ(recovered.view().roots, original.roots);
  EXPECT_THROW(owner->initialize(other_identity), Error);
  EXPECT_EQ(decode_head(*store->head), original.head);
}

TEST(VersionDirectoryRecovery, MissingHeadOrJournalNeverImplicitlyInitializes) {
  auto store = std::make_shared<MemoryStore>();
  auto journal = std::make_shared<MemoryJournal>();
  Owner missing_both(store, journal);
  EXPECT_THROW(missing_both.recover(), Error);
  EXPECT_TRUE(missing_both.frozen());
  Owner initial(store, journal);
  initial.initialize(identity);
  const auto head = store->head;
  const auto registry = journal->state;
  journal->state.reset();
  Owner missing_journal(store, journal);
  EXPECT_THROW(missing_journal.recover(), Error);
  EXPECT_THROW(missing_journal.initialize(other_identity), Error);
  EXPECT_EQ(store->head, head);
  journal->state = registry;
  store->head.reset();
  Owner missing_head(store, journal);
  EXPECT_THROW(missing_head.recover(), Error);
  EXPECT_THROW(missing_head.initialize(other_identity), Error);
  EXPECT_FALSE(store->head);
}

TEST_F(VersionDirectoryOwner, FourTablesPublishTogetherAndOldViewsRemainImmutable) {
  const auto before = owner->view();
  auto transaction = owner->begin("publish");
  for (auto table : {Table::Current, Table::Exact, Table::Liveness, Table::Permission}) {
    transaction->put(table, "/science/a.h5", Bytes{static_cast<std::uint8_t>(table), 7});
    EXPECT_FALSE(owner->get(before, table, "/science/a.h5"));
    EXPECT_EQ(transaction->get(table, "/science/a.h5"),
              (Bytes{static_cast<std::uint8_t>(table), 7}));
  }
  EXPECT_EQ(decode_head(*store->head), before.head);
  transaction->commit();
  const auto after = owner->view();
  EXPECT_EQ(after.head.sequence, before.head.sequence + 1);
  EXPECT_NE(after.head.commit, before.head.commit);
  EXPECT_NE(after.head.root, before.head.root);
  for (auto table : {Table::Current, Table::Exact, Table::Liveness, Table::Permission}) {
    EXPECT_FALSE(owner->get(before, table, "/science/a.h5"));
    EXPECT_EQ(owner->get(after, table, "/science/a.h5"),
              (Bytes{static_cast<std::uint8_t>(table), 7}));
  }
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_EQ(restarted.view().roots, after.roots);
}

TEST_F(VersionDirectoryOwner, PermissionOnlyEditReusesUnchangedTableRoots) {
  put_all("/a", bytes("shared-record"));
  const auto before = owner->view();
  auto transaction = owner->begin("chmod");
  transaction->put(Table::Permission, "/a", bytes("new-permission"));
  transaction->commit();
  const auto after = owner->view();
  for (auto table : {Table::Current, Table::Exact, Table::Liveness})
    EXPECT_EQ(after.roots[static_cast<std::size_t>(table)], before.roots[static_cast<std::size_t>(table)]);
  EXPECT_NE(after.roots[static_cast<std::size_t>(Table::Permission)],
            before.roots[static_cast<std::size_t>(Table::Permission)]);
  EXPECT_EQ(owner->get(before, Table::Permission, "/a"), bytes("shared-record"));
  EXPECT_EQ(owner->get(after, Table::Permission, "/a"), bytes("new-permission"));
}

TEST_F(VersionDirectoryOwner, AbortAndDestructorDoNotExposeStagedChanges) {
  const auto before = owner->view();
  const auto writes = store->head_writes;
  {
    auto transaction = owner->begin("abandoned");
    transaction->put(Table::Current, "/abandoned", bytes("value"));
  }
  {
    auto transaction = owner->begin("aborted");
    transaction->put(Table::Permission, "/aborted", bytes("permission"));
    transaction->abort();
  }
  EXPECT_EQ(owner->view().head, before.head);
  EXPECT_EQ(store->head_writes, writes);
  EXPECT_TRUE(owner->scan(owner->view(), Table::Current).empty());
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_EQ(restarted.view().head, before.head);
}

TEST_F(VersionDirectoryOwner, RenameAndDeletionAreOneRootChangeAcrossTables) {
  put_all("/old", bytes("v1"));
  const auto before = owner->view();
  auto rename = owner->begin("rename");
  for (auto table : {Table::Current, Table::Exact, Table::Permission}) {
    rename->erase(table, "/old");
    rename->put(table, "/new", bytes("v1"));
  }
  rename->put(Table::Liveness, "/old", bytes("tombstone"));
  rename->put(Table::Liveness, "/new", bytes("live"));
  EXPECT_EQ(decode_head(*store->head), before.head);
  rename->commit();
  const auto renamed = owner->view();
  for (auto table : {Table::Current, Table::Exact, Table::Permission}) {
    EXPECT_FALSE(owner->get(renamed, table, "/old"));
    EXPECT_EQ(owner->get(renamed, table, "/new"), bytes("v1"));
    EXPECT_EQ(owner->get(before, table, "/old"), bytes("v1"));
  }
  EXPECT_EQ(owner->get(renamed, Table::Liveness, "/old"), bytes("tombstone"));
  auto remove = owner->begin("delete");
  for (auto table : {Table::Current, Table::Exact, Table::Permission}) remove->erase(table, "/new");
  remove->put(Table::Liveness, "/new", bytes("tombstone"));
  remove->commit();
  EXPECT_FALSE(owner->get(owner->view(), Table::Current, "/new"));
  EXPECT_EQ(owner->get(owner->view(), Table::Liveness, "/new"), bytes("tombstone"));
  EXPECT_EQ(owner->get(renamed, Table::Current, "/new"), bytes("v1"));
}

TEST_F(VersionDirectoryOwner, UnknownCommittedHeadIsReconciledBeforeSuccess) {
  store->write = MemoryStore::Write::UnknownCommitted;
  const auto before = owner->view();
  put_all("/a", bytes("value"));
  EXPECT_FALSE(owner->frozen());
  EXPECT_EQ(store->reconciliations, 1u);
  EXPECT_EQ(owner->view().head.sequence, before.head.sequence + 1);
  EXPECT_EQ(journal->state->committed, store->head);
  EXPECT_FALSE(journal->state->pending);
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_EQ(restarted.get(restarted.view(), Table::Current, "/a"), bytes("value"));
}

TEST_F(VersionDirectoryOwner, UnknownOldHeadFreezesAndCannotRetryOrAdmit) {
  const auto old = store->head;
  store->write = MemoryStore::Write::UnknownNotCommitted;
  EXPECT_THROW(put_all("/a", bytes("value")), Error);
  EXPECT_TRUE(owner->frozen());
  EXPECT_EQ(store->head, old);
  ASSERT_TRUE(journal->state->pending);
  EXPECT_THROW(owner->view(), Error);
  EXPECT_THROW(owner->begin("retry"), Error);
  const auto writes = store->head_writes;
  Owner restarted(store, journal);
  EXPECT_THROW(restarted.recover(), Error);
  EXPECT_TRUE(restarted.frozen());
  EXPECT_EQ(store->head_writes, writes);
  // Model the original in-flight request becoming visible after the uncertainty.
  store->head = journal->state->pending;
  restarted.recover();
  EXPECT_FALSE(restarted.frozen());
  EXPECT_EQ(restarted.get(restarted.view(), Table::Current, "/a"), bytes("value"));
}

TEST_F(VersionDirectoryOwner, RecoverRejectsDatabaseRollbackAgainstJournalHighWater) {
  const auto old = store->head;
  put_all("/a", bytes("v1"));
  const auto committed = journal->state->committed;
  store->head = old;
  Owner restarted(store, journal);
  EXPECT_THROW(restarted.recover(), Error);
  EXPECT_TRUE(restarted.frozen());
  EXPECT_EQ(journal->state->committed, committed);
  EXPECT_THROW(restarted.begin("must-not-resurrect"), Error);
}

TEST_F(VersionDirectoryOwner, JournalPendingWriteFailureNeverAttemptsHeadWrite) {
  const auto old = store->head;
  const auto writes = store->head_writes;
  journal->fail_save_at = journal->saves + 1;
  EXPECT_THROW(put_all("/a", bytes("value")), Error);
  EXPECT_EQ(store->head_writes, writes);
  EXPECT_EQ(store->head, old);
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_FALSE(restarted.get(restarted.view(), Table::Current, "/a"));
}

TEST_F(VersionDirectoryOwner, CrashAfterHeadBeforeJournalAcknowledgementRecoversTarget) {
  const auto before = owner->view();
  journal->fail_save_at = journal->saves + 2;
  EXPECT_THROW(put_all("/a", bytes("value")), Error);
  ASSERT_TRUE(journal->state->pending);
  EXPECT_EQ(journal->state->pending, store->head);
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_EQ(restarted.view().head.sequence, before.head.sequence + 1);
  EXPECT_FALSE(journal->state->pending);
  EXPECT_EQ(restarted.get(restarted.view(), Table::Current, "/a"), bytes("value"));
}

TEST_F(VersionDirectoryOwner, ObjectWriteFailureCannotExposeAnyPartialTable) {
  const auto before = owner->view();
  const auto writes = store->head_writes;
  store->fail_put = true;
  EXPECT_THROW(put_all("/a", bytes("value")), Error);
  EXPECT_EQ(store->head_writes, writes);
  EXPECT_EQ(decode_head(*store->head), before.head);
  store->fail_put = false;
  Owner restarted(store, journal);
  restarted.recover();
  for (auto table : {Table::Current, Table::Exact, Table::Liveness, Table::Permission})
    EXPECT_FALSE(restarted.get(restarted.view(), table, "/a"));
}

TEST_F(VersionDirectoryOwner, UnknownReadbackFailureKeepsPendingForFreshRecovery) {
  store->write = MemoryStore::Write::UnknownCommitted;
  store->fail_read_head = true;
  EXPECT_THROW(put_all("/a", bytes("value")), Error);
  EXPECT_TRUE(owner->frozen());
  ASSERT_TRUE(journal->state->pending);
  EXPECT_EQ(journal->state->pending, store->head);
  store->fail_read_head = false;
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_FALSE(restarted.frozen());
  EXPECT_FALSE(journal->state->pending);
  EXPECT_EQ(restarted.get(restarted.view(), Table::Permission, "/a"), bytes("value"));
}

TEST_F(VersionDirectoryOwner, CompletedAndNestedTransactionsCannotMutateAgain) {
  auto transaction = owner->begin("first");
  EXPECT_THROW(owner->begin("nested"), Error);
  transaction->put(Table::Current, "/a", bytes("value"));
  transaction->commit();
  EXPECT_THROW(transaction->put(Table::Current, "/a", bytes("changed")), Error);
  EXPECT_THROW(transaction->erase(Table::Current, "/a"), Error);
  EXPECT_THROW(transaction->commit(), Error);
  EXPECT_EQ(owner->get(owner->view(), Table::Current, "/a"), bytes("value"));
  EXPECT_THROW(owner->begin(""), Error);
  EXPECT_THROW(owner->begin(std::string(129, 'x')), Error);
  auto next = owner->begin("next");
  next->abort();
}

TEST_F(VersionDirectoryOwner, OversizedRecordsAndScanLimitsFailBeforeHeadPublication) {
  const auto before = store->head;
  const auto writes = store->head_writes;
  EXPECT_THROW(owner->scan(owner->view(), Table::Current, "", kScanEntries + 1), Error);
  auto transaction = owner->begin("oversized");
  const Bytes oversized(kMetadataBytes + 1, 0);
  expect_error(ErrorCode::Limit, [&] { transaction->put(Table::Current, "/huge", oversized); });
  transaction->abort();
  EXPECT_EQ(store->head_writes, writes);
  EXPECT_EQ(store->head, before);
}

TEST_F(VersionDirectoryOwner, SequenceOverflowCannotWrapOrPublishAnotherHead) {
  const auto current = owner->view().head;
  const Commit last{identity, UINT64_MAX, current.commit, current.root,
                    "123e4567-e89b-42d3-a456-426614174002", "fixture-highwater", {}};
  const auto encoded_commit = encode_commit(last);
  const auto ref = object_ref("commit", encoded_commit);
  store->put_object(identity, ref, encoded_commit);
  store->head = encode_head(Head{identity, UINT64_MAX, ref.id, current.root});
  journal->state->committed = store->head;
  owner->recover();
  const auto maximum = store->head;
  const auto writes = store->head_writes;
  expect_error(ErrorCode::Limit, [&] { put_all("/a", bytes("value")); });
  EXPECT_EQ(store->head, maximum);
  EXPECT_EQ(store->head_writes, writes);
}

TEST_F(VersionDirectoryOwner, CorruptOrMissingRootFailsClosedDuringRecovery) {
  put_all("/a", bytes("value"));
  const auto saved = store->objects;
  const auto root_id = decode_head(*store->head).root;
  const auto found = std::find_if(store->objects.begin(), store->objects.end(),
    [&](const auto& item) { return std::get<4>(item.first) == root_id; });
  ASSERT_NE(found, store->objects.end());
  found->second.back() ^= 1;
  Owner corrupt(store, journal);
  EXPECT_THROW(corrupt.recover(), Error);
  EXPECT_TRUE(corrupt.frozen());
  store->objects = saved;
  for (auto it = store->objects.begin(); it != store->objects.end();) {
    if (std::get<4>(it->first) == root_id) it = store->objects.erase(it);
    else ++it;
  }
  Owner missing(store, journal);
  EXPECT_THROW(missing.recover(), Error);
  EXPECT_TRUE(missing.frozen());
}

TEST_F(VersionDirectoryOwner, RecoveryUsesCurrentClosureWithoutFollowingPreviousCommits) {
  put_all("/a", bytes("v1"));
  put_all("/a", bytes("v2"));
  const auto current = owner->view();
  for (auto it = store->objects.begin(); it != store->objects.end();) {
    if (std::get<2>(it->first) == "commit" && std::get<4>(it->first) != current.head.commit)
      it = store->objects.erase(it);
    else ++it;
  }
  Owner restarted(store, journal);
  restarted.recover();
  EXPECT_EQ(restarted.view().head, current.head);
  EXPECT_EQ(restarted.get(restarted.view(), Table::Exact, "/a"), bytes("v2"));
}

TEST_F(VersionDirectoryOwner, RecoveryRejectsChildLevelAndMaximumKeyMismatch) {
  const auto old = owner->view();
  const auto original_objects = store->objects;
  const auto original_head = store->head;
  const auto original_state = journal->state;
  for (const auto& parent : {std::make_pair(std::uint8_t{2}, std::string("/a")),
                             std::make_pair(std::uint8_t{1}, std::string("/z"))}) {
    store->objects = original_objects;
    store->head = original_head;
    journal->state = original_state;
    auto save = [&](const std::string& type, const Bytes& value) {
      const auto ref = object_ref(type, value);
      store->put_object(identity, ref, value);
      return ref.id;
    };
    const auto record_id = save("catalog-record", bytes("value"));
    const auto leaf = save("catalog-page", encode_page(Page{0, {{"/a", record_id}}}));
    const auto internal = save("catalog-page", encode_page(Page{parent.first, {{parent.second, leaf}}}));
    Roots roots{};
    roots[0] = internal;
    const auto root = save("catalog-root", encode_roots(roots));
    const Commit commit{identity, old.head.sequence + 1, old.head.commit, root,
                        "123e4567-e89b-42d3-a456-426614174002", "fixture-invalid-tree", {}};
    const auto commit_id = save("commit", encode_commit(commit));
    store->head = encode_head(Head{identity, commit.sequence, commit_id, root});
    journal->state->committed = store->head;
    Owner restarted(store, journal);
    expect_error(ErrorCode::Corrupt, [&] { restarted.recover(); });
    EXPECT_TRUE(restarted.frozen());
  }
}

TEST_F(VersionDirectoryOwner, CopyOnWriteSplitsPagesAndPrefixScanIsOrderedAndBounded) {
  auto transaction = owner->begin("tree-split");
  for (unsigned i = 0; i < 300; ++i) {
    std::ostringstream key;
    key << "/science/" << std::setw(4) << std::setfill('0') << i;
    transaction->put(Table::Exact, key.str(), bytes(std::to_string(i)));
  }
  transaction->commit();
  const auto populated = owner->view();
  const auto rows = owner->scan(populated, Table::Exact, "/science/00");
  ASSERT_EQ(rows.size(), 100u);
  EXPECT_EQ(rows.front(), std::make_pair(std::string("/science/0000"), bytes("0")));
  EXPECT_EQ(rows.back(), std::make_pair(std::string("/science/0099"), bytes("99")));
  EXPECT_THROW(owner->scan(populated, Table::Exact, "/science/00", 99), Error);
  auto update = owner->begin("tree-update");
  update->erase(Table::Exact, "/science/0000");
  update->put(Table::Exact, "/science/0256", bytes("replacement"));
  update->commit();
  EXPECT_EQ(owner->scan(owner->view(), Table::Exact).size(), 299u);
  EXPECT_EQ(owner->get(populated, Table::Exact, "/science/0000"), bytes("0"));
  EXPECT_EQ(owner->get(populated, Table::Exact, "/science/0256"), bytes("256"));
  EXPECT_FALSE(owner->get(owner->view(), Table::Exact, "/science/0000"));
  EXPECT_EQ(owner->get(owner->view(), Table::Exact, "/science/0256"), bytes("replacement"));
}

TEST(VersionDirectoryFileJournal, LockExcludesAnotherOwnerAndSurvivesReopenWithPending) {
  test_support::TempDirectory directory;
  const auto path = directory.file("registry");
  Head old{identity, 1, std::string(64, 'a'), std::string(64, 'b')};
  Head target{identity, 2, std::string(64, 'c'), std::string(64, 'd')};
  const JournalState state{identity, encode_head(old), encode_head(target)};
  {
    FileJournal registry(path);
    EXPECT_FALSE(registry.load());
    EXPECT_THROW(FileJournal competitor(path), Error);
    registry.save(state);
  }
  FileJournal reopened(path);
  const auto saved = reopened.load();
  ASSERT_TRUE(saved);
  EXPECT_EQ(saved->identity, identity);
  EXPECT_EQ(saved->committed, state.committed);
  EXPECT_EQ(saved->pending, state.pending);
}

TEST(VersionDirectoryFileJournal, RejectsSymlinkRegistryDirectory) {
  test_support::TempDirectory directory;
  const auto target = directory.file("target");
  ASSERT_EQ(::mkdir(target.c_str(), 0700), 0);
  const auto alias = directory.file("alias");
  ASSERT_EQ(::symlink(target.c_str(), alias.c_str()), 0);
  EXPECT_THROW(FileJournal registry(alias), Error);
}

TEST(VersionDirectoryFileJournal, ProcessExitReleasesLockWithoutLosingDurablePending) {
  test_support::TempDirectory directory;
  const auto path = directory.file("registry");
  const Head head{identity, 7, std::string(64, 'a'), std::string(64, 'b')};
  const JournalState state{identity, encode_head(head), encode_head(head)};
  const auto pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    try {
      FileJournal registry(path);
      registry.save(state);
      ::_exit(0); // No C++ destructor; the kernel must release the owner lock.
    } catch (...) { ::_exit(1); }
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  FileJournal registry(path);
  const auto loaded = registry.load();
  ASSERT_TRUE(loaded);
  EXPECT_EQ(loaded->committed, state.committed);
  EXPECT_EQ(loaded->pending, state.pending);
}

TEST(VersionDirectoryFileJournal, CorruptStateAndNonprivateDirectoryAreRejected) {
  test_support::TempDirectory directory;
  const auto path = directory.file("registry");
  {
    FileJournal registry(path);
    const Head head{identity, 7, std::string(64, 'a'), std::string(64, 'b')};
    registry.save(JournalState{identity, encode_head(head), {}});
  }
  {
    std::ofstream state(path + "/state", std::ios::binary | std::ios::app);
    state.put('\0');
  }
  {
    FileJournal registry(path);
    EXPECT_THROW(registry.load(), Error);
  }
  ASSERT_EQ(::chmod(path.c_str(), 0755), 0);
  EXPECT_THROW(FileJournal registry(path), Error);
}

NamespaceRecord record() {
  NamespaceRecord result;
  result.inode.path = "/science/a.h5";
  result.inode.parent = "/science";
  result.inode.name = "a.h5";
  result.inode.row_time = std::numeric_limits<std::int64_t>::max();
  result.inode.size = 4096;
  result.inode.mode = 0640;
  result.inode.uid = 1001;
  result.inode.gid = 40077;
  result.inode.ctime = 1234;
  result.inode.mtime = 1235;
  result.inode.atime = 1236;
  result.inode.format_id = "hdf5";
  result.inode.metadata = "{}";
  result.manifest.key = {result.inode.path, result.inode.row_time};
  result.manifest.format_id = result.inode.format_id;
  result.manifest.payload_size = result.inode.size;
  result.manifest.layout_size = 256;
  result.manifest.revision = 19;
  result.manifest.published = true;
  result.manifest.phase = StructuredBlobPhase::Published;
  result.descriptor_id = std::string(64, 'a');
  result.seal_id = std::string(64, 'b');
  return result;
}

TEST(VersionNamespaceRecord, RoundTripKeepsExactInodeAndPublishedManifestIdentity) {
  const auto original = record();
  const auto encoded = encode_namespace_record(original);
  const auto decoded = decode_namespace_record(encoded);
  EXPECT_EQ(encode_namespace_record(decoded), encoded);
  EXPECT_EQ(decoded.inode.path, original.inode.path);
  EXPECT_EQ(decoded.inode.parent, original.inode.parent);
  EXPECT_EQ(decoded.inode.name, original.inode.name);
  EXPECT_EQ(decoded.inode.row_time, original.inode.row_time);
  EXPECT_EQ(decoded.inode.size, original.inode.size);
  EXPECT_EQ(decoded.inode.mode, original.inode.mode);
  EXPECT_EQ(decoded.inode.uid, original.inode.uid);
  EXPECT_EQ(decoded.inode.gid, original.inode.gid);
  EXPECT_EQ(decoded.inode.ctime, original.inode.ctime);
  EXPECT_EQ(decoded.inode.mtime, original.inode.mtime);
  EXPECT_EQ(decoded.inode.atime, original.inode.atime);
  EXPECT_EQ(decoded.manifest.key.path, original.manifest.key.path);
  EXPECT_EQ(decoded.manifest.key.version, original.manifest.key.version);
  EXPECT_EQ(decoded.manifest.payload_size, original.manifest.payload_size);
  EXPECT_EQ(decoded.manifest.layout_size, original.manifest.layout_size);
  EXPECT_EQ(decoded.manifest.revision, original.manifest.revision);
  EXPECT_EQ(decoded.manifest.phase, StructuredBlobPhase::Published);
  EXPECT_TRUE(decoded.manifest.published);
  EXPECT_EQ(decoded.descriptor_id, original.descriptor_id);
  EXPECT_EQ(decoded.seal_id, original.seal_id);
  EXPECT_TRUE(decoded.inode.metadata.empty());
  EXPECT_TRUE(decoded.manifest.metadata.empty());
}

TEST(VersionNamespaceRecord, RejectsIdentityPermissionAndPayloadContradictions) {
  const std::vector<std::function<void(NamespaceRecord&)>> changes{
    [](auto& r) { r.inode.path = "/science/../a.h5"; },
    [](auto& r) { r.inode.parent = "/other"; },
    [](auto& r) { r.inode.uid = -1; },
    [](auto& r) { r.inode.gid = -1; },
    [](auto& r) { r.inode.mode = -1; },
    [](auto& r) { r.inode.row_time = -1; },
    [](auto& r) { r.inode.size = -1; },
    [](auto& r) { r.manifest.key.path = "/foreign"; },
    [](auto& r) { --r.manifest.key.version; },
    [](auto& r) { ++r.manifest.payload_size; },
    [](auto& r) { r.manifest.format_id = "grib"; },
    [](auto& r) { r.manifest.published = false; },
    [](auto& r) { r.manifest.phase = StructuredBlobPhase::Described; },
    [](auto& r) { r.descriptor_id.clear(); },
    [](auto& r) { r.seal_id.clear(); },
    [](auto& r) { r.descriptor_id = std::string(64, 'A'); }
  };
  for (std::size_t i = 0; i < changes.size(); ++i) {
    SCOPED_TRACE(static_cast<int>(i));
    auto bad = record();
    changes[i](bad);
    EXPECT_THROW(encode_namespace_record(bad), Error);
  }
  auto encoded = encode_namespace_record(record());
  encoded.pop_back();
  EXPECT_THROW(decode_namespace_record(encoded), Error);
  encoded = encode_namespace_record(record());
  encoded.push_back(0);
  EXPECT_THROW(decode_namespace_record(encoded), Error);
}

TEST(VersionNamespaceRecord, RejectsSelfConsistentNoncanonicalPathAndMismatchedName) {
  for (const auto& path : {std::string("/science/../a.h5"), std::string("/science/./a.h5"),
                           std::string("/science//a.h5"), std::string("/science/a.h5/"),
                           std::string("/science/a\0b", 12)}) {
    auto bad = record();
    bad.inode.path = path;
    bad.inode.parent = path.substr(0, path.rfind('/'));
    bad.inode.name = path.substr(path.rfind('/') + 1);
    bad.manifest.key.path = path;
    EXPECT_THROW(encode_namespace_record(bad), Error);
  }
  auto bad = record();
  bad.inode.name = "foreign.h5";
  EXPECT_THROW(encode_namespace_record(bad), Error);
}

TEST(VersionNamespaceRecord, FileMetadataIsExternalWhileDirectoriesKeepCanonicalInlineMetadata) {
  auto file = record();
  file.inode.metadata = "invalid inline metadata deliberately omitted";
  file.manifest.metadata["description"] = MetadataValue("stored through descriptor");
  auto decoded = decode_namespace_record(encode_namespace_record(file));
  EXPECT_TRUE(decoded.inode.metadata.empty()); EXPECT_TRUE(decoded.manifest.metadata.empty());
  auto directory = record();
  directory.inode.is_dir = true; directory.inode.metadata = "{}";
  directory.descriptor_id.clear(); directory.seal_id.clear();
  EXPECT_NO_THROW(decode_namespace_record(encode_namespace_record(directory)));
  directory.inode.metadata = "{bad-json";
  EXPECT_THROW(encode_namespace_record(directory), Error);
  directory.inode.metadata = "{}"; directory.descriptor_id = std::string(64, 'a');
  EXPECT_THROW(encode_namespace_record(directory), Error);
  auto previous = encode_namespace_record(record());
  const std::string marker = "iotdbfs-namespace-record/3";
  auto position = std::search(previous.begin(), previous.end(), marker.begin(), marker.end());
  ASSERT_NE(position, previous.end()); position[marker.size() - 1] = '2';
  EXPECT_THROW(decode_namespace_record(previous), Error);
}

} // namespace

TEST_F(VersionDirectoryOwner, HotDirectoryObjectsAndParsedPagesAvoidStoreReads) {
  put_all("/a", bytes("payload"));
  owner->clear_read_cache();
  const auto view = owner->view();
  ASSERT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
  const auto cold = owner->stats();
  ASSERT_GT(cold.object_reads, 0u);
  for (int i=0;i<3;++i) {
    EXPECT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
    EXPECT_EQ(owner->scan(view, Table::Exact).size(), 1u);
  }
  const auto hot = owner->stats();
  EXPECT_EQ(hot.object_reads, cold.object_reads);
  EXPECT_GT(hot.page_cache_hits, cold.page_cache_hits);
  EXPECT_GT(hot.object_cache_hits, cold.object_cache_hits);
  EXPECT_LE(hot.cache_bytes, kMetadataBytes);
}
TEST_F(VersionDirectoryOwner, DependencyWindowSeparatesExactVersionsAndCurrentAncestors) {
  const auto exact = exact_key("/d/a", 1);
  auto t=owner->begin("fixture");t->put(Table::Exact,exact,bytes("v1"));t->put(Table::Current,"/d",bytes("parent"));t->commit();
  const auto base=owner->view();
  std::vector<std::string> deps={std::string(1,char(Table::Current))+"/d",std::string(1,char(Table::Exact))+exact};
  std::sort(deps.begin(),deps.end());
  auto publish=owner->begin("new-version");publish->put(Table::Exact,exact_key("/d/a",2),bytes("v2"));publish->put(Table::Current,"/d/a",bytes("v2"));publish->commit();
  EXPECT_EQ(owner->check_dependencies(base,deps).state,DependencyState::Unchanged);
  auto chmod=owner->begin("ancestor-chmod");chmod->put(Table::Current,"/d",bytes("revoked"));chmod->commit();
  const auto changed=owner->check_dependencies(base,deps);
  EXPECT_EQ(changed.state,DependencyState::Changed);EXPECT_EQ(changed.changed_dependencies,1u);
  EXPECT_EQ(changed.current.head,owner->view().head);
}
TEST_F(VersionDirectoryOwner, HistoryGapAndRecoveryNeverClaimIncrementalUnchanged) {
  auto limited=std::make_unique<Owner>(store,journal,OwnerConfig{kMetadataBytes,2,65536,8*1024*1024});
  limited->recover();const auto old=limited->view();
  for(int i=0;i<3;++i){auto tx=limited->begin("other");tx->put(Table::Current,"/other"+std::to_string(i),bytes("x"));tx->commit();}
  const std::vector<std::string> deps={std::string(1,char(Table::Exact))+exact_key("/a",1)};
  EXPECT_EQ(limited->check_dependencies(old,deps).state,DependencyState::Unknown);
  const auto current=limited->view();
  EXPECT_EQ(limited->check_dependencies(current,deps).state,DependencyState::Unchanged);
  auto restored=std::make_unique<Owner>(store,journal);restored->recover();
  EXPECT_EQ(restored->check_dependencies(old,deps).state,DependencyState::Unknown);
  auto forged=current;forged.head.identity=other_identity;
  EXPECT_EQ(restored->check_dependencies(forged,deps).state,DependencyState::Unknown);
}
TEST_F(VersionDirectoryOwner, OversizeChangeEntryLeavesCoverageGapNotFalseSuccess) {
  auto limited=std::make_unique<Owner>(store,journal,OwnerConfig{kMetadataBytes,256,1,8*1024*1024});limited->recover();
  const auto before=limited->view();
  auto tx=limited->begin("two");tx->put(Table::Current,"/a",bytes("a"));tx->put(Table::Current,"/b",bytes("b"));tx->commit();
  EXPECT_EQ(limited->check_dependencies(before,{std::string(1,char(Table::Current))+"/a"}).state,DependencyState::Unknown);
}
TEST_F(VersionDirectoryOwner, AdmissionRejectsOldHeadAndLeaseIsNotRegistrationLifetime) {
  const auto before=owner->view();
  auto gate=owner->admit(before.head);
  auto lease=owner->acquire_execution_lease();
  EXPECT_EQ(owner->stats().active_execution_leases,1u);
  gate.reset();lease.reset();EXPECT_EQ(owner->stats().active_execution_leases,0u);
  put_all("/changed",bytes("x"));
  expect_error(ErrorCode::Conflict,[&]{owner->admit(before.head);});
  store->write=MemoryStore::Write::UnknownNotCommitted;
  EXPECT_THROW(put_all("/frozen",bytes("x")),Error);
  expect_error(ErrorCode::Frozen,[&]{owner->admit(before.head);});
  EXPECT_THROW(owner->check_dependencies(before,{}),Error);
}
TEST_F(VersionDirectoryOwner, StoreReadDoesNotHoldNamespaceCommitMutex) {
  put_all("/a",bytes("old"));const auto view=owner->view();owner->clear_read_cache();
  std::mutex mutex;std::condition_variable condition;bool entered=false,release=false;std::atomic<bool> first{true};
  store->before_get=[&](const ObjectRef& ref){
    if(ref.type!="catalog-page"||!first.exchange(false))return;
    std::unique_lock<std::mutex> lock(mutex);entered=true;condition.notify_all();condition.wait(lock,[&]{return release;});
  };
  auto reader=std::async(std::launch::async,[&]{return owner->get(view,Table::Exact,"/a");});
  {std::unique_lock<std::mutex> lock(mutex);ASSERT_TRUE(condition.wait_for(lock,std::chrono::seconds(5),[&]{return entered;}));}
  auto writer=std::async(std::launch::async,[&]{auto tx=owner->begin("concurrent");tx->put(Table::Current,"/other",bytes("x"));tx->commit();});
  const auto state=writer.wait_for(std::chrono::seconds(2));
  {std::lock_guard<std::mutex> lock(mutex);release=true;}condition.notify_all();
  EXPECT_EQ(state,std::future_status::ready);writer.get();EXPECT_EQ(reader.get(),bytes("old"));
  store->before_get={};
}
TEST_F(VersionDirectoryOwner, RecoveryBypassesWarmDirectoryCacheAndDetectsCorruption) {
  put_all("/a",bytes("v1"));const auto view=owner->view();ASSERT_TRUE(owner->get(view,Table::Exact,"/a"));
  for(auto& item:store->objects)if(std::get<2>(item.first)=="catalog-record"&&item.second==bytes("v1")){item.second=bytes("corrupt");break;}
  EXPECT_THROW(owner->recover(),Error);EXPECT_TRUE(owner->frozen());
}

TEST_F(VersionDirectoryOwner, ColdDirectoryReadChargesSharedLedgerWithoutFreezingOwner) {
  put_all("/a", bytes("payload"));
  const auto view = owner->view();
  owner->clear_read_cache();
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW(owner->get(view, Table::Exact, "/a"), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(request.budget()->consumed_bytes(), QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW(owner->view(), QueryMetadataBudgetExceeded);
    EXPECT_FALSE(owner->frozen());
  }
  EXPECT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
  EXPECT_FALSE(owner->frozen());
  EXPECT_NO_THROW(owner->admit(view.head));
}

TEST_F(VersionDirectoryOwner, WarmParsedPageStillChargesSharedLedgerAndFailureIsSticky) {
  put_all("/a", bytes("payload"));
  const auto view = owner->view();
  ASSERT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
  ASSERT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
  const auto warm = owner->stats();
  ASSERT_GT(warm.page_cache_hits, 0u);
  {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW(owner->get(view, Table::Exact, "/a"), QueryMetadataBudgetExceeded);
    EXPECT_THROW(owner->check_dependencies(view, {}), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(owner->stats().object_reads, warm.object_reads);
    EXPECT_FALSE(owner->frozen());
  }
  {
    ScopedQueryMetadataBudget next_request;
    EXPECT_EQ(owner->get(view, Table::Exact, "/a"), bytes("payload"));
    EXPECT_GT(next_request.budget()->consumed_bytes(), 0u);
    EXPECT_TRUE(next_request.status().ok());
    EXPECT_EQ(owner->stats().object_reads, warm.object_reads);
  }
}

TEST_F(VersionDirectoryOwner, HistoryGapRevalidationAccumulatesNewControlIdentitiesInOneLedger) {
  put_all("/a", bytes("original"));
  auto limited = std::make_unique<Owner>(store, journal,
    OwnerConfig{kMetadataBytes, 2, 65536, 8 * 1024 * 1024});
  limited->recover();
  const auto original = limited->view();
  for (int i = 0; i < 3; ++i) {
    auto tx = limited->begin("unrelated");
    tx->put(Table::Current, "/other" + std::to_string(i), bytes("other"));
    tx->commit();
  }
  const auto current = limited->view();
  const std::vector<std::string> dependencies{std::string(1, char(Table::Exact)) + "/a"};
  const auto ledger = std::make_shared<QueryMetadataBudget>();
  std::uint64_t first_control_bytes = 0;
  {
    ScopedQueryMetadataBudget request(ledger);
    EXPECT_EQ(limited->check_dependencies(original, dependencies).state, DependencyState::Unknown);
    EXPECT_EQ(limited->get(current, Table::Exact, "/a"), bytes("original"));
    first_control_bytes = ledger->consumed_bytes();
    EXPECT_GT(first_control_bytes, encode_head(current.head).size());
    // A cached revalidation of the same controls is the same logical demand.
    EXPECT_EQ(limited->check_dependencies(original, dependencies).state, DependencyState::Unknown);
    EXPECT_EQ(limited->get(current, Table::Exact, "/a"), bytes("original"));
    EXPECT_EQ(ledger->consumed_bytes(), first_control_bytes);
  }
  auto tx = limited->begin("another-control-root");
  tx->put(Table::Current, "/last", bytes("new record"));
  tx->commit();
  const auto newest = limited->view();
  {
    ScopedQueryMetadataBudget same_request(ledger);
    EXPECT_EQ(limited->check_dependencies(original, dependencies).state, DependencyState::Unknown);
    EXPECT_EQ(ledger->consumed_bytes(), first_control_bytes + encode_head(newest.head).size());
    EXPECT_EQ(limited->get(newest, Table::Current, "/last"), bytes("new record"));
    EXPECT_GT(ledger->consumed_bytes(), first_control_bytes + encode_head(newest.head).size());
    const auto charged = ledger->consumed_bytes();
    EXPECT_EQ(limited->get(newest, Table::Current, "/last"), bytes("new record"));
    EXPECT_EQ(ledger->consumed_bytes(), charged);
    EXPECT_TRUE(same_request.status().ok());
  }
  EXPECT_FALSE(limited->frozen());
}

namespace {
class StoreBarrier {
public:
  void enter_once() {
    if (!first_.exchange(false)) return;
    std::unique_lock<std::mutex> lock(mutex_);
    entered_ = true;
    condition_.notify_all();
    condition_.wait(lock, [&] { return released_; });
  }
  bool wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(5), [&] { return entered_; });
  }
  void release() {
    { std::lock_guard<std::mutex> lock(mutex_); released_ = true; }
    condition_.notify_all();
  }
private:
  std::atomic<bool> first_{true};
  std::mutex mutex_;
  std::condition_variable condition_;
  bool entered_{false}, released_{false};
};

void verify_staging_progress(Owner& owner, MemoryStore& store, bool read) {
  const auto before = owner.view();
  owner.clear_read_cache();
  StoreBarrier barrier;
  if (read) store.before_get = [&](const ObjectRef& ref) {
    if (ref.type == "catalog-page") barrier.enter_once();
  };
  else store.before_put = [&](const ObjectRef& ref) {
    if (ref.type == "catalog-record") barrier.enter_once();
  };
  auto writer = std::async(std::launch::async, [&] {
    auto tx = owner.begin("first-staging-writer");
    if (read) EXPECT_EQ(tx->get(Table::Exact, "/base"), bytes("old"));
    tx->put(Table::Current, "/writer-one", bytes("new"));
    tx->commit();
  });
  const bool entered = barrier.wait();
  std::promise<void> observer_started, other_writer_started;
  auto observation_attempt = observer_started.get_future();
  auto writer_attempt = other_writer_started.get_future();
  auto observer = std::async(std::launch::async, [&] {
    observer_started.set_value();
    const auto view = owner.view();
    auto gate = owner.admit(view.head);
    return view.head;
  });
  auto other_writer = std::async(std::launch::async, [&] {
    other_writer_started.set_value();
    auto tx = owner.begin("second-staging-writer");
    const auto observed_sequence = owner.view().head.sequence;
    tx->put(Table::Current, "/writer-two", bytes("second"));
    tx->commit();
    return observed_sequence;
  });
  const bool observer_was_started = observation_attempt.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  const bool writer_was_started = writer_attempt.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  const auto observed_state = observer.wait_for(std::chrono::seconds(2));
  const auto writer_state = other_writer.wait_for(std::chrono::milliseconds(50));
  // Release before assertions or future destruction, including on a failed
  // implementation, so the test reports the lock regression instead of hanging.
  barrier.release();
  writer.get();
  const auto second_writer_base = other_writer.get();
  const auto observed = observer.get();
  store.before_get = {}; store.before_put = {};
  EXPECT_TRUE(entered);
  EXPECT_TRUE(observer_was_started);
  EXPECT_TRUE(writer_was_started);
  EXPECT_EQ(observed_state, std::future_status::ready);
  EXPECT_EQ(observed, before.head);
  EXPECT_EQ(writer_state, std::future_status::timeout);
  EXPECT_EQ(second_writer_base, before.head.sequence + 1);
  const auto final = owner.view();
  EXPECT_EQ(final.head.sequence, before.head.sequence + 2);
  EXPECT_EQ(owner.get(final, Table::Current, "/writer-one"), bytes("new"));
  EXPECT_EQ(owner.get(final, Table::Current, "/writer-two"), bytes("second"));
}
} // namespace

TEST_F(VersionDirectoryOwner, TransactionPreparingStoreReadAllowsAdmissionButSerializesWriters) {
  put_all("/base", bytes("old"));
  verify_staging_progress(*owner, *store, true);
}

TEST_F(VersionDirectoryOwner, TransactionPreparingStoreWriteAllowsAdmissionButSerializesWriters) {
  put_all("/base", bytes("old"));
  verify_staging_progress(*owner, *store, false);
}

TEST_F(VersionDirectoryOwner, FinalHeadWriteSerializesAdmissionAndCommittedView) {
  const auto before = owner->view();
  StoreBarrier barrier;
  store->before_head = [&] { barrier.enter_once(); };
  auto writer = std::async(std::launch::async, [&] { put_all("/commit", bytes("value")); });
  const bool entered = barrier.wait();
  std::promise<void> admission_started, view_started;
  auto admission_attempt = admission_started.get_future();
  auto view_attempt = view_started.get_future();
  auto admission = std::async(std::launch::async, [&]() -> std::optional<ErrorCode> {
    admission_started.set_value();
    try { auto gate = owner->admit(before.head); return {}; }
    catch (const Error& error) { return error.code(); }
  });
  auto view = std::async(std::launch::async, [&] {
    view_started.set_value(); return owner->view().head;
  });
  const bool admission_was_started = admission_attempt.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  const bool view_was_started = view_attempt.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  const auto admission_state = admission.wait_for(std::chrono::milliseconds(50));
  const auto view_state = view.wait_for(std::chrono::milliseconds(50));
  barrier.release();
  writer.get();
  const auto denied = admission.get();
  const auto published = view.get();
  store->before_head = {};
  EXPECT_TRUE(entered);
  EXPECT_TRUE(admission_was_started);
  EXPECT_TRUE(view_was_started);
  EXPECT_EQ(admission_state, std::future_status::timeout);
  EXPECT_EQ(view_state, std::future_status::timeout);
  EXPECT_EQ(denied, ErrorCode::Conflict);
  EXPECT_EQ(published.sequence, before.head.sequence + 1);
  EXPECT_EQ(published, owner->view().head);
}

TEST_F(VersionDirectoryOwner, UnknownHeadFinalWindowReleasesWaitersOnlyIntoFrozenState) {
  const auto before = owner->view();
  const auto writes = store->head_writes;
  store->write = MemoryStore::Write::UnknownNotCommitted;
  StoreBarrier barrier;
  store->before_head = [&] { barrier.enter_once(); };
  auto writer = std::async(std::launch::async, [&]() -> std::optional<ErrorCode> {
    try { put_all("/ambiguous", bytes("value")); return {}; }
    catch (const Error& error) { return error.code(); }
  });
  const bool entered = barrier.wait();
  std::promise<void> admission_started;
  auto attempt = admission_started.get_future();
  auto admission = std::async(std::launch::async, [&]() -> std::optional<ErrorCode> {
    admission_started.set_value();
    try { auto gate = owner->admit(before.head); return {}; }
    catch (const Error& error) { return error.code(); }
  });
  const bool started = attempt.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  const auto state = admission.wait_for(std::chrono::milliseconds(50));
  barrier.release();
  const auto write_error = writer.get();
  const auto read_error = admission.get();
  store->before_head = {};
  EXPECT_TRUE(entered);
  EXPECT_TRUE(started);
  EXPECT_EQ(state, std::future_status::timeout);
  EXPECT_EQ(write_error, ErrorCode::Frozen);
  EXPECT_EQ(read_error, ErrorCode::Frozen);
  EXPECT_TRUE(owner->frozen());
  EXPECT_EQ(store->head_writes, writes + 1);
  EXPECT_THROW(owner->begin("must-not-retry"), Error);
  EXPECT_EQ(store->head_writes, writes + 1);
}

} // namespace iotdbfs::versioning
