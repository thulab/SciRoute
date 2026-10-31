#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/storage/versioning/detail/PayloadBlockFlights.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <tuple>

namespace iotdbfs::versioning {
namespace {
using namespace std::chrono_literals;
const NamespaceIdentity coalesce_identity{"123e4567-e89b-42d3-a456-426614174000",
                                          "123e4567-e89b-42d3-a456-426614174001"};

// Only get_objects is gated. Initialization and transaction verification reads
// cannot accidentally satisfy the physical-read barriers used by these tests.
class CoalescingStore final : public Store {
public:
  enum class Damage { None, Count, Size, Digest };
  using Key = std::tuple<std::string, std::string, std::string, std::string, ContentId>;
  static Key key(const NamespaceIdentity& ns, const ObjectRef& ref) {
    return {ns.namespace_id, ns.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<Bytes> read_head() override {
    std::lock_guard<std::mutex> lock(mutex_); return head_;
  }
  Bytes get_object(const NamespaceIdentity& ns, const ObjectRef& ref) override {
    std::lock_guard<std::mutex> lock(mutex_); return objects_.at(key(ns, ref));
  }
  std::vector<Bytes> get_objects(const NamespaceIdentity& ns,
      const std::vector<ObjectReadRequest>& requests) override {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto ordinal = ++calls_;
    ++active_;
    peak_ = std::max(peak_, active_);
    batch_sizes_.push_back(requests.size());
    changed_.notify_all();
    // Failure paths still finish if an assertion or implementation defect stops
    // the coordinating thread from releasing the gate.
    if (!changed_.wait_for(lock, 5s, [&] { return !blocked_; })) {
      --active_;
      throw Error(ErrorCode::Unavailable, "test physical-read barrier timed out");
    }
    std::vector<Bytes> result;
    for (const auto& request : requests) result.push_back(objects_.at(key(ns, request.ref)));
    --active_;
    if (ordinal == 1 && damage_ != Damage::None) {
      if (damage_ == Damage::Count) result.clear();
      else if (damage_ == Damage::Size) result.front().pop_back();
      else result.front().front() ^= 1;
    }
    return result;
  }
  void put_object(const NamespaceIdentity& ns, const ObjectRef& ref, const Bytes& bytes) override {
    std::lock_guard<std::mutex> lock(mutex_); objects_[key(ns, ref)] = bytes;
  }
  HeadWriteResult write_head(const Bytes& bytes) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (unknown_head_) return HeadWriteResult::Indeterminate;
    head_ = bytes; return HeadWriteResult::Acknowledged;
  }
  void block(Damage damage = Damage::None) {
    std::lock_guard<std::mutex> lock(mutex_);
    blocked_ = true; damage_ = damage;
    calls_ = active_ = peak_ = 0; batch_sizes_.clear();
  }
  bool wait_calls(unsigned count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 2s, [&] { return calls_ >= count; });
  }
  void release() {
    { std::lock_guard<std::mutex> lock(mutex_); blocked_ = false; }
    changed_.notify_all();
  }
  void unknown_head() { std::lock_guard<std::mutex> lock(mutex_); unknown_head_ = true; }
  unsigned calls() const { std::lock_guard<std::mutex> lock(mutex_); return calls_; }
  unsigned peak() const { std::lock_guard<std::mutex> lock(mutex_); return peak_; }
  std::vector<std::size_t> batches() const {
    std::lock_guard<std::mutex> lock(mutex_); return batch_sizes_;
  }
private:
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::map<Key, Bytes> objects_;
  std::optional<Bytes> head_;
  bool blocked_{false}, unknown_head_{false};
  Damage damage_{Damage::None};
  unsigned calls_{0}, active_{0}, peak_{0};
  std::vector<std::size_t> batch_sizes_;
};

class CoalescingJournal final : public Journal {
public:
  std::optional<JournalState> load() override {
    std::lock_guard<std::mutex> lock(mutex_); return state_;
  }
  void save(const JournalState& state) override {
    std::lock_guard<std::mutex> lock(mutex_); state_ = state;
  }
private:
  std::mutex mutex_;
  std::optional<JournalState> state_;
};

template<class Predicate>
bool observed(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::yield();
  }
  return true;
}

template<class Action>
void coalesce_error(ErrorCode expected, Action action) {
  try { action(); FAIL() << "operation unexpectedly succeeded"; }
  catch (const Error& error) { EXPECT_EQ(error.code(), expected) << error.what(); }
}

class PayloadReadCoalescing : public ::testing::Test {
protected:
  std::shared_ptr<CoalescingStore> store = std::make_shared<CoalescingStore>();
  std::shared_ptr<CoalescingJournal> journal = std::make_shared<CoalescingJournal>();
  std::unique_ptr<Owner> owner;
  View view;
  const Bytes value = Bytes(4096, 37);
  ObjectReadRequest request;
  void SetUp() override { reset(true); }
  void reset(bool enabled) {
    OwnerConfig config;
    config.coalesce_payload_reads = enabled;
    config.payload_cache_bytes = 0; // These cases isolate the existing in-flight/request path.
    owner = std::make_unique<Owner>(store, journal, config);
    owner->initialize(coalesce_identity);
    view = owner->view();
    request = {object_ref("payload-block", value), value.size()};
    store->put_object(coalesce_identity, request.ref, value);
  }
  std::vector<Bytes> read(const ObjectReadRequest& item, bool force = false) {
    std::uint64_t metadata = 19;
    auto result = owner->read_artifacts(view, {item}, metadata, true, force);
    EXPECT_EQ(metadata, 19u); // Payload never consumes the directory metadata ledger.
    return result;
  }
  auto async_read(const ObjectReadRequest& item, bool force = false) {
    return std::async(std::launch::async, [&, item, force] { return read(item, force); });
  }
  void enable_resident_cache() {
    owner=std::make_unique<Owner>(store,journal);
    owner->recover();view=owner->view();
  }
};

TEST_F(PayloadReadCoalescing, CrossRequestCacheKeepsPrivateOutputsAndLogicalAccounting) {
  enable_resident_cache();
  auto first=read(request);
  const auto before=owner->stats();
  auto second=read(request);
  EXPECT_EQ(store->calls(),1u);
  EXPECT_EQ(second,std::vector<Bytes>{value});
  first[0][0]^=1;EXPECT_EQ(second[0],value);
  const auto after=owner->stats();
  EXPECT_EQ(after.payload_cache_hits,before.payload_cache_hits+1);
  EXPECT_EQ(after.payload_cache_resident_entries,1u);
  EXPECT_EQ(after.payload_cache_resident_bytes,value.size());
  EXPECT_EQ(after.object_reads,before.object_reads+1); // Existing counters remain logical.
  EXPECT_EQ(after.hash_bytes,before.hash_bytes+value.size());
  owner->clear_read_cache();
  EXPECT_EQ(owner->stats().payload_cache_resident_bytes,0u);
  EXPECT_EQ(read(request),std::vector<Bytes>{value});
  EXPECT_EQ(store->calls(),2u);
}

TEST_F(PayloadReadCoalescing, ResidentPayloadDoesNotBypassForceIntegrityOrCacheCorruptBytes) {
  enable_resident_cache();
  EXPECT_EQ(read(request),std::vector<Bytes>{value});
  store->block(CoalescingStore::Damage::Digest);store->release();
  coalesce_error(ErrorCode::Corrupt,[&] {read(request,true);});
  EXPECT_EQ(store->calls(),1u); // Force performed its own physical Store call.
  owner->clear_read_cache();
  store->block(CoalescingStore::Damage::Digest);store->release();
  coalesce_error(ErrorCode::Corrupt,[&] {read(request);});
  EXPECT_EQ(owner->stats().payload_cache_resident_entries,0u);
  EXPECT_EQ(read(request),std::vector<Bytes>{value});
  EXPECT_EQ(store->calls(),2u); // Failed physical load was not cached/retried.
}

TEST_F(PayloadReadCoalescing, ResidentPayloadStillRequiresOwnerNamespaceReadinessAndExactLength) {
  enable_resident_cache();read(request);
  auto foreign=view;foreign.head.identity.incarnation="123e4567-e89b-42d3-a456-426614174099";
  std::uint64_t used=0;
  coalesce_error(ErrorCode::Conflict,[&] {owner->read_artifacts(foreign,{request},used,true);});
  EXPECT_EQ(store->calls(),1u);
  auto wrong_size=request;--wrong_size.size;
  coalesce_error(ErrorCode::Corrupt,[&] {read(wrong_size);});
  EXPECT_EQ(store->calls(),2u);
  store->unknown_head();
  auto tx=owner->begin("resident-freeze-check");
  tx->put(Table::Current,"/freeze-check",Bytes{'x'}); // A no-op commit never writes a head.
  coalesce_error(ErrorCode::Frozen,[&] {tx->commit();});
  EXPECT_TRUE(owner->frozen());
  coalesce_error(ErrorCode::Frozen,[&] {read(request);});
  EXPECT_EQ(store->calls(),2u);
}

TEST_F(PayloadReadCoalescing, ResidentOwnerDestructionReleasesAccountingBeforeRuntimeMembersDie) {
  enable_resident_cache();
  unsigned released=0;
  struct Pin {
    Owner* owner;unsigned* released;
    ~Pin(){(void)owner->stats();++*released;}
  };
  auto pin=std::shared_ptr<void>(new Pin{owner.get(),&released});
  std::uint64_t used=0;
  EXPECT_EQ(owner->read_artifacts(view,{request},used,true,false,pin),std::vector<Bytes>{value});
  pin.reset();EXPECT_EQ(released,0u);
  owner.reset();EXPECT_EQ(released,1u);
}

TEST_F(PayloadReadCoalescing, FourSameBlockReadsShareOnlyTheInFlightPhysicalReadAndOwnTheirOutputs) {
  store->block();
  auto leader = async_read(request);
  const bool entered = store->wait_calls(1);
  auto a = async_read(request), b = async_read(request), c = async_read(request);
  const bool joined = observed([&] { return owner->stats().payload_coalesce_joins == 3; });
  const auto during = owner->stats();
  store->release();
  auto first = leader.get(), second = a.get(), third = b.get(), fourth = c.get();
  EXPECT_TRUE(entered); EXPECT_TRUE(joined);
  EXPECT_EQ(during.payload_coalesce_active_flights, 1u);
  EXPECT_EQ(store->calls(), 1u);
  EXPECT_EQ(first, std::vector<Bytes>{value});
  EXPECT_EQ(second, first); EXPECT_EQ(third, first); EXPECT_EQ(fourth, first);
  first[0][0] ^= 1;
  EXPECT_EQ(second[0], value); EXPECT_EQ(third[0], value); EXPECT_EQ(fourth[0], value);
  EXPECT_EQ(owner->stats().payload_coalesce_retained_entries, 0u);
  EXPECT_EQ(owner->stats().payload_coalesce_retained_bytes, 0u);
  EXPECT_EQ(read(request), std::vector<Bytes>{value});
  EXPECT_EQ(store->calls(), 2u); // A completed read is never a payload cache hit.
}

TEST_F(PayloadReadCoalescing, DistinctBlocksEnterThePhysicalStoreConcurrently) {
  const Bytes other(4096, 42);
  const ObjectReadRequest other_request{object_ref("payload-block", other), other.size()};
  store->put_object(coalesce_identity, other_request.ref, other);
  store->block();
  auto a = async_read(request), b = async_read(other_request);
  const bool both_entered = store->wait_calls(2);
  store->release();
  EXPECT_EQ(a.get(), std::vector<Bytes>{value});
  EXPECT_EQ(b.get(), std::vector<Bytes>{other});
  EXPECT_TRUE(both_entered); EXPECT_EQ(store->peak(), 2u);
  EXPECT_EQ(owner->stats().payload_coalesce_joins, 0u);
}

TEST_F(PayloadReadCoalescing, DisabledCoalescingAndForcedVerificationEachKeepFourPhysicalReads) {
  for (bool force : {false, true}) {
    SCOPED_TRACE(force);
    // A fresh owner isolates counters; initialization does not use get_objects.
    store = std::make_shared<CoalescingStore>();
    journal = std::make_shared<CoalescingJournal>();
    reset(force); // Disabled for normal reads; enabled for forced reads.
    store->block();
    auto a = async_read(request, force), b = async_read(request, force);
    auto c = async_read(request, force), d = async_read(request, force);
    const bool entered = store->wait_calls(4);
    store->release();
    EXPECT_EQ(a.get(), std::vector<Bytes>{value}); EXPECT_EQ(b.get(), std::vector<Bytes>{value});
    EXPECT_EQ(c.get(), std::vector<Bytes>{value}); EXPECT_EQ(d.get(), std::vector<Bytes>{value});
    EXPECT_TRUE(entered); EXPECT_EQ(store->calls(), 4u);
    EXPECT_EQ(owner->stats().payload_coalesce_joins, 0u);
    EXPECT_EQ(owner->stats().payload_coalesce_active_flights, 0u);
  }
}

TEST_F(PayloadReadCoalescing, MultiBlockBatchKeepsOneNativeMissingSubsetBatchAndJoinsSingletonRead) {
  const Bytes other(4096, 42);
  const ObjectReadRequest second{object_ref("payload-block", other), other.size()};
  store->put_object(coalesce_identity, second.ref, other);
  store->block();
  auto single = async_read(request);
  const bool first_entered = store->wait_calls(1);
  auto batch = std::async(std::launch::async, [&] {
    std::uint64_t used = 0;
    return owner->read_artifacts(view, {request, second}, used, true);
  });
  const bool batch_entered = store->wait_calls(2);
  store->release();
  EXPECT_EQ(single.get(), std::vector<Bytes>{value});
  EXPECT_EQ(batch.get(), (std::vector<Bytes>{value, other}));
  EXPECT_TRUE(first_entered); EXPECT_TRUE(batch_entered);
  EXPECT_EQ(store->batches(), (std::vector<std::size_t>{1, 1}));
  EXPECT_EQ(owner->stats().payload_coalesce_joins, 1u);
}

TEST_F(PayloadReadCoalescing, InvalidLeaderBytesAreNotSharedAndEachWaiterMakesItsOwnFirstAttempt) {
  for (auto damage : {CoalescingStore::Damage::Count,
                      CoalescingStore::Damage::Size, CoalescingStore::Damage::Digest}) {
    SCOPED_TRACE(static_cast<int>(damage));
    store->block(damage);
    auto leader = async_read(request);
    const bool entered = store->wait_calls(1);
    const auto previous_joins = owner->stats().payload_coalesce_joins;
    auto a = async_read(request), b = async_read(request), c = async_read(request);
    const bool joined = observed([&] {
      return owner->stats().payload_coalesce_joins == previous_joins + 3;
    });
    store->release();
    coalesce_error(ErrorCode::Corrupt, [&] { leader.get(); });
    EXPECT_EQ(a.get(), std::vector<Bytes>{value}); EXPECT_EQ(b.get(), std::vector<Bytes>{value});
    EXPECT_EQ(c.get(), std::vector<Bytes>{value});
    EXPECT_TRUE(entered); EXPECT_TRUE(joined); EXPECT_EQ(store->calls(), 4u);
    EXPECT_EQ(owner->stats().payload_coalesce_retained_entries, 0u);
    EXPECT_EQ(read(request), std::vector<Bytes>{value});
    EXPECT_EQ(store->calls(), 5u);
  }
  EXPECT_EQ(owner->stats().payload_coalesce_fallbacks, 9u);
}

TEST_F(PayloadReadCoalescing, CancelledWaiterStopsWithoutCancellingTheProducerOrAnotherWaiter) {
  store->block();
  auto leader = async_read(request);
  const bool entered = store->wait_calls(1);
  std::atomic<bool> cancel{false};
  auto cancelled = std::async(std::launch::async, [&] {
    ScopedQueryStorageReadControl control([&] {
      if (cancel.load()) throw QueryStorageReadInterrupted(QueryStatus::failure(
        QueryError::Cancelled, QueryPhase::Storage, "this waiter cancelled"));
    }, {});
    return read(request);
  });
  auto survivor = async_read(request);
  const bool joined = observed([&] { return owner->stats().payload_coalesce_joins == 2; });
  cancel.store(true);
  const auto cancelled_ready = cancelled.wait_for(2s);
  const auto physical_calls_before_release = store->calls();
  const auto leader_ready = leader.wait_for(0ms);
  const auto survivor_ready = survivor.wait_for(0ms);
  store->release();
  try { cancelled.get(); FAIL() << "cancelled waiter returned data"; }
  catch (const QueryStorageReadInterrupted& error) {
    EXPECT_EQ(error.status().code(), QueryError::Cancelled);
  }
  EXPECT_EQ(leader.get(), std::vector<Bytes>{value});
  EXPECT_EQ(survivor.get(), std::vector<Bytes>{value});
  EXPECT_TRUE(entered); EXPECT_TRUE(joined);
  EXPECT_EQ(cancelled_ready, std::future_status::ready);
  EXPECT_EQ(leader_ready, std::future_status::timeout);
  EXPECT_EQ(survivor_ready, std::future_status::timeout);
  EXPECT_EQ(physical_calls_before_release, 1u); EXPECT_EQ(store->calls(), 1u);
}

TEST_F(PayloadReadCoalescing, SeparateOwnersAndIncarnationsNeverJoinEachOthersPhysicalReads) {
  for (bool same_incarnation : {true, false}) {
    SCOPED_TRACE(same_incarnation);
    // Two owner instances must not share their flights even when they address
    // the same Store and namespace. The other case also changes incarnation.
    auto other_store = same_incarnation ? store : std::make_shared<CoalescingStore>();
    auto other_journal = std::make_shared<CoalescingJournal>();
    Owner other(other_store, same_incarnation ? journal : other_journal);
    auto other_identity = coalesce_identity;
    if (!same_incarnation)
      other_identity.incarnation = "123e4567-e89b-42d3-a456-426614174002";
    if (same_incarnation) other.recover();
    else other.initialize(other_identity);
    const auto other_view = other.view();
    other_store->put_object(other_identity, request.ref, value);
    store->block();
    if (other_store != store) other_store->block();
    auto a = async_read(request);
    auto b = std::async(std::launch::async, [&] {
      std::uint64_t used = 0;
      return other.read_artifacts(other_view, {request}, used, true);
    });
    const bool first_entered = store->wait_calls(1);
    const bool second_entered = other_store->wait_calls(other_store == store ? 2 : 1);
    store->release();
    if (other_store != store) other_store->release();
    EXPECT_EQ(a.get(), std::vector<Bytes>{value}); EXPECT_EQ(b.get(), std::vector<Bytes>{value});
    EXPECT_TRUE(first_entered); EXPECT_TRUE(second_entered);
    EXPECT_EQ(store->calls(), other_store == store ? 2u : 1u);
    EXPECT_EQ(other_store->calls(), other_store == store ? 2u : 1u);
    EXPECT_EQ(other.stats().payload_coalesce_joins, 0u);
  }
}

TEST_F(PayloadReadCoalescing, InvalidRequestsAndForeignViewsFailBeforeJoiningAValidFlight) {
  store->block();
  auto leader = async_read(request);
  const bool entered = store->wait_calls(1);
  auto foreign = view;
  foreign.head.identity.incarnation = "123e4567-e89b-42d3-a456-426614174002";
  std::uint64_t used = 0;
  coalesce_error(ErrorCode::Conflict, [&] { owner->read_artifacts(foreign, {request}, used, true); });
  coalesce_error(ErrorCode::Invalid, [&] { owner->read_artifacts(view, {request, request}, used, true); });
  auto invalid = request; invalid.ref.protocol = "not-the-payload-protocol";
  coalesce_error(ErrorCode::Invalid, [&] { owner->read_artifacts(view, {invalid}, used, true); });
  const auto stats = owner->stats();
  store->release();
  EXPECT_EQ(leader.get(), std::vector<Bytes>{value});
  EXPECT_TRUE(entered); EXPECT_EQ(stats.payload_coalesce_joins, 0u); EXPECT_EQ(store->calls(), 1u);
}

TEST_F(PayloadReadCoalescing, FrozenOwnerRejectsBothCompletedSharedReadsAndNewAdmissions) {
  store->block();
  auto leader = async_read(request);
  const bool entered = store->wait_calls(1);
  auto waiter = async_read(request);
  const bool joined = observed([&] { return owner->stats().payload_coalesce_joins == 1; });
  // Reads do not hold the short publish/admission gate while waiting on I/O.
  bool admitted = false;
  { auto admission = owner->admit(view.head); admitted = true; }
  store->unknown_head();
  coalesce_error(ErrorCode::Frozen, [&] {
    auto tx = owner->begin("injected-uncertain-head");
    tx->put(Table::Current, "/freeze", Bytes{1}); tx->commit();
  });
  store->release();
  coalesce_error(ErrorCode::Frozen, [&] { leader.get(); });
  coalesce_error(ErrorCode::Frozen, [&] { waiter.get(); });
  coalesce_error(ErrorCode::Frozen, [&] { read(request); });
  coalesce_error(ErrorCode::Frozen, [&] { owner->admit(view.head); });
  EXPECT_TRUE(entered); EXPECT_TRUE(joined); EXPECT_TRUE(admitted); EXPECT_TRUE(owner->frozen());
  EXPECT_EQ(store->calls(), 1u);
  EXPECT_EQ(owner->stats().payload_coalesce_retained_entries, 0u);
}

TEST_F(PayloadReadCoalescing, FourDifferentMessageBlockSetsFetchEightUniqueBlocksInsteadOfEleven) {
  std::vector<Bytes> values;
  std::vector<ObjectReadRequest> requests;
  for(int i=0;i<8;++i) {
    values.emplace_back(4096,static_cast<std::uint8_t>(10+i));
    requests.push_back({object_ref("payload-block",values.back()),values.back().size()});
    store->put_object(coalesce_identity,requests.back().ref,values.back());
  }
  const std::vector<std::vector<std::size_t>> blocks{{0,1},{1,2,3},{3,4,5},{5,6,7}};
  store->block();
  std::vector<std::future<std::vector<Bytes>>> tasks;
  for(std::size_t i=0;i<blocks.size();++i) {
    tasks.push_back(std::async(std::launch::async,[&,i] {
      std::vector<ObjectReadRequest> query;
      for(auto j:blocks[i])query.push_back(requests[j]);
      std::uint64_t metadata=31;
      auto result=owner->read_artifacts(view,query,metadata,true);
      EXPECT_EQ(metadata,31u);
      return result;
    }));
    EXPECT_TRUE(store->wait_calls(i+1));
  }
  EXPECT_EQ(owner->stats().payload_coalesce_joins,3u);
  EXPECT_EQ(store->batches(),(std::vector<std::size_t>{2,2,2,2}));
  store->release();
  for(std::size_t i=0;i<tasks.size();++i) {
    std::vector<Bytes> expected;
    for(auto j:blocks[i])expected.push_back(values[j]);
    EXPECT_EQ(tasks[i].get(),expected);
  }
  EXPECT_EQ(store->calls(),4u);
  // Owner counters deliberately retain logical-read semantics.
  EXPECT_EQ(owner->stats().payload_coalesce_retained_bytes,0u);
  EXPECT_EQ(owner->stats().payload_coalesce_active_flights,0u);
}

TEST_F(PayloadReadCoalescing, ReverseBatchesKeepCallerOrderingAndOnePhysicalValidatedBatch) {
  const Bytes other(4096,42);
  const ObjectReadRequest second{object_ref("payload-block",other),other.size()};
  store->put_object(coalesce_identity,second.ref,other);
  store->block();
  auto a=std::async(std::launch::async,[&] {
    std::uint64_t used=0;return owner->read_artifacts(view,{request,second},used,true);
  });
  EXPECT_TRUE(store->wait_calls(1));
  auto b=std::async(std::launch::async,[&] {
    std::uint64_t used=0;return owner->read_artifacts(view,{second,request},used,true);
  });
  EXPECT_TRUE(observed([&] {return owner->stats().payload_coalesce_joins==2;}));
  store->release();
  EXPECT_EQ(a.get(),(std::vector<Bytes>{value,other}));
  EXPECT_EQ(b.get(),(std::vector<Bytes>{other,value}));
  EXPECT_EQ(store->batches(),(std::vector<std::size_t>{2}));
}

TEST_F(PayloadReadCoalescing, InvalidWholeBatchPublishesNoGoodPrefixAndFollowerReadsItsOwnBatch) {
  for(auto damage:{CoalescingStore::Damage::Count,CoalescingStore::Damage::Size,
                   CoalescingStore::Damage::Digest}) {
    SCOPED_TRACE(static_cast<int>(damage));
    const Bytes other(4096,42);
    const ObjectReadRequest second{object_ref("payload-block",other),other.size()};
    store->put_object(coalesce_identity,second.ref,other);
    store->block(damage);
    const auto before=owner->stats().payload_coalesce_joins;
    auto a=std::async(std::launch::async,[&] {
      std::uint64_t used=0;return owner->read_artifacts(view,{request,second},used,true);
    });
    EXPECT_TRUE(store->wait_calls(1));
    auto b=std::async(std::launch::async,[&] {
      std::uint64_t used=0;return owner->read_artifacts(view,{second,request},used,true);
    });
    EXPECT_TRUE(observed([&] {return owner->stats().payload_coalesce_joins==before+2;}));
    store->release();
    coalesce_error(ErrorCode::Corrupt,[&] {a.get();});
    EXPECT_EQ(b.get(),(std::vector<Bytes>{other,value}));
    EXPECT_EQ(store->batches(),(std::vector<std::size_t>{2,2}));
    EXPECT_EQ(owner->stats().payload_coalesce_retained_bytes,0u);
  }
}

TEST_F(PayloadReadCoalescing, ScopedReuseStillChecksForceNamespaceAndFrozenStateAndDoesNotChargeMetadata) {
  const Bytes other(4096,42);
  const ObjectReadRequest second{object_ref("payload-block",other),other.size()};
  store->put_object(coalesce_identity,second.ref,other);
  store->release();
  {
    detail::PayloadBlockFlights::ReuseScope original_request;
    std::uint64_t used=21;
    EXPECT_EQ(owner->read_artifacts(view,{request,second},used,true),
              (std::vector<Bytes>{value,other}));
    EXPECT_EQ(read(request),std::vector<Bytes>{value});
    EXPECT_EQ(store->calls(),1u);
    EXPECT_EQ(used,21u);
    EXPECT_EQ(read(request,true),std::vector<Bytes>{value});
    EXPECT_EQ(store->calls(),2u);
    auto foreign=view;
    foreign.head.identity.incarnation="123e4567-e89b-42d3-a456-426614174002";
    coalesce_error(ErrorCode::Conflict,[&] {owner->read_artifacts(foreign,{request},used,true);});
    store->unknown_head();
    coalesce_error(ErrorCode::Frozen,[&] {
      auto tx=owner->begin("freeze-scoped-payload");
      tx->put(Table::Current,"/freeze-scoped",Bytes{1});tx->commit();
    });
    coalesce_error(ErrorCode::Frozen,[&] {read(request);});
  }
  EXPECT_EQ(owner->stats().payload_coalesce_retained_bytes,0u);
}

} // namespace
} // namespace iotdbfs::versioning
