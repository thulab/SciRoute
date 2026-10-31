#include "iotdbfs/storage/versioning/detail/PayloadBlockFlights.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

using namespace std::chrono_literals;
using iotdbfs::versioning::Bytes;
using iotdbfs::versioning::detail::PayloadBlockFlights;

namespace {
// Timeouts are failure guards only. Test ordering uses explicit notifications.
class Gate {
public:
  void arrive() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++arrived_;
    changed_.notify_all();
  }
  bool await(std::size_t count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 4s, [&] { return arrived_ >= count; });
  }
  void block() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++arrived_;
    changed_.notify_all();
    if (!changed_.wait_for(lock, 4s, [&] { return open_; }))
      throw std::runtime_error("test gate timed out");
  }
  void open() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true;
    changed_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable changed_;
  std::size_t arrived_{0};
  bool open_{false};
};
void checkpoint() {}
auto attached_checkpoint(Gate& attached) {
  return [&attached, count = 0]() mutable {
    // First checkpoint precedes lookup; second proves lookup has joined.
    if (++count == 2) attached.arrive();
  };
}
}

TEST(PayloadBlockFlights, FourIdenticalReadsShareOneProducerAndImmutableBytes) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  std::atomic<int> loads{0};
  auto loader = [&] { ++loads; producer.block(); return Bytes{1, 2, 3}; };
  auto leader = std::async(std::launch::async, [&] {
    return flights.read("namespace/incarnation/payload/v1/digest", 3, loader, checkpoint);
  });
  EXPECT_TRUE(producer.await(1));
  std::vector<std::future<PayloadBlockFlights::Result>> followers;
  for (int i = 0; i < 3; ++i)
    followers.push_back(std::async(std::launch::async, [&] {
      return flights.read("namespace/incarnation/payload/v1/digest", 3, loader,
                          attached_checkpoint(attached));
    }));
  EXPECT_TRUE(attached.await(3));
  EXPECT_EQ(flights.stats().joins, 3u);
  producer.open();
  const auto first = leader.get();
  EXPECT_FALSE(first.joined);
  for (auto& task : followers) {
    const auto result = task.get();
    EXPECT_TRUE(result.joined);
    EXPECT_FALSE(result.bypassed);
    EXPECT_EQ(result.bytes.get(), first.bytes.get());
    EXPECT_EQ(*result.bytes, (Bytes{1, 2, 3}));
  }
  EXPECT_EQ(loads, 1);
  EXPECT_EQ(flights.stats().active_flights, 0u);
}

TEST(PayloadBlockFlights, DistinctKeysAndSizesLoadConcurrently) {
  PayloadBlockFlights flights;
  Gate producers;
  std::vector<std::future<PayloadBlockFlights::Result>> tasks;
  const std::vector<std::pair<std::string, std::size_t>> keys{
    {"ns1/inc1/type/protocol/digest1", 1},
    {"ns2/inc1/type/protocol/digest1", 1},
    {"ns1/inc2/type/protocol/digest1", 1},
    {"ns1/inc1/type/protocol/digest2", 1},
    {"ns1/inc1/type/protocol/digest1", 2}};
  for (const auto& key : keys)
    tasks.push_back(std::async(std::launch::async, [&, key] {
      return flights.read(key.first, key.second, [&] {
        producers.block(); return Bytes(key.second, 9);
      }, checkpoint);
    }));
  EXPECT_TRUE(producers.await(keys.size()));
  EXPECT_EQ(flights.stats().active_flights, keys.size());
  EXPECT_EQ(flights.stats().joins, 0u);
  producers.open();
  for (auto& task : tasks) EXPECT_FALSE(task.get().joined);
}

TEST(PayloadBlockFlights, CompletedFlightIsNotAResidentCache) {
  PayloadBlockFlights flights;
  int loads = 0;
  auto loader = [&] { return Bytes{static_cast<std::uint8_t>(++loads)}; };
  const auto first = flights.read("same", 1, loader, checkpoint);
  const auto second = flights.read("same", 1, loader, checkpoint);
  EXPECT_EQ(*first.bytes, Bytes{1});
  EXPECT_EQ(*second.bytes, Bytes{2});
  EXPECT_EQ(loads, 2);
  EXPECT_NE(first.bytes.get(), second.bytes.get());
  EXPECT_EQ(flights.stats().retained_entries, 2u);
}

TEST(PayloadBlockFlights, ProducerFailureGivesFollowerItsOwnFirstAttemptAndError) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  int producer_loads = 0, follower_loads = 0;
  auto leader = std::async(std::launch::async, [&] {
    try {
      flights.read("same", 1, [&]() -> Bytes {
        ++producer_loads; producer.block(); throw std::runtime_error("producer error");
      }, checkpoint);
      return std::string("no error");
    } catch (const std::exception& error) { return std::string(error.what()); }
  });
  EXPECT_TRUE(producer.await(1));
  auto follower = std::async(std::launch::async, [&] {
    try {
      flights.read("same", 1, [&]() -> Bytes {
        ++follower_loads; throw std::runtime_error("follower error");
      }, attached_checkpoint(attached));
      return std::string("no error");
    } catch (const std::exception& error) { return std::string(error.what()); }
  });
  EXPECT_TRUE(attached.await(1));
  producer.open();
  EXPECT_EQ(leader.get(), "producer error");
  EXPECT_EQ(follower.get(), "follower error");
  EXPECT_EQ(producer_loads, 1);
  EXPECT_EQ(follower_loads, 1);
  EXPECT_EQ(flights.stats().fallbacks, 1u);
  EXPECT_EQ(flights.stats().retained_entries, 0u);
}

TEST(PayloadBlockFlights, CancelledProducerDoesNotCancelSurvivingFollower) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  std::atomic<bool> cancel{false};
  int follower_loads = 0;
  Bytes follower_bytes{2};
  const auto original_buffer = follower_bytes.data();
  auto leader = std::async(std::launch::async, [&] {
    EXPECT_THROW(flights.read("same", 1, [&] {
      producer.block(); return Bytes{1};
    }, [&] { if (cancel) throw std::runtime_error("producer cancelled"); }), std::runtime_error);
  });
  EXPECT_TRUE(producer.await(1));
  auto follower = std::async(std::launch::async, [&] {
    return flights.read("same", 1, [&] { ++follower_loads; return std::move(follower_bytes); },
                        attached_checkpoint(attached));
  });
  EXPECT_TRUE(attached.await(1));
  cancel = true;
  producer.open();
  leader.get();
  const auto result = follower.get();
  EXPECT_EQ(result.value(), Bytes{2});
  EXPECT_FALSE(result.bytes);
  EXPECT_EQ(result.owned.data(), original_buffer);
  EXPECT_TRUE(result.joined);
  EXPECT_TRUE(result.bypassed);
  EXPECT_EQ(follower_loads, 1);
}

TEST(PayloadBlockFlights, CancelledFollowerLeavesProducerAndOtherFollowerUnaffected) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  std::atomic<bool> cancel{false};
  std::atomic<int> loads{0};
  auto loader = [&] { ++loads; producer.block(); return Bytes{7}; };
  auto leader = std::async(std::launch::async, [&] {
    return flights.read("same", 1, loader, checkpoint);
  });
  EXPECT_TRUE(producer.await(1));
  auto cancelled = std::async(std::launch::async, [&] {
    int checks = 0;
    EXPECT_THROW(flights.read("same", 1, loader, [&] {
      if (++checks == 2) attached.arrive();
      if (cancel) throw std::runtime_error("follower cancelled");
    }), std::runtime_error);
  });
  auto survivor = std::async(std::launch::async, [&] {
    return flights.read("same", 1, loader, attached_checkpoint(attached));
  });
  EXPECT_TRUE(attached.await(2));
  cancel = true;
  EXPECT_EQ(cancelled.wait_for(1s), std::future_status::ready);
  cancelled.get();
  EXPECT_EQ(flights.stats().active_flights, 1u);
  producer.open();
  EXPECT_EQ(*leader.get().bytes, Bytes{7});
  EXPECT_EQ(*survivor.get().bytes, Bytes{7});
  EXPECT_EQ(loads, 1);
}

TEST(PayloadBlockFlights, CompletedResultsRemainChargedUntilLastConsumerRelease) {
  PayloadBlockFlights flights(2, 4);
  auto pin = std::make_shared<int>(1);
  std::weak_ptr<int> weak = pin;
  auto producer_pin = pin;
  auto held = flights.read("a", 4, [] { return Bytes(4, 1); }, checkpoint, producer_pin);
  producer_pin.reset();
  pin.reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_EQ(flights.stats().active_flights, 0u);
  EXPECT_EQ(flights.stats().retained_bytes, 4u);
  const auto bypass = flights.read("b", 1, [] { return Bytes{2}; }, checkpoint);
  EXPECT_TRUE(bypass.bypassed);
  EXPECT_EQ(flights.stats().retained_bytes, 4u);
  auto second_consumer = held.bytes;
  held.bytes.reset();
  EXPECT_EQ(flights.stats().retained_bytes, 4u);
  EXPECT_FALSE(weak.expired());
  second_consumer.reset();
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
  EXPECT_EQ(flights.stats().retained_entries, 0u);
  EXPECT_TRUE(weak.expired());
  const auto accepted = flights.read("c", 4, [] { return Bytes(4, 3); }, checkpoint);
  EXPECT_FALSE(accepted.bypassed);
}

TEST(PayloadBlockFlights, EntryCapAppliesToRetainedOutputsAsWellAsActiveLoads) {
  PayloadBlockFlights flights(1, 1024);
  auto held = flights.read("a", 1, [] { return Bytes{1}; }, checkpoint);
  EXPECT_TRUE(flights.read("b", 1, [] { return Bytes{2}; }, checkpoint).bypassed);
  held.bytes.reset();
  EXPECT_FALSE(flights.read("b", 1, [] { return Bytes{2}; }, checkpoint).bypassed);
}

TEST(PayloadBlockFlights, DisabledOversizeAndZeroSizeBypassWithoutSharing) {
  for (const auto limits : {std::pair<std::size_t, std::size_t>{0, 100}, {4, 0}, {4, 1}}) {
    PayloadBlockFlights flights(limits.first, limits.second);
    int loads = 0;
    for (int i = 0; i < 2; ++i)
      EXPECT_TRUE(flights.read("same", 2, [&] { ++loads; return Bytes{1, 2}; }, checkpoint).bypassed);
    EXPECT_EQ(loads, 2);
    EXPECT_EQ(flights.stats().retained_entries, 0u);
  }
  PayloadBlockFlights flights;
  EXPECT_TRUE(flights.read("zero", 0, [] { return Bytes{}; }, checkpoint).bypassed);
}

TEST(PayloadBlockFlights, SizeMismatchDoesNotPublishUnexpectedBytesOrExceedBound) {
  PayloadBlockFlights flights(1, 1);
  const auto result = flights.read("a", 1, [] { return Bytes(10, 1); }, checkpoint);
  EXPECT_EQ(result.value().size(), 10u);
  EXPECT_FALSE(result.bytes);
  EXPECT_TRUE(result.bypassed);
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
  EXPECT_EQ(flights.stats().active_flights, 0u);
}

TEST(PayloadBlockFlights, CheckpointsAndAccountingDestructorsMayReenterStats) {
  PayloadBlockFlights flights;
  struct Pin {
    PayloadBlockFlights& flights;
    bool& released;
    ~Pin() { (void)flights.stats(); released = true; }
  };
  bool released = false;
  auto pin = std::shared_ptr<void>(new Pin{flights, released});
  auto result = flights.read("a", 1, [] { return Bytes{1}; },
                            [&] { (void)flights.stats(); }, pin);
  pin.reset();
  EXPECT_FALSE(released);
  result.bytes.reset();
  EXPECT_TRUE(released);
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, InitialCancellationDoesNotAdmitOrLoad) {
  PayloadBlockFlights flights;
  int loads = 0;
  EXPECT_THROW(flights.read("a", 1, [&] { ++loads; return Bytes{1}; }, [] {
    throw std::runtime_error("cancelled");
  }), std::runtime_error);
  EXPECT_EQ(loads, 0);
  EXPECT_EQ(flights.stats().leaders, 0u);
}

TEST(PayloadBlockFlights, PrivatePathsMoveLoaderBufferWithoutAnOutputAllocation) {
  static_assert(std::is_nothrow_move_constructible<Bytes>::value,
                "private results must move without allocating");
  static_assert(std::is_nothrow_move_constructible<PayloadBlockFlights::Result>::value,
                "returning an already loaded private result must not allocate");
  for (const bool disabled : {true, false}) {
    PayloadBlockFlights flights(disabled ? 0 : 1, 1);
    Bytes prepared(disabled ? 1 : 2, 42);
    const auto original_buffer = prepared.data();
    auto result = flights.read("block", 1, [&] { return std::move(prepared); }, checkpoint);
    EXPECT_TRUE(result.bypassed);
    EXPECT_FALSE(result.bytes);
    EXPECT_EQ(result.owned.data(), original_buffer);
    EXPECT_EQ(result.value().data(), original_buffer);
    EXPECT_EQ(result.value().size(), disabled ? 1u : 2u);
    EXPECT_EQ(flights.stats().retained_entries, 0u);
  }
}

namespace {
using BlockKeys = std::vector<std::pair<std::string, std::size_t>>;
std::vector<Bytes> block_values(const BlockKeys& keys,
                               const std::vector<std::size_t>* indices) {
  std::vector<Bytes> values;
  if (indices) {
    for (auto i : *indices) values.emplace_back(keys[i].second, keys[i].first.front());
  } else for (const auto& key : keys) values.emplace_back(key.second, key.first.front());
  return values;
}
}

TEST(PayloadBlockFlights, ReverseOrderedBatchesShareOneValidatedBatchWithoutDeadlock) {
  PayloadBlockFlights flights;
  const BlockKeys ab{{"a", 1}, {"b", 1}}, ba{{"b", 1}, {"a", 1}};
  Gate producer, attached;
  std::atomic<int> loads{0};
  auto leader = std::async(std::launch::async, [&] {
    return flights.read_batch(ab, [&](const auto* indices) {
      ++loads; producer.block(); return block_values(ab, indices);
    }, checkpoint);
  });
  EXPECT_TRUE(producer.await(1));
  auto follower = std::async(std::launch::async, [&] {
    return flights.read_batch(ba, [&](const auto* indices) {
      ++loads; return block_values(ba, indices);
    }, attached_checkpoint(attached));
  });
  EXPECT_TRUE(attached.await(1));
  EXPECT_EQ(flights.stats().joins, 2u);
  producer.open();
  EXPECT_EQ(leader.get(), (std::vector<Bytes>{{'a'}, {'b'}}));
  EXPECT_EQ(follower.get(), (std::vector<Bytes>{{'b'}, {'a'}}));
  EXPECT_EQ(loads, 1);
  EXPECT_EQ(flights.stats().retained_entries, 0u);
}

TEST(PayloadBlockFlights, FourPartlyOverlappingBatchesReadEightUniqueBlocksInFourBatches) {
  PayloadBlockFlights flights;
  const std::vector<BlockKeys> keys{
    {{"a", 1}, {"b", 1}}, {{"b", 1}, {"c", 1}, {"d", 1}},
    {{"d", 1}, {"e", 1}, {"f", 1}}, {{"f", 1}, {"g", 1}, {"h", 1}}};
  Gate producers;
  std::atomic<int> loads{0}, physical{0};
  std::vector<std::future<std::vector<Bytes>>> tasks;
  for (std::size_t i=0; i<keys.size(); ++i) {
    tasks.push_back(std::async(std::launch::async, [&, i] {
      return flights.read_batch(keys[i], [&](const auto* indices) {
        ++loads; physical += indices?indices->size():keys[i].size();
        producers.block(); return block_values(keys[i], indices);
      }, checkpoint);
    }));
    // Establish overlap without inserting any product wait/coalescing window.
    EXPECT_TRUE(producers.await(i+1));
  }
  EXPECT_EQ(flights.stats().leaders, 8u);
  EXPECT_EQ(flights.stats().joins, 3u);
  producers.open();
  for(std::size_t i=0;i<tasks.size();++i)
    EXPECT_EQ(tasks[i].get(), block_values(keys[i], nullptr));
  EXPECT_EQ(loads, 4); EXPECT_EQ(physical, 8);
  EXPECT_EQ(flights.stats().active_flights, 0u);
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, CompletedOwnedBlockRemainsJoinableWhileBatchWaitsForAnotherProducer) {
  PayloadBlockFlights flights;
  Gate a_producer, b_published;
  const BlockKeys ab{{"a", 1}, {"b", 1}}, bc{{"b", 1}, {"c", 1}};
  auto a = std::async(std::launch::async, [&] {
    return flights.read("a", 1, [&] { a_producer.block(); return Bytes{'a'}; }, checkpoint);
  });
  EXPECT_TRUE(a_producer.await(1));
  auto waiting = std::async(std::launch::async, [&] {
    return flights.read_batch(ab, [&](const auto* indices) {
      EXPECT_EQ(*indices, (std::vector<std::size_t>{1}));
      return block_values(ab, indices);
    }, [&, n=0]() mutable { if (++n==4) b_published.arrive(); });
  });
  EXPECT_TRUE(b_published.await(1));
  const auto bc_result=flights.read_batch(bc, [&](const auto* indices) {
    EXPECT_EQ(*indices, (std::vector<std::size_t>{1}));
    return block_values(bc, indices);
  }, checkpoint);
  EXPECT_EQ(bc_result, (std::vector<Bytes>{{'b'}, {'c'}}));
  a_producer.open();
  EXPECT_EQ(a.get().value(), Bytes{'a'});
  EXPECT_EQ(waiting.get(), (std::vector<Bytes>{{'a'}, {'b'}}));
  EXPECT_EQ(flights.stats().joins, 2u);
}

TEST(PayloadBlockFlights, FailedOverlappingProducerDoesNotReplayItsErrorOrReloadOwnSuccessfulBlock) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  const BlockKeys bc{{"b", 1}, {"c", 1}};
  auto b = std::async(std::launch::async, [&] {
    try {
      flights.read("b", 1, [&]() -> Bytes {
        producer.block(); throw std::runtime_error("producer b error");
      }, checkpoint);
      return std::string("unexpected success");
    } catch(const std::exception& error) { return std::string(error.what()); }
  });
  EXPECT_TRUE(producer.await(1));
  std::vector<std::vector<std::size_t>> calls;
  auto bc_task=std::async(std::launch::async, [&] {
    try {
      flights.read_batch(bc, [&](const auto* indices) -> std::vector<Bytes> {
        calls.push_back(*indices);
        if(indices->front()==0)throw std::runtime_error("own b error");
        return block_values(bc, indices);
      }, [&, n=0]() mutable { if(++n==4)attached.arrive(); });
      return std::string("unexpected success");
    } catch(const std::exception& error) { return std::string(error.what()); }
  });
  EXPECT_TRUE(attached.await(1));
  producer.open();
  EXPECT_EQ(b.get(), "producer b error");
  EXPECT_EQ(bc_task.get(), "own b error");
  EXPECT_EQ(calls, (std::vector<std::vector<std::size_t>>{{1}, {0}}));
  EXPECT_EQ(flights.stats().fallbacks, 1u);
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, FailedWholeBatchPublishesNoPartialBlockAndFollowerHasOwnFirstBatch) {
  PayloadBlockFlights flights;
  const BlockKeys ab{{"a", 1}, {"b", 1}};
  Gate producer, attached;
  auto leader=std::async(std::launch::async, [&] {
    EXPECT_THROW(flights.read_batch(ab, [&](const auto*) -> std::vector<Bytes> {
      producer.block(); throw std::runtime_error("whole batch invalid");
    }, checkpoint), std::runtime_error);
  });
  EXPECT_TRUE(producer.await(1));
  int loads=0;
  auto follower=std::async(std::launch::async, [&] {
    return flights.read_batch(ab, [&](const auto* indices) {
      ++loads; EXPECT_EQ(*indices, (std::vector<std::size_t>{0,1}));
      return block_values(ab, indices);
    }, attached_checkpoint(attached));
  });
  EXPECT_TRUE(attached.await(1));
  producer.open(); leader.get();
  EXPECT_EQ(follower.get(), block_values(ab, nullptr));
  EXPECT_EQ(loads, 1); EXPECT_EQ(flights.stats().fallbacks, 2u);
  EXPECT_EQ(flights.stats().retained_entries, 0u);
}

TEST(PayloadBlockFlights, BatchCapsIncludeScopeRetainedBytesAndBypassStaysOneBatch) {
  PayloadBlockFlights flights(2, 2);
  const BlockKeys ab{{"a", 1}, {"b", 1}}, cd{{"c", 1}, {"d", 1}};
  int calls=0;
  {
    PayloadBlockFlights::ReuseScope request;
    EXPECT_EQ(flights.read_batch(ab, [&](const auto* indices) {
      ++calls; return block_values(ab, indices);
    }, checkpoint), block_values(ab, nullptr));
    EXPECT_EQ(flights.stats().retained_entries, 2u);
    EXPECT_EQ(flights.stats().retained_bytes, 2u);
    EXPECT_EQ(flights.read_batch(cd, [&](const auto* indices) {
      ++calls; EXPECT_EQ(indices->size(), 2u); return block_values(cd, indices);
    }, checkpoint), block_values(cd, nullptr));
    EXPECT_EQ(flights.stats().retained_bytes, 2u);
    EXPECT_EQ(flights.stats().bypasses, 2u);
  }
  EXPECT_EQ(calls, 2); EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, ExplicitScalarScopeReusesAcrossBatchesAndNestedAdaptersThenReleasesPin) {
  PayloadBlockFlights flights;
  const BlockKeys ab{{"a", 1}, {"b", 1}}, bc{{"b", 1}, {"c", 1}};
  int physical=0;
  auto pin=std::make_shared<int>(42);
  std::weak_ptr<int> weak=pin;
  {
    PayloadBlockFlights::ReuseScope original_scalar;
    {
      PayloadBlockFlights::ReuseScope nested_adapter;
      flights.read_batch(ab, [&](const auto* indices) {
        physical += indices->size(); return block_values(ab, indices);
      }, checkpoint, pin);
    }
    pin.reset();
    EXPECT_FALSE(weak.expired());
    flights.read_batch(bc, [&](const auto* indices) {
      EXPECT_EQ(*indices, (std::vector<std::size_t>{1}));
      physical += indices->size(); return block_values(bc, indices);
    }, checkpoint);
    EXPECT_EQ(physical, 3); EXPECT_EQ(flights.stats().retained_bytes, 3u);
  }
  EXPECT_TRUE(weak.expired()); EXPECT_EQ(flights.stats().retained_bytes, 0u);
  flights.read_batch(ab, [&](const auto* indices) {
    physical += indices->size(); return block_values(ab, indices);
  }, checkpoint);
  EXPECT_EQ(physical, 5); EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, ScopedSingletonReuseKeepsExactIdentityAndSizeAndEndsAtRequestBoundary) {
  PayloadBlockFlights flights;
  int loads=0;
  auto read=[&](const std::string& key, std::size_t size) {
    return flights.read(key, size, [&] { ++loads; return Bytes(size, 7); }, checkpoint);
  };
  {
    PayloadBlockFlights::ReuseScope request;
    EXPECT_EQ(read("namespace/incarnation/type/protocol/id", 1).value(), Bytes{7});
    EXPECT_TRUE(read("namespace/incarnation/type/protocol/id", 1).joined);
    EXPECT_FALSE(read("other namespace/incarnation/type/protocol/id", 1).joined);
    EXPECT_FALSE(read("namespace/incarnation/type/protocol/id", 2).joined);
    EXPECT_EQ(loads, 3);
  }
  EXPECT_FALSE(read("namespace/incarnation/type/protocol/id", 1).joined);
  EXPECT_EQ(loads, 4); EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, BatchWaiterCancellationDoesNotCancelProducerOrKeepOwnPublishedBlocksAlive) {
  PayloadBlockFlights flights;
  Gate producer, waiting;
  std::atomic<bool> cancel{false};
  const BlockKeys ab{{"a", 1}, {"b", 1}};
  auto a=std::async(std::launch::async,[&] {
    return flights.read("a",1,[&] {producer.block();return Bytes{'a'};},checkpoint);
  });
  EXPECT_TRUE(producer.await(1));
  auto cancelled=std::async(std::launch::async,[&] {
    int checks=0;
    EXPECT_THROW(flights.read_batch(ab,[&](const auto* indices) {
      EXPECT_EQ(*indices,(std::vector<std::size_t>{1}));
      return block_values(ab,indices);
    },[&] {
      if(++checks==4)waiting.arrive();
      if(cancel)throw std::runtime_error("batch waiter cancelled");
    }),std::runtime_error);
  });
  EXPECT_TRUE(waiting.await(1));
  cancel=true;
  EXPECT_EQ(cancelled.wait_for(1s),std::future_status::ready);
  cancelled.get();
  EXPECT_EQ(flights.stats().active_flights,1u);
  EXPECT_EQ(flights.stats().retained_entries,1u);
  producer.open(); EXPECT_EQ(a.get().value(),Bytes{'a'});
  EXPECT_EQ(flights.stats().retained_bytes,0u);
}

TEST(PayloadBlockFlights, CancelledBatchProducerLeavesSurvivingFollowerItsOwnSingleFirstAttempt) {
  PayloadBlockFlights flights;
  Gate producer, attached;
  std::atomic<bool> cancel{false};
  const BlockKeys ab{{"a",1},{"b",1}};
  auto a=std::async(std::launch::async,[&] {
    EXPECT_THROW(flights.read_batch(ab,[&](const auto* indices) {
      producer.block();return block_values(ab,indices);
    },[&] {if(cancel)throw std::runtime_error("batch producer cancelled");}),std::runtime_error);
  });
  EXPECT_TRUE(producer.await(1));
  int loads=0;
  auto b=std::async(std::launch::async,[&] {
    return flights.read_batch(ab,[&](const auto* indices) {
      ++loads;EXPECT_EQ(*indices,(std::vector<std::size_t>{0,1}));
      return block_values(ab,indices);
    },attached_checkpoint(attached));
  });
  EXPECT_TRUE(attached.await(1));cancel=true;producer.open();a.get();
  EXPECT_EQ(b.get(),block_values(ab,nullptr));EXPECT_EQ(loads,1);
  EXPECT_EQ(flights.stats().fallbacks,2u);
  EXPECT_EQ(flights.stats().retained_bytes,0u);
}

TEST(PayloadBlockFlights, BudgetReclaimDropsOptionalRootRetentionAcrossNestedScopesAndRestoresPreviousHook) {
  PayloadBlockFlights flights;
  int previous_calls=0,loads=0;
  iotdbfs::ScopedStorageReadOptionalReclaim previous([](void* pointer) noexcept {
    ++*static_cast<int*>(pointer);
  },&previous_calls);
  {
    PayloadBlockFlights::ReuseScope original_request;
    flights.read("a",1,[&] {++loads;return Bytes{'a'};},checkpoint);
    EXPECT_EQ(flights.stats().retained_bytes,1u);
    {
      PayloadBlockFlights::ReuseScope nested_adapter;
      iotdbfs::reclaim_storage_read_optional();
      EXPECT_EQ(flights.stats().retained_bytes,0u);
    }
    flights.read("a",1,[&] {++loads;return Bytes{'a'};},checkpoint);
    EXPECT_EQ(flights.stats().retained_bytes,1u);
    iotdbfs::reclaim_storage_read_optional();
    EXPECT_EQ(flights.stats().retained_bytes,0u);
  }
  EXPECT_EQ(loads,2);EXPECT_EQ(previous_calls,0);
  iotdbfs::reclaim_storage_read_optional();EXPECT_EQ(previous_calls,1);
}

TEST(PayloadBlockFlights, ResidentCacheReusesValidatedBytesAcrossIndependentRequests) {
  PayloadBlockFlights flights(4, 4, 4);
  unsigned loads = 0;
  auto read = [&] {
    return flights.read("ns/inc/payload/protocol/digest", 2,
      [&] { ++loads; return Bytes{7, 9}; }, checkpoint);
  };
  {
    PayloadBlockFlights::ReuseScope first_request;
    EXPECT_EQ(read().value(), (Bytes{7, 9}));
  }
  EXPECT_EQ(flights.stats().cache_resident_entries, 1u);
  EXPECT_EQ(flights.stats().cache_resident_bytes, 2u);
  {
    PayloadBlockFlights::ReuseScope second_request;
    auto result = read();
    EXPECT_EQ(result.value(), (Bytes{7, 9}));
    EXPECT_FALSE(result.bypassed);
  }
  EXPECT_EQ(loads, 1u);
  EXPECT_EQ(flights.stats().cache_hits, 1u);
  EXPECT_EQ(flights.stats().cache_insertions, 1u);
  EXPECT_EQ(flights.stats().active_flights, 0u);
}

TEST(PayloadBlockFlights, ResidentLruTouchProtectsRecentBlockWithinBothByteBounds) {
  PayloadBlockFlights flights(8, 4, 4);
  unsigned loads = 0;
  auto read = [&](const std::string& key) {
    auto result = flights.read(key, 2, [&] {
      ++loads; return Bytes(2, static_cast<std::uint8_t>(key.front()));
    }, checkpoint);
    EXPECT_EQ(result.value(), Bytes(2, static_cast<std::uint8_t>(key.front())));
    EXPECT_LE(flights.stats().retained_bytes, 4u);
    EXPECT_LE(flights.stats().cache_resident_bytes, 4u);
  };
  read("a"); read("b"); read("a"); read("c");
  EXPECT_EQ(loads, 3u);
  read("a");
  EXPECT_EQ(loads, 3u); // Touching a made b the victim, not a.
  read("b");
  EXPECT_EQ(loads, 4u);
  EXPECT_GE(flights.stats().cache_evictions, 2u);
  EXPECT_EQ(flights.stats().cache_resident_entries, 2u);
  EXPECT_EQ(flights.stats().cache_resident_bytes, 4u);
}

TEST(PayloadBlockFlights, ResidentEntryCapEvictsIdleEntryBeforeBypassingNewBlock) {
  PayloadBlockFlights flights(1, 8, 8);
  unsigned loads = 0;
  auto read = [&](const std::string& key) {
    return flights.read(key, 1, [&] { ++loads; return Bytes{1}; }, checkpoint);
  };
  read("a"); read("b"); read("b");
  EXPECT_EQ(loads, 2u);
  EXPECT_EQ(flights.stats().retained_entries, 1u);
  EXPECT_EQ(flights.stats().cache_resident_entries, 1u);
  EXPECT_EQ(flights.stats().cache_evictions, 1u);
  EXPECT_EQ(flights.stats().bypasses, 0u);
}

TEST(PayloadBlockFlights, ResidentClearDropsOnlyOptionalOwnershipUntilLiveConsumerReleases) {
  PayloadBlockFlights flights(2, 4, 4);
  auto pin = std::make_shared<int>(0);
  std::weak_ptr<int> weak = pin;
  auto held = flights.read("a", 2, [] { return Bytes{1, 2}; }, checkpoint, pin);
  pin.reset();
  flights.clear();
  EXPECT_EQ(flights.stats().cache_resident_entries, 0u);
  EXPECT_EQ(flights.stats().cache_resident_bytes, 0u);
  EXPECT_EQ(flights.stats().retained_bytes, 2u);
  EXPECT_FALSE(weak.expired());
  EXPECT_EQ(held.value(), (Bytes{1, 2}));
  held = {};
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(flights.stats().retained_entries, 0u);
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
  flights.clear(); // Repeated clearing must not double-release accounting.
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
}

TEST(PayloadBlockFlights, ResidentDestructionBreaksOwnerCycleAndPreservesLiveAlias) {
  std::weak_ptr<int> idle_pin, held_pin;
  PayloadBlockFlights::Result held;
  {
    PayloadBlockFlights flights(4, 4, 4);
    auto idle = std::make_shared<int>(1), live = std::make_shared<int>(2);
    idle_pin = idle; held_pin = live;
    flights.read("idle", 1, [] { return Bytes{1}; }, checkpoint, idle);
    held = flights.read("live", 1, [] { return Bytes{2}; }, checkpoint, live);
    idle.reset(); live.reset();
    EXPECT_FALSE(idle_pin.expired());
    EXPECT_FALSE(held_pin.expired());
  }
  EXPECT_TRUE(idle_pin.expired());
  EXPECT_FALSE(held_pin.expired());
  EXPECT_EQ(held.value(), Bytes{2});
  held = {};
  EXPECT_TRUE(held_pin.expired());
}

TEST(PayloadBlockFlights, UnexpectedLengthAndExcessCapacityNeverBecomeResident) {
  for (bool excess_capacity : {false, true}) {
    SCOPED_TRACE(excess_capacity);
    PayloadBlockFlights flights(2, 4, 4);
    unsigned loads = 0;
    auto read = [&] {
      return flights.read("a", 1, [&] {
        ++loads;
        Bytes result(excess_capacity ? 1 : 2, 7);
        if (excess_capacity) result.reserve(1024);
        return result;
      }, checkpoint);
    };
    EXPECT_EQ(read().value(), Bytes(excess_capacity ? 1 : 2, 7));
    EXPECT_EQ(flights.stats().cache_resident_entries, 0u);
    EXPECT_EQ(read().value(), Bytes(excess_capacity ? 1 : 2, 7));
    EXPECT_EQ(loads, 2u);
    EXPECT_EQ(flights.stats().cache_insertions, 0u);
    EXPECT_EQ(flights.stats().retained_bytes, 0u);
  }
}

TEST(PayloadBlockFlights, ResidentHitsStillCheckpointAndFailedProducerDoesNotCacheError) {
  PayloadBlockFlights flights(2, 4, 4);
  unsigned loads = 0;
  EXPECT_THROW(flights.read("a", 1, [&]() -> Bytes {
    ++loads; throw std::runtime_error("physical read failed");
  }, checkpoint), std::runtime_error);
  EXPECT_EQ(flights.stats().cache_resident_entries, 0u);
  flights.read("a", 1, [&] { ++loads; return Bytes{7}; }, checkpoint);
  EXPECT_THROW(flights.read("a", 1, [&] { ++loads; return Bytes{8}; },
    [] { throw std::runtime_error("this request cancelled"); }), std::runtime_error);
  EXPECT_EQ(loads, 2u);
  EXPECT_EQ(flights.read("a", 1, [&] { ++loads; return Bytes{9}; }, checkpoint).value(), Bytes{7});
  EXPECT_EQ(loads, 2u);
}

TEST(PayloadBlockFlights, ResidentBatchHitsPreserveMissingSubsetBatchAndCallerOrder) {
  PayloadBlockFlights flights(4, 4, 4);
  const BlockKeys ab{{"a", 1}, {"b", 1}}, bc{{"b", 1}, {"c", 1}};
  std::vector<std::vector<std::size_t>> physical;
  auto read = [&](const BlockKeys& keys) {
    return flights.read_batch(keys, [&](const auto* indices) {
      EXPECT_NE(indices, nullptr);
      if (indices) physical.push_back(*indices);
      return block_values(keys, indices);
    }, checkpoint);
  };
  EXPECT_EQ(read(ab), block_values(ab, nullptr));
  EXPECT_EQ(read(bc), block_values(bc, nullptr));
  EXPECT_EQ(physical, (std::vector<std::vector<std::size_t>>{{0, 1}, {1}}));
  EXPECT_EQ(flights.stats().cache_resident_entries, 3u);
  EXPECT_EQ(flights.stats().cache_resident_bytes, 3u);
  EXPECT_EQ(flights.stats().active_flights, 0u);
}

TEST(PayloadBlockFlights, ResidentEvictionNeverRefundsBytesStillHeldByConsumer) {
  PayloadBlockFlights flights(1, 1, 1);
  auto held = flights.read("a", 1, [] { return Bytes{'a'}; }, checkpoint);
  auto next = flights.read("b", 1, [] { return Bytes{'b'}; }, checkpoint);
  EXPECT_EQ(held.value(), Bytes{'a'});
  EXPECT_EQ(next.value(), Bytes{'b'});
  EXPECT_TRUE(next.bypassed);
  EXPECT_EQ(flights.stats().retained_entries, 1u);
  EXPECT_EQ(flights.stats().retained_bytes, 1u);
  EXPECT_LE(flights.stats().cache_resident_bytes, 1u);
  flights.clear();
  held = {};
  EXPECT_EQ(flights.stats().retained_bytes, 0u);
}
