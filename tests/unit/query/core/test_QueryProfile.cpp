#include "iotdbfs/query/core/QueryProfile.h"

#include <gtest/gtest.h>

#include <thread>

namespace {
using namespace iotdbfs;

TEST(QueryProfileTest, RestoresNestedScopesAndSupportsDisabledProfiling) {
  QueryProfileRecord outer;
  QueryProfileRecord inner;
  auto* previous = active_query_profile;
  {
    ScopedQueryProfile scope(&outer);
    { QueryProfileTimer timer("outer_before"); }
    {
      ScopedQueryProfile nested(&inner);
      QueryProfileTimer timer("inner");
    }
    {
      ScopedQueryProfile disabled(nullptr);
      QueryProfileTimer timer("disabled");
    }
    { QueryProfileTimer timer("outer_after"); }
    EXPECT_EQ(active_query_profile, &outer);
  }
  EXPECT_EQ(active_query_profile, previous);
  EXPECT_EQ(outer.nanoseconds.size(), 2U);
  EXPECT_EQ(outer.nanoseconds.count("outer_before"), 1U);
  EXPECT_EQ(outer.nanoseconds.count("outer_after"), 1U);
  EXPECT_EQ(inner.nanoseconds.size(), 1U);
  EXPECT_EQ(inner.nanoseconds.count("inner"), 1U);
}

TEST(QueryProfileTest, RecordsOnReturnAndAccumulatesRepeatedStages) {
  QueryProfileRecord record;
  ScopedQueryProfile scope(&record);
  const auto run = [] {
    QueryProfileTimer timer("stage");
    return 42;
  };
  EXPECT_EQ(run(), 42);
  ASSERT_EQ(record.nanoseconds.count("stage"), 1U);
  // A sentinel verifies the next timer adds rather than overwrites, without
  // depending on clock resolution or any minimum elapsed duration.
  const auto first = record.nanoseconds.at("stage") + 1000000000000ULL;
  record.nanoseconds.at("stage") = first;
  EXPECT_EQ(run(), 42);
  EXPECT_GE(record.nanoseconds.at("stage"), first);
}

TEST(QueryProfileTest, DoesNotPropagateProfileToAnotherThread) {
  QueryProfileRecord parent;
  QueryProfileRecord worker;
  ScopedQueryProfile scope(&parent);
  bool starts_disabled = false;
  bool restored_disabled = false;
  std::thread thread([&] {
    starts_disabled = active_query_profile == nullptr;
    { QueryProfileTimer timer("unscoped_worker"); }
    {
      ScopedQueryProfile worker_scope(&worker);
      QueryProfileTimer timer("worker");
    }
    restored_disabled = active_query_profile == nullptr;
  });
  thread.join();
  { QueryProfileTimer timer("parent"); }
  EXPECT_TRUE(starts_disabled);
  EXPECT_TRUE(restored_disabled);
  EXPECT_EQ(parent.nanoseconds.size(), 1U);
  EXPECT_EQ(parent.nanoseconds.count("parent"), 1U);
  EXPECT_EQ(worker.nanoseconds.size(), 1U);
  EXPECT_EQ(worker.nanoseconds.count("worker"), 1U);
}

TEST(QueryProfileTest, CountersAreOptionalScopedAndSaturating) {
  query_profile_count("ignored", 5);
  QueryProfileRecord outer, inner;
  ScopedQueryProfile scope(&outer);
  query_profile_count("bytes", 3);
  query_profile_count("bytes", 4);
  { ScopedQueryProfile nested(&inner); query_profile_count("bytes", 9); }
  { ScopedQueryProfile disabled(nullptr); query_profile_count("bytes", 9); }
  EXPECT_EQ(outer.counters.at("bytes"), 7U);
  EXPECT_EQ(inner.counters.at("bytes"), 9U);
  query_profile_count("bytes", std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(outer.counters.at("bytes"), std::numeric_limits<std::uint64_t>::max());
}

} // namespace
