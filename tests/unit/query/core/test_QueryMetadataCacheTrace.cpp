#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include <gtest/gtest.h>
#include <future>
#include <memory>
#include <random>
#include <stdexcept>
#include <vector>

using namespace iotdbfs;

TEST(QueryMetadataCacheTrace, StableReplayKeepsIdentityMaximumAndRequestIsolation) {
  QueryMetadataTrace captured;
  {
    ScopedQueryMetadataBudget request;
    ScopedQueryMetadataTrace trace("",{},QueryMetadataTracePolicy::CacheStable);
    require_query_metadata("artifact:source@1",11);
    require_query_metadata("artifact:source@1",7);
    require_query_metadata("artifact:source@1",17);
    require_query_metadata("artifact:source@2",19);
    EXPECT_FALSE(trace.failed());
    EXPECT_EQ(request.budget()->consumed_bytes(),36);
    captured=trace.release();
  }
  for(int n=0;n<2;++n) {
    ScopedQueryMetadataBudget request;
    require_query_metadata("artifact:source@1",9);
    replay_query_metadata_trace(captured);
    replay_query_metadata_trace(captured);
    EXPECT_EQ(request.budget()->consumed_bytes(),36);
  }
}

TEST(QueryMetadataCacheTrace, ReservationFailureOptsOutWithoutPoisoningRequestOrOuterTrace) {
  ScopedQueryMetadataBudget request;
  ScopedQueryMetadataTrace outer("");
  {
    ScopedQueryMetadataTrace trace("",[](std::uint64_t){throw std::runtime_error("cache is full");},
      QueryMetadataTracePolicy::CacheStable);
    EXPECT_NO_THROW(require_query_metadata("metadata:first",13));
    EXPECT_NO_THROW(require_query_metadata("metadata:second",17));
    EXPECT_TRUE(trace.failed());
    EXPECT_TRUE(trace.release().empty());
  }
  EXPECT_TRUE(request.status().ok());
  EXPECT_EQ(request.budget()->consumed_bytes(),30);
  const auto fees=outer.release();
  EXPECT_EQ(fees.at("metadata:first"),13);
  EXPECT_EQ(fees.at("metadata:second"),17);
}

TEST(QueryMetadataCacheTrace, ActualRequestExhaustionStillThrowsAndStaysSticky) {
  ScopedQueryMetadataBudget request;
  ScopedQueryMetadataTrace trace("",{},QueryMetadataTracePolicy::CacheStable);
  require_query_metadata("full",QueryMetadataBudget::maximum_bytes);
  EXPECT_THROW(require_query_metadata("overflow",1),QueryMetadataBudgetExceeded);
  EXPECT_FALSE(request.status().ok());
  EXPECT_THROW(require_query_metadata("full",0),QueryMetadataBudgetExceeded);
}

TEST(QueryMetadataCacheTrace, OperationSpecificFeesRejectCacheButKeepOriginalCharges) {
  for(const auto* key:{"unique-demand/1","v04-payload-bytes/1","v04-extents/1"}) {
    ScopedQueryMetadataBudget request;
    ScopedQueryMetadataTrace trace("",{},QueryMetadataTracePolicy::CacheStable);
    require_query_metadata("stable",5);
    EXPECT_NO_THROW(require_query_metadata(key,7));
    EXPECT_TRUE(trace.failed());
    EXPECT_TRUE(trace.release().empty());
    EXPECT_TRUE(request.status().ok());
    EXPECT_EQ(request.budget()->consumed_bytes(),std::string(key)=="unique-demand/1"?12:5);
  }
}

TEST(QueryMetadataCacheTrace, OptionalTraceLimitDoesNotChangeSuccessfulQuery) {
  ScopedQueryMetadataBudget request;
  ScopedQueryMetadataTrace trace("",{},QueryMetadataTracePolicy::CacheStable);
  for(std::size_t i=0;i<65537;++i)
    require_query_metadata("small:"+std::to_string(i),1);
  EXPECT_TRUE(trace.failed());
  EXPECT_TRUE(trace.release().empty());
  EXPECT_TRUE(request.status().ok());
  EXPECT_EQ(request.budget()->consumed_bytes(),65537);
}

TEST(QueryMetadataCacheTrace, RequiredTraceReservationFailureStillPropagates) {
  ScopedQueryMetadataBudget request;
  ScopedQueryMetadataTrace trace("",[](std::uint64_t){throw std::runtime_error("required trace failure");});
  EXPECT_THROW(require_query_metadata("metadata:first",13),std::runtime_error);
  EXPECT_TRUE(trace.failed());
  EXPECT_THROW(require_query_metadata("metadata:second",17),QueryMetadataBudgetExceeded);
}

TEST(QueryMetadataCacheTrace, PreparedBatchesOverlapDirectEvidenceAndEachOtherByIdentityMaximum) {
  const auto first=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
    {"ns/source@1/layout",17},{"ns/source@2/layout",19}});
  const auto overlapping=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
    {"ns/source@1/layout",23},{"ns/other@1/layout",29}});
  ScopedQueryMetadataBudget request;
  require_query_metadata("ns/source@1/layout",9);
  replay_query_metadata_trace(first);
  EXPECT_EQ(request.budget()->consumed_bytes(),36u);
  replay_query_metadata_trace(overlapping);
  EXPECT_EQ(request.budget()->consumed_bytes(),71u);
  require_query_metadata("ns/source@1/layout",31);
  replay_query_metadata_trace(first);
  replay_query_metadata_trace(overlapping);
  EXPECT_EQ(request.budget()->consumed_bytes(),79u);
  EXPECT_TRUE(request.status().ok());
}

TEST(QueryMetadataCacheTrace, PreparedMemoStillFeedsCollectorsCreatedAfterItsFirstReplay) {
  const QueryMetadataTrace fees{{"directory/page",11},{"artifact:source@1",17}};
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(fees);
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  {
    ScopedQueryMetadataTrace outer("");
    {
      ScopedQueryMetadataTrace inner("directory/");
      replay_query_metadata_trace(prepared);
      EXPECT_EQ(inner.release(),(QueryMetadataTrace{{"directory/page",11}}));
    }
    EXPECT_EQ(outer.release(),fees);
  }
  EXPECT_EQ(request.budget()->consumed_bytes(),28u);
}

TEST(QueryMetadataCacheTrace, PreparedReplayWithRequiredCollectorKeepsFirstFailureAndChargePrefix) {
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
    {"a:first",7},{"b:second",11},{"c:third",13}});
  ScopedQueryMetadataBudget request;
  ScopedQueryMetadataTrace outer("");
  {
    ScopedQueryMetadataTrace inner("",[](std::uint64_t){throw std::runtime_error("required stop");});
    try {
      replay_query_metadata_trace(prepared);
      FAIL()<<"required collector failure was hidden";
    } catch(const std::runtime_error& error) {
      EXPECT_STREQ(error.what(),"required stop");
    }
    EXPECT_TRUE(inner.failed());
    EXPECT_TRUE(inner.release().empty());
    EXPECT_EQ(request.budget()->consumed_bytes(),7u);
    EXPECT_THROW(replay_query_metadata_trace(prepared),QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.budget()->consumed_bytes(),7u);
  }
  // The ledger is healthy: the collector, rather than the request limit,
  // failed. Later entries in the aborted replay must not be precharged.
  EXPECT_TRUE(request.status().ok());
  require_query_metadata("b:second",11);
  EXPECT_EQ(request.budget()->consumed_bytes(),18u);
  EXPECT_EQ(outer.release(),(QueryMetadataTrace{{"b:second",11}}));
}

TEST(QueryMetadataCacheTrace, PreparedMemoCannotBypassFailedRequiredCollector) {
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(
    QueryMetadataTrace{{"directory/page",13}});
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  ScopedQueryMetadataTrace collector("unrelated/",[](std::uint64_t){throw std::runtime_error("stop");});
  EXPECT_THROW(require_query_metadata("unrelated/first",1),std::runtime_error);
  // A failed Required collector rejects even subsequent nonmatching keys.
  EXPECT_THROW(replay_query_metadata_trace(prepared),QueryMetadataBudgetExceeded);
  EXPECT_EQ(request.budget()->consumed_bytes(),14u);
}

TEST(QueryMetadataCacheTrace, PreparedReplayOptionalOptOutStillReachesOuterCollector) {
  const QueryMetadataTrace fees{{"metadata:first",13},{"metadata:second",17}};
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(fees);
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  ScopedQueryMetadataTrace outer("");
  {
    ScopedQueryMetadataTrace optional("",[](std::uint64_t){throw std::runtime_error("cache full");},
      QueryMetadataTracePolicy::CacheStable);
    EXPECT_NO_THROW(replay_query_metadata_trace(prepared));
    EXPECT_TRUE(optional.failed());
    EXPECT_TRUE(optional.release().empty());
  }
  EXPECT_EQ(outer.release(),fees);
  EXPECT_EQ(request.budget()->consumed_bytes(),30u);
  EXPECT_TRUE(request.status().ok());
}

TEST(QueryMetadataCacheTrace, PreparedReplayPreservesReservedDemandsAndCollectorPrefixPolicy) {
  const QueryMetadataTrace fees{{"metadata:first",5},{"v04-extents/prepared-test",7},
    {"v04-payload-bytes/prepared-test",11}};
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(fees);
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  {
    ScopedQueryMetadataTrace outer("");
    {
      ScopedQueryMetadataTrace metadata_only("metadata:",{},QueryMetadataTracePolicy::CacheStable);
      replay_query_metadata_trace(prepared);
      EXPECT_FALSE(metadata_only.failed());
      EXPECT_EQ(metadata_only.release(),(QueryMetadataTrace{{"metadata:first",5}}));
    }
    EXPECT_EQ(outer.release(),fees);
  }
  {
    ScopedQueryMetadataTrace all("",{},QueryMetadataTracePolicy::CacheStable);
    EXPECT_NO_THROW(replay_query_metadata_trace(prepared));
    EXPECT_TRUE(all.failed());
    EXPECT_TRUE(all.release().empty());
  }
  EXPECT_EQ(request.budget()->consumed_bytes(),5u);
  EXPECT_TRUE(request.status().ok());
}

TEST(QueryMetadataCacheTrace, PreparedFailureKeepsEachBudgetDomainStickyAndEarlierFeesCharged) {
  const auto already=std::make_shared<const PreparedQueryMetadataTrace>(
    QueryMetadataTrace{{"already",1}});
  for(const std::string domain:{"metadata","payload","extent"}) {
    SCOPED_TRACE(domain);
    ScopedQueryMetadataBudget request;
    replay_query_metadata_trace(already);
    std::string failing_key,message;
    std::uint64_t expected_metadata=3;
    if(domain=="metadata") {
      request.budget()->observe_unique(QueryMetadataBudget::maximum_bytes-4);
      failing_key="b:metadata";
      message="request metadata byte budget exhausted";
      expected_metadata=QueryMetadataBudget::maximum_bytes-1;
    } else if(domain=="payload") {
      require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes-1,0);
      failing_key="v04-payload-bytes/failing";
      message="request payload byte budget exhausted";
    } else {
      require_query_payload_demand(0,QueryMetadataBudget::maximum_extents-1);
      failing_key="v04-extents/failing";
      message="request extent budget exhausted";
    }
    const auto overflowing=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
      {"a:prefix",2},{failing_key,2},{"z:unreached",19}});
    EXPECT_THROW(replay_query_metadata_trace(overflowing),QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.budget()->consumed_bytes(),expected_metadata);
    EXPECT_EQ(request.status().message(),message);
    EXPECT_THROW(replay_query_metadata_trace(already),QueryMetadataBudgetExceeded);
    EXPECT_THROW(require_query_metadata("already",0),QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status(QueryPhase::Execution).message(),message);
    EXPECT_EQ(request.status(QueryPhase::Execution).phase(),QueryPhase::Execution);
  }
}

TEST(QueryMetadataCacheTrace, PreparedReplayChoosesFirstFailingDomainInTraceOrder) {
  ScopedQueryMetadataBudget request;
  request.budget()->observe_unique(QueryMetadataBudget::maximum_bytes-1);
  require_query_payload_demand(QueryMetadataBudget::maximum_payload_bytes-1,
    QueryMetadataBudget::maximum_extents-1);
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
    {"a:metadata",1},{"v04-extents/overflow",2},{"v04-payload-bytes/overflow",2}});
  EXPECT_THROW(replay_query_metadata_trace(prepared),QueryMetadataBudgetExceeded);
  EXPECT_EQ(request.budget()->consumed_bytes(),QueryMetadataBudget::maximum_bytes);
  EXPECT_EQ(request.status().message(),"request extent budget exhausted");
  EXPECT_THROW(request.budget()->observe_unique(1),QueryMetadataBudgetExceeded);
  EXPECT_EQ(request.status().message(),"request extent budget exhausted");
}

TEST(QueryMetadataCacheTrace, PreparedBatchPinsAreBoundedAndOverflowFallsBackWithoutExemption) {
  std::vector<std::weak_ptr<int>> retained;
  std::weak_ptr<int> overflow_pin;
  {
    ScopedQueryMetadataBudget request;
    for(std::size_t i=0;i<QueryMetadataBudget::maximum_replayed_batches;++i) {
      auto pin=std::make_shared<int>(static_cast<int>(i));
      retained.push_back(pin);
      auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(
        QueryMetadataTrace{{"source:"+std::to_string(i),1}},pin);
      replay_query_metadata_trace(prepared);
      prepared.reset();pin.reset();
      EXPECT_FALSE(retained.back().expired());
    }
    {
      auto pin=std::make_shared<int>(-1);
      overflow_pin=pin;
      const auto overflow=std::make_shared<const PreparedQueryMetadataTrace>(
        QueryMetadataTrace{{"overflow-source",3}},pin);
      replay_query_metadata_trace(overflow);
      replay_query_metadata_trace(overflow);
    }
    EXPECT_TRUE(overflow_pin.expired());
    const auto larger=std::make_shared<const PreparedQueryMetadataTrace>(
      QueryMetadataTrace{{"overflow-source",5}});
    replay_query_metadata_trace(larger);
    EXPECT_EQ(request.budget()->consumed_bytes(),QueryMetadataBudget::maximum_replayed_batches+5);
    EXPECT_TRUE(request.status().ok());
    for(const auto& pin:retained) EXPECT_FALSE(pin.expired());
  }
  for(const auto& pin:retained) EXPECT_TRUE(pin.expired());
}

TEST(QueryMetadataCacheTrace, PreparedBatchMemoBelongsToEachLedgerAndNestedScopeRestoresIt) {
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(
    QueryMetadataTrace{{"source@1",11}});
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  {
    const auto independent=std::make_shared<QueryMetadataBudget>();
    ScopedQueryMetadataBudget other(independent);
    require_query_metadata("other",5);
    replay_query_metadata_trace(prepared);
    replay_query_metadata_trace(prepared);
    EXPECT_EQ(independent->consumed_bytes(),16u);
    EXPECT_EQ(request.budget()->consumed_bytes(),11u);
  }
  EXPECT_EQ(current_query_metadata_budget(),request.budget());
  require_query_metadata("source@1",19);
  replay_query_metadata_trace(prepared);
  EXPECT_EQ(request.budget()->consumed_bytes(),19u);
}

TEST(QueryMetadataCacheTrace, PreparedMemoStillChargesTraceCapturedWithoutARequestLedger) {
  const QueryMetadataTrace fees{{"metadata:source@1",17},{"metadata:source@2",19}};
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(fees);
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(prepared);
  {
    ScopedQueryMetadataProducer producer;
    EXPECT_FALSE(current_query_metadata_budget());
    ScopedQueryMetadataTrace captured("metadata:");
    replay_query_metadata_trace(prepared);
    EXPECT_EQ(captured.release(),fees);
  }
  EXPECT_EQ(current_query_metadata_budget(),request.budget());
  EXPECT_EQ(request.budget()->consumed_bytes(),36u);
}

TEST(QueryMetadataCacheTrace, ConcurrentSharedLedgerStillPopulatesEveryWaitersCollector) {
  const QueryMetadataTrace fees{{"metadata:source@1",17},{"metadata:source@2",19}};
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(fees);
  const auto shared=std::make_shared<QueryMetadataBudget>();
  {
    ScopedQueryMetadataBudget request(shared);
    replay_query_metadata_trace(prepared);
  }
  std::promise<void> start;
  const auto ready=start.get_future().share();
  std::vector<std::future<QueryMetadataTrace>> workers;
  for(int i=0;i<8;++i) workers.push_back(std::async(std::launch::async,[shared,prepared,ready] {
    ready.wait();
    ScopedQueryMetadataBudget request(shared);
    ScopedQueryMetadataTrace collector("metadata:");
    replay_query_metadata_trace(prepared);
    return collector.release();
  }));
  start.set_value();
  for(auto& worker:workers) EXPECT_EQ(worker.get(),fees);
  EXPECT_EQ(shared->consumed_bytes(),36u);
  EXPECT_TRUE(shared->status().ok());
}

TEST(QueryMetadataCacheTrace, ConcurrentPreparedFastReplayAndDirectDemandShareOneMaximum) {
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{
    {"metadata:source@1",17},{"metadata:source@2",19}});
  const auto shared=std::make_shared<QueryMetadataBudget>();
  std::promise<void> start;
  const auto ready=start.get_future().share();
  std::vector<std::future<void>> workers;
  for(int i=0;i<8;++i) workers.push_back(std::async(std::launch::async,[shared,prepared,ready,i] {
    ready.wait();
    ScopedQueryMetadataBudget request(shared);
    replay_query_metadata_trace(prepared);
    require_query_metadata("metadata:source@1",25);
    require_query_metadata("waiter:"+std::to_string(i),3);
    replay_query_metadata_trace(prepared);
  }));
  start.set_value();
  for(auto& worker:workers) EXPECT_NO_THROW(worker.get());
  EXPECT_EQ(shared->consumed_bytes(),25u+19u+8u*3u);
  EXPECT_TRUE(shared->status().ok());
}

TEST(QueryMetadataCacheTrace, PreparedEmptyMemoStillChecksStickyLedgerWithOrWithoutCollector) {
  const auto empty=std::make_shared<const PreparedQueryMetadataTrace>(QueryMetadataTrace{});
  ScopedQueryMetadataBudget request;
  replay_query_metadata_trace(empty);
  EXPECT_THROW(request.budget()->observe_unique(QueryMetadataBudget::maximum_bytes+1),
    QueryMetadataBudgetExceeded);
  EXPECT_THROW(replay_query_metadata_trace(empty),QueryMetadataBudgetExceeded);
  ScopedQueryMetadataTrace collector("");
  EXPECT_THROW(replay_query_metadata_trace(empty),QueryMetadataBudgetExceeded);
  EXPECT_TRUE(collector.release().empty());
  EXPECT_EQ(request.budget()->consumed_bytes(),0u);
}

TEST(QueryMetadataCacheTrace, OneConcurrentWaitersBudgetFailureDoesNotPoisonPreparedBatch) {
  const auto prepared=std::make_shared<const PreparedQueryMetadataTrace>(
    QueryMetadataTrace{{"metadata:source",11}});
  const auto exhausted=std::make_shared<QueryMetadataBudget>();
  exhausted->observe_unique(QueryMetadataBudget::maximum_bytes-5);
  const auto healthy=std::make_shared<QueryMetadataBudget>();
  std::promise<void> start;
  const auto ready=start.get_future().share();
  auto replay=[prepared,ready](const std::shared_ptr<QueryMetadataBudget>& ledger) {
    ready.wait();
    ScopedQueryMetadataBudget request(ledger);
    try {replay_query_metadata_trace(prepared);return true;}
    catch(const QueryMetadataBudgetExceeded&){return false;}
  };
  auto first=std::async(std::launch::async,replay,exhausted);
  auto second=std::async(std::launch::async,replay,healthy);
  start.set_value();
  EXPECT_FALSE(first.get());EXPECT_TRUE(second.get());
  EXPECT_EQ(exhausted->consumed_bytes(),QueryMetadataBudget::maximum_bytes-5);
  EXPECT_EQ(healthy->consumed_bytes(),11u);
  ScopedQueryMetadataBudget request;
  EXPECT_NO_THROW(replay_query_metadata_trace(prepared));
  EXPECT_EQ(request.budget()->consumed_bytes(),11u);
}

namespace {
using StageBatch=std::shared_ptr<const PreparedQueryMetadataTrace>;
using StagePlan=std::shared_ptr<const PreparedQueryMetadataStageSummary>;
StageBatch stage_batch(QueryMetadataTrace fees,std::shared_ptr<const void> pin={}) {
  return std::make_shared<const PreparedQueryMetadataTrace>(std::move(fees),std::move(pin));
}
struct SummaryQuota {
  std::uint64_t used{0},peak{0},limit{QueryMetadataBudget::maximum_bytes};
  unsigned allocations{0},fail_at{0};
  void reserve(std::uint64_t amount) {
    ++allocations;
    if((fail_at&&allocations==fail_at)||amount>limit-used)
      throw std::runtime_error("optional summary quota exhausted");
    used+=amount;peak=std::max(peak,used);
  }
  void release(std::uint64_t amount) {EXPECT_LE(amount,used);used-=amount;}
};
struct SummaryReservation {
  explicit SummaryReservation(std::shared_ptr<SummaryQuota> q):quota(std::move(q)){}
  ~SummaryReservation(){if(owned)quota->release(owned);}
  void reserve(std::uint64_t n){quota->reserve(n);owned+=n;}
  void release(std::uint64_t n){quota->release(n);owned-=n;}
  std::shared_ptr<SummaryQuota> quota;
  std::uint64_t owned{0};
};
StagePlan stage_plan(const std::vector<StageBatch>& batches,
    const std::shared_ptr<SummaryQuota>& quota=std::make_shared<SummaryQuota>()) {
  const auto reservation=std::make_shared<SummaryReservation>(quota);
  return PreparedQueryMetadataStageSummary::compile(batches,
    [reservation](std::uint64_t n){reservation->reserve(n);},
    [reservation](std::uint64_t n){reservation->release(n);},reservation);
}
template<class Operation> std::string summary_error(Operation&& operation) {
  try {operation();return {};}
  catch(const std::exception& error){return error.what();}
}
void compare_summary_status(const std::shared_ptr<QueryMetadataBudget>& reference,
    const std::shared_ptr<QueryMetadataBudget>& candidate) {
  EXPECT_EQ(reference->consumed_bytes(),candidate->consumed_bytes());
  for(auto phase:{QueryPhase::Catalog,QueryPhase::Execution,QueryPhase::Encoding}) {
    const auto a=reference->status(phase),b=candidate->status(phase);
    EXPECT_EQ(a.ok(),b.ok());EXPECT_EQ(a.code(),b.code());
    EXPECT_EQ(a.phase(),b.phase());EXPECT_EQ(a.message(),b.message());
  }
}
}

TEST(QueryMetadataStageSummary, KnownStagesBorrowBatchesAndCorrectInterleavedMaximums) {
  const auto first=stage_batch({{"head",140},{"shared",10},
    {"v04-extents/x",3},{"v04-payload-bytes/x",7}});
  const auto second=stage_batch({{"other",5},{"shared",20},
    {"v04-extents/x",5},{"v04-payload-bytes/x",11}});
  const auto covered=stage_batch({{"other",5},{"shared",12}});
  const auto plan=stage_plan({first,second,covered,second});ASSERT_TRUE(plan);
  ScopedQueryMetadataBudget request;
  require_query_metadata("head",140);require_query_metadata("shared",30);
  require_query_metadata("v04-extents/x",7);
  require_query_metadata("v04-payload-bytes/x",19);
  ASSERT_TRUE(arm_query_metadata_stage_summary(plan));
  replay_query_metadata_trace(first);replay_query_metadata_trace(second);
  replay_query_metadata_trace(covered);replay_query_metadata_trace(second);
  EXPECT_EQ(request.budget()->consumed_bytes(),175u);
  require_query_metadata("shared",40);require_query_metadata("shared",39);
  EXPECT_EQ(request.budget()->consumed_bytes(),185u);
  EXPECT_TRUE(request.status().ok());
  const auto stats=request.budget()->stage_summary_stats();
  EXPECT_EQ(stats.successful_stages,3u);EXPECT_GE(stats.memo_hits,1u);
  EXPECT_EQ(stats.full_fee_entries_visited,0u);
}

TEST(QueryMetadataStageSummary, BudgetPreflightFallsBackToExactSortedFailurePrefix) {
  for(const std::string domain:{"metadata","payload","extent"}) {
    SCOPED_TRACE(domain);
    std::string failing_key,message;
    const auto budget=std::make_shared<QueryMetadataBudget>();
    ScopedQueryMetadataBudget request(budget);
    if(domain=="metadata") {
      budget->observe_unique(QueryMetadataBudget::maximum_bytes-3);
      failing_key="b:metadata";message="request metadata byte budget exhausted";
    } else if(domain=="payload") {
      require_query_metadata("v04-payload-bytes/prelude",QueryMetadataBudget::maximum_payload_bytes-1);
      failing_key="v04-payload-bytes/failing";message="request payload byte budget exhausted";
    } else {
      require_query_metadata("v04-extents/prelude",QueryMetadataBudget::maximum_extents-1);
      failing_key="v04-extents/failing";message="request extent budget exhausted";
    }
    const auto fees=stage_batch({{"a:prefix",2},{failing_key,2},{"z:unreached",19}});
    ASSERT_TRUE(arm_query_metadata_stage_summary(stage_plan({fees})));
    EXPECT_THROW(replay_query_metadata_trace(fees),QueryMetadataBudgetExceeded);
    EXPECT_EQ(budget->consumed_bytes(),domain=="metadata"?QueryMetadataBudget::maximum_bytes-1:2u);
    EXPECT_EQ(request.status().message(),message);
    EXPECT_EQ(budget->stage_summary_stats().successful_stages,0u);
    EXPECT_EQ(budget->stage_summary_stats().full_fee_entries_visited,2u);
    EXPECT_THROW(require_query_metadata("a:prefix",0),QueryMetadataBudgetExceeded);
    EXPECT_THROW(budget->observe_unique(0),QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.status().message(),message);
  }
}

TEST(QueryMetadataStageSummary, MixedDomainsChooseFirstFailureAndPreserveZeroAndBinaryIdentities) {
  const std::string binary("x\0\xff\x80",4);
  const auto first=stage_batch({{binary,17},{"zero",0},{"v04-extents/x",3},
    {"v04-payload-bytes/x",11}});
  const auto empty=stage_batch({});
  ScopedQueryMetadataBudget request;
  ASSERT_TRUE(arm_query_metadata_stage_summary(stage_plan({first,empty,first})));
  replay_query_metadata_trace(first);replay_query_metadata_trace(empty);
  require_query_metadata(binary,19);replay_query_metadata_trace(first);
  EXPECT_EQ(request.budget()->consumed_bytes(),19u);
  require_query_metadata("v04-extents/fill",QueryMetadataBudget::maximum_extents-4);
  require_query_metadata("v04-payload-bytes/fill",QueryMetadataBudget::maximum_payload_bytes-12);
  const auto failing=stage_batch({{"a",1},{"v04-extents/fail",2},
    {"v04-payload-bytes/fail",2}});
  EXPECT_THROW(replay_query_metadata_trace(failing),QueryMetadataBudgetExceeded);
  EXPECT_EQ(request.status().message(),"request extent budget exhausted");
  EXPECT_EQ(request.budget()->consumed_bytes(),20u);
  EXPECT_THROW(replay_query_metadata_trace(empty),QueryMetadataBudgetExceeded);
}

TEST(QueryMetadataStageSummary, CollectorsPresentAtActivationAndInstalledLaterKeepAllEvidence) {
  const auto first=stage_batch({{"directory/page",11},{"other/source",17}});
  const auto second=stage_batch({{"directory/page",19},{"directory/next",23}});
  const auto plan=stage_plan({first,second});ASSERT_TRUE(plan);
  for(bool before:{false,true}) {
    SCOPED_TRACE(before);
    const auto budget=std::make_shared<QueryMetadataBudget>();
    ScopedQueryMetadataBudget request(budget);
    if(!before) {
      ASSERT_TRUE(arm_query_metadata_stage_summary(plan));
      replay_query_metadata_trace(first);
    }
    ScopedQueryMetadataTrace outer("");
    if(before)arm_query_metadata_stage_summary(plan);
    {
      ScopedQueryMetadataTrace inner("directory/");
      replay_query_metadata_trace(first);replay_query_metadata_trace(second);
      EXPECT_EQ(inner.release(),(QueryMetadataTrace{{"directory/page",19},{"directory/next",23}}));
    }
    EXPECT_EQ(outer.release(),(QueryMetadataTrace{{"directory/page",19},
      {"directory/next",23},{"other/source",17}}));
    EXPECT_EQ(budget->consumed_bytes(),59u);EXPECT_TRUE(request.status().ok());
    EXPECT_EQ(budget->stage_summary_stats().successful_stages,before?0u:1u);
    if(!before)EXPECT_GE(budget->stage_summary_stats().collector_exits,1u);
  }
}

TEST(QueryMetadataStageSummary, RequiredCollectorFailureAfterBorrowPreservesChargeAndCapturePrefix) {
  const auto first=stage_batch({{"a:first",7}});
  const auto second=stage_batch({{"a:first",11},{"b:second",13},{"c:third",17}});
  ScopedQueryMetadataBudget request;
  ASSERT_TRUE(arm_query_metadata_stage_summary(stage_plan({first,second})));
  replay_query_metadata_trace(first);
  ScopedQueryMetadataTrace outer("");
  unsigned reserves=0;
  {
    ScopedQueryMetadataTrace inner("",[&](std::uint64_t) {
      if(++reserves==2)throw std::runtime_error("required collector stopped");
    });
    EXPECT_EQ(summary_error([&]{replay_query_metadata_trace(second);}),"required collector stopped");
    EXPECT_TRUE(inner.failed());
    EXPECT_EQ(inner.release(),(QueryMetadataTrace{{"a:first",11}}));
    // Charging precedes the failing collector; the outer collector has not yet
    // seen b:second, and c:third must still be uncharged.
    EXPECT_EQ(request.budget()->consumed_bytes(),24u);
    EXPECT_THROW(replay_query_metadata_trace(first),QueryMetadataBudgetExceeded);
  }
  EXPECT_TRUE(request.status().ok());
  require_query_metadata("c:third",17);
  EXPECT_EQ(request.budget()->consumed_bytes(),41u);
  EXPECT_EQ(outer.release(),(QueryMetadataTrace{{"a:first",11},{"c:third",17}}));
}

TEST(QueryMetadataStageSummary, OptionalCollectorExitStillForwardsToOuterAndDoesNotPoisonRequest) {
  const auto first=stage_batch({{"a",7},{"b",11}});
  ScopedQueryMetadataBudget request;
  ASSERT_TRUE(arm_query_metadata_stage_summary(stage_plan({first,first})));
  replay_query_metadata_trace(first);
  ScopedQueryMetadataTrace outer("");
  {
    ScopedQueryMetadataTrace optional("",[](std::uint64_t){throw std::runtime_error("full");},
      QueryMetadataTracePolicy::CacheStable);
    EXPECT_NO_THROW(replay_query_metadata_trace(first));EXPECT_TRUE(optional.failed());
    EXPECT_TRUE(optional.release().empty());
  }
  EXPECT_EQ(outer.release(),first->fees());EXPECT_EQ(request.budget()->consumed_bytes(),18u);
  EXPECT_TRUE(request.status().ok());
}

TEST(QueryMetadataStageSummary, PreexistingCollectorAndTemporaryCollectorBothPermanentlyExitAggregation) {
  const auto first=stage_batch({{"a",7}}),second=stage_batch({{"a",11},{"b",13}});
  const auto plan=stage_plan({first,second});ASSERT_TRUE(plan);
  auto budget=std::make_shared<QueryMetadataBudget>();ASSERT_TRUE(budget->arm_summary(plan));
  {
    // The collector was constructed before this armed budget became active.
    ScopedQueryMetadataTrace collector("");ScopedQueryMetadataBudget request(budget);
    replay_query_metadata_trace(first);
    EXPECT_EQ(collector.release(),first->fees());
  }
  {
    ScopedQueryMetadataBudget request(budget);replay_query_metadata_trace(second);
    EXPECT_EQ(budget->consumed_bytes(),24u);
    EXPECT_EQ(budget->stage_summary_stats().successful_stages,0u);
    EXPECT_EQ(budget->stage_summary_stats().collector_exits,1u);
  }
  budget=std::make_shared<QueryMetadataBudget>();
  {
    ScopedQueryMetadataBudget request(budget);ASSERT_TRUE(arm_query_metadata_stage_summary(plan));
    replay_query_metadata_trace(first);
    {ScopedQueryMetadataTrace temporary("");}
    replay_query_metadata_trace(second);
    EXPECT_EQ(budget->consumed_bytes(),24u);
    EXPECT_EQ(budget->stage_summary_stats().successful_stages,1u);
    EXPECT_EQ(budget->stage_summary_stats().collector_exits,1u);
  }
}

TEST(QueryMetadataStageSummary, PrincipalPlanRearmRequiresIdenticalAlreadyBorrowedPrefix) {
  const auto selection=stage_batch({{"x",7}});
  const auto proof_a=stage_batch({{"x",11},{"principal/a",13}});
  const auto proof_b=stage_batch({{"x",19},{"principal/b",17}});
  const auto manifest=stage_batch({{"manifest",23}});
  const auto a=stage_plan({selection,proof_a,manifest});
  const auto b=stage_plan({selection,proof_b,manifest});ASSERT_TRUE(a);ASSERT_TRUE(b);
  for(bool after_proof:{false,true}) {
    SCOPED_TRACE(after_proof);ScopedQueryMetadataBudget request(std::make_shared<QueryMetadataBudget>());
    ASSERT_TRUE(arm_query_metadata_stage_summary(a));replay_query_metadata_trace(selection);
    if(after_proof)replay_query_metadata_trace(proof_a);
    EXPECT_EQ(arm_query_metadata_stage_summary(b),!after_proof);
    replay_query_metadata_trace(proof_b);replay_query_metadata_trace(manifest);
    EXPECT_EQ(request.budget()->consumed_bytes(),after_proof?72u:59u);
    require_query_metadata("x",29);
    EXPECT_EQ(request.budget()->consumed_bytes(),after_proof?82u:69u);
    EXPECT_TRUE(request.status().ok());
  }
}

TEST(QueryMetadataStageSummary, UnknownOrderCrossPlanAndLargeOverlayExitWithoutLosingBorrowedMaximums) {
  const auto first=stage_batch({{"x",7}}),second=stage_batch({{"x",11},{"y",13}});
  const auto other=stage_batch({{"x",19},{"z",17}});
  const auto plan=stage_plan({first,second});ASSERT_TRUE(plan);
  for(unsigned mode=0;mode<4;++mode) {
    SCOPED_TRACE(mode);ScopedQueryMetadataBudget request(std::make_shared<QueryMetadataBudget>());
    ASSERT_TRUE(arm_query_metadata_stage_summary(plan));
    if(mode==0)replay_query_metadata_trace(second);
    else replay_query_metadata_trace(first);
    if(mode==1)replay_query_metadata_trace(other);
    if(mode==2) {
      EXPECT_FALSE(arm_query_metadata_stage_summary(stage_plan({other})));
      replay_query_metadata_trace(other);
    }
    if(mode==3)for(unsigned i=0;i<129;++i)require_query_metadata("overlay/"+std::to_string(i),1);
    replay_query_metadata_trace(first);replay_query_metadata_trace(second);
    require_query_metadata("x",23);replay_query_metadata_trace(first);
    const auto expected=mode==3?165u:(mode==1||mode==2?53u:36u);
    EXPECT_EQ(request.budget()->consumed_bytes(),expected);EXPECT_TRUE(request.status().ok());
    EXPECT_GE(request.budget()->stage_summary_stats().fallbacks,1u);
  }
}

TEST(QueryMetadataStageSummary, QuotaDeclinesAndPlanBoundsDoNotChangeOrdinaryReplay) {
  const auto fees=stage_batch({{"a",7},{"b",11}});
  auto quota=std::make_shared<SummaryQuota>();quota->limit=0;
  EXPECT_FALSE(stage_plan({fees},quota));EXPECT_EQ(quota->used,0u);
  for(unsigned fail=1;fail<=3;++fail) {
    quota=std::make_shared<SummaryQuota>();quota->fail_at=fail;
    const auto plan=stage_plan({fees},quota);
    EXPECT_FALSE(plan);EXPECT_EQ(quota->used,0u);
  }
  EXPECT_FALSE(stage_plan(std::vector<StageBatch>(9,fees)));
  EXPECT_FALSE(stage_plan({StageBatch{}}));
  const auto overflow=stage_batch({{"a",UINT64_MAX},{"b",UINT64_MAX}});
  EXPECT_FALSE(stage_plan({overflow}));
  ScopedQueryMetadataBudget request;
  EXPECT_FALSE(arm_query_metadata_stage_summary({}));
  EXPECT_NO_THROW(replay_query_metadata_trace(fees));EXPECT_EQ(request.budget()->consumed_bytes(),18u);
  EXPECT_THROW(replay_query_metadata_trace(overflow),QueryMetadataBudgetExceeded);
}

TEST(QueryMetadataStageSummary, BorrowedPinsAndQuotaSurviveCacheRetirementUntilLastRequest) {
  auto quota=std::make_shared<SummaryQuota>();
  auto batch_pin=std::make_shared<int>(1);std::weak_ptr<int> weak_pin=batch_pin;
  auto fees=stage_batch({{std::string(4096,'k'),17}},batch_pin);
  std::weak_ptr<const PreparedQueryMetadataTrace> weak_batch=fees;
  auto plan=stage_plan({fees},quota);ASSERT_TRUE(plan);
  const auto retained=quota->used;EXPECT_GT(retained,0u);EXPECT_GT(quota->peak,retained);
  auto budget=std::make_shared<QueryMetadataBudget>();
  ASSERT_TRUE(budget->arm_summary(plan));
  {
    ScopedQueryMetadataBudget request(budget);replay_query_metadata_trace(fees);
  }
  // Simulate retiring both cache owners; only the request ledger remains.
  plan.reset();fees.reset();batch_pin.reset();
  EXPECT_FALSE(weak_batch.expired());EXPECT_FALSE(weak_pin.expired());
  EXPECT_EQ(quota->used,retained);
  // The external quota observer does not itself own the plan or batch.
  EXPECT_EQ(budget->consumed_bytes(),17u);
  budget.reset();
  EXPECT_TRUE(weak_batch.expired());EXPECT_TRUE(weak_pin.expired());
  EXPECT_EQ(quota->used,0u);
}

TEST(QueryMetadataStageSummary, SummaryRetainsNoIdentityLengthDependentStorageAndReleasesLastPin) {
  std::uint64_t short_retained=0,long_retained=0;
  for(bool long_key:{false,true}) {
    auto quota=std::make_shared<SummaryQuota>();std::weak_ptr<int> weak;
    {
      auto pin=std::make_shared<int>(7);weak=pin;
      const auto fees=stage_batch({{std::string(long_key?32768:4,'k'),17}},pin);
      const auto plan=stage_plan({fees},quota);ASSERT_TRUE(plan);
      if(long_key)long_retained=quota->used;else short_retained=quota->used;
      ScopedQueryMetadataBudget request(std::make_shared<QueryMetadataBudget>());
      ASSERT_TRUE(arm_query_metadata_stage_summary(plan));replay_query_metadata_trace(fees);
      EXPECT_EQ(request.budget()->consumed_bytes(),17u);
    }
    EXPECT_TRUE(weak.expired());EXPECT_EQ(quota->used,0u);
  }
  EXPECT_EQ(short_retained,long_retained);
}

TEST(QueryMetadataStageSummary, MemoCapacityAndConcurrentLedgersDoNotExemptUnknownBatches) {
  const auto first=stage_batch({{"x",7}}),second=stage_batch({{"x",11}});
  const auto plan=stage_plan({first,second});ASSERT_TRUE(plan);
  const auto budget=std::make_shared<QueryMetadataBudget>();
  {
    ScopedQueryMetadataBudget request(budget);ASSERT_TRUE(arm_query_metadata_stage_summary(plan));
    replay_query_metadata_trace(first);replay_query_metadata_trace(second);
    for(unsigned i=0;i<258;++i) {
      const auto extra=stage_batch({{"extra/"+std::to_string(i),1}});
      replay_query_metadata_trace(extra);replay_query_metadata_trace(extra);
    }
    const auto larger=stage_batch({{"extra/257",5}});replay_query_metadata_trace(larger);
    EXPECT_EQ(budget->consumed_bytes(),273u);
  }
  std::vector<std::future<QueryMetadataTrace>> workers;
  for(unsigned i=0;i<4;++i)workers.push_back(std::async(std::launch::async,[budget,first,second]{
    ScopedQueryMetadataBudget request(budget);ScopedQueryMetadataTrace collector("");
    replay_query_metadata_trace(first);replay_query_metadata_trace(second);
    return collector.release();
  }));
  for(auto& worker:workers)EXPECT_EQ(worker.get(),(QueryMetadataTrace{{"x",11}}));
  EXPECT_EQ(budget->consumed_bytes(),273u);
}

TEST(QueryMetadataStageSummary, SeededDifferentialReplayMatchesUnarmedLedgerAfterEveryAction) {
  std::mt19937_64 random(0xfee20261023ULL);
  for(unsigned trial=0;trial<600;++trial) {
    SCOPED_TRACE(trial);
    std::vector<StageBatch> batches;
    for(unsigned b=0;b<4;++b) {
      QueryMetadataTrace fees;
      for(unsigned i=0;i<18;++i) {
        const auto n=random()%18;
        std::string key=n<12?"metadata/":(n<15?"v04-payload-bytes/":"v04-extents/");
        key+=std::to_string(n);if(n%4==0)key+=std::string("\0\xff",2);
        std::uint64_t amount=random()%200;
        if(trial>=400&&random()%5==0)amount=n<12?QueryMetadataBudget::maximum_bytes/2:
          (n<15?QueryMetadataBudget::maximum_payload_bytes/2:QueryMetadataBudget::maximum_extents/2);
        fees[key]=amount;
      }
      batches.push_back(stage_batch(std::move(fees)));
    }
    const auto plan=stage_plan({batches[0],batches[1],batches[2],batches[3],batches[1]});
    ASSERT_TRUE(plan);
    const auto reference=std::make_shared<QueryMetadataBudget>();
    const auto candidate=std::make_shared<QueryMetadataBudget>();ASSERT_TRUE(candidate->arm_summary(plan));
    auto pair_action=[&](const std::function<void()>& operation) {
      const auto run=[&](const std::shared_ptr<QueryMetadataBudget>& ledger) {
        ScopedQueryMetadataBudget scope(ledger);return summary_error(operation);
      };
      EXPECT_EQ(run(reference),run(candidate));compare_summary_status(reference,candidate);
    };
    for(unsigned action=0;action<35;++action) {
      SCOPED_TRACE(action);
      const auto choice=random()%10;
      if(trial%2==0&&action<4)pair_action([&,action]{replay_query_metadata_trace(batches[action]);});
      else if(choice<5) {
        const auto batch=batches[random()%4];pair_action([&]{replay_query_metadata_trace(batch);});
      } else if(choice<9) {
        const auto n=random()%18;
        const auto key=(n<12?"metadata/":(n<15?"v04-payload-bytes/":"v04-extents/"))+std::to_string(n);
        const auto amount=random()%1000;pair_action([&]{require_query_metadata(key,amount);});
      } else {
        const auto amount=random()%1000;pair_action([&]{require_query_metadata_unique(amount);});
      }
    }
    // Follow-up increments exercise identities hidden in borrowed prefixes,
    // including overlaps after ordering/overlay exits, rather than only totals.
    for(const auto& batch:batches)for(const auto& fee:batch->fees()) {
      const auto amount=fee.second+1;
      pair_action([&]{require_query_metadata(fee.first,amount);});
    }
  }
}
