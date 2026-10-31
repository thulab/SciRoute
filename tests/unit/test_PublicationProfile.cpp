#include "iotdbfs/PublicationProfile.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <iterator>
#include <thread>
#include <type_traits>
#include <vector>

using namespace iotdbfs;

static_assert(std::is_nothrow_constructible<PublicationProfileScope,
  std::shared_ptr<PublicationProfileRecord>>::value, "activation must fail open");
static_assert(std::is_nothrow_constructible<PublicationStageScope, const char*>::value,
  "literal stages must not allocate before entering a noexcept constructor");
static_assert(std::is_nothrow_constructible<PublicationOperationTimer,
  PublicationOperation, const PublicationProfileContext&>::value,
  "deferred context copies must be caught inside the timer");
static_assert(std::is_nothrow_constructible<PublicationDetailTimer, const char*>::value,
  "detail activation must fail open without caller-side allocation");

TEST(PublicationProfile, DisabledScopesAndTimersDoNotCreateRecords) {
  EXPECT_FALSE(active_publication_profile.record);
  {
    PublicationStageScope stage("disabled");
    PublicationOperationTimer operation(PublicationOperation::Tablet);
    EXPECT_FALSE(operation.context().record);
    operation.finish(true);
  }
  EXPECT_FALSE(active_publication_profile.record);
  EXPECT_TRUE(active_publication_profile.stage.empty());
}

TEST(PublicationProfile, DisabledLongLiteralStageLeavesContextUntouched) {
  char long_stage[4096];
  std::fill(std::begin(long_stage), std::end(long_stage), 'x');
  long_stage[sizeof(long_stage) - 1] = '\0';
  PublicationStageScope stage(long_stage);
  PublicationOperationTimer operation(PublicationOperation::QuerySubmit);
  EXPECT_FALSE(active_publication_profile.record);
  EXPECT_TRUE(active_publication_profile.stage.empty());
  EXPECT_FALSE(operation.deferred_context());
}

TEST(PublicationProfile, NestedPublicationAndStageScopesRestoreTheCaller) {
  auto outer = std::make_shared<PublicationProfileRecord>();
  auto inner = std::make_shared<PublicationProfileRecord>();
  {
    PublicationProfileScope publication(outer);
    PublicationStageScope total("total");
    {
      PublicationStageScope payload("payload");
      PublicationOperationTimer timer(PublicationOperation::Tablet);
      timer.finish(true);
      {
        PublicationProfileScope nested(inner);
        PublicationStageScope stage("inner");
        PublicationOperationTimer timer(PublicationOperation::NonQuery);
        timer.finish(true);
      }
      EXPECT_EQ(active_publication_profile.record, outer);
      EXPECT_EQ(active_publication_profile.stage, "payload");
    }
    EXPECT_EQ(active_publication_profile.stage, "total");
    PublicationOperationTimer timer(PublicationOperation::QuerySubmit);
    timer.finish(true);
  }
  EXPECT_FALSE(active_publication_profile.record);
  const auto stages = outer->snapshot();
  EXPECT_EQ(stages.at("payload").tablet.calls, 1);
  EXPECT_EQ(stages.at("total").query_submit.calls, 1);
  EXPECT_EQ(stages.count("inner"), 0);
  EXPECT_EQ(inner->snapshot().at("inner").nonquery.calls, 1);
}

TEST(PublicationProfile, FinishIsIdempotentAndUnfinishedOrThrowingOperationsFail) {
  auto record = std::make_shared<PublicationProfileRecord>();
  PublicationProfileScope publication(record);
  PublicationStageScope stage("description");
  {
    PublicationOperationTimer timer(PublicationOperation::NonQuery);
    timer.finish(true);
    timer.finish(false);
  }
  { PublicationOperationTimer failed(PublicationOperation::NonQuery); }
  try {
    PublicationOperationTimer failed(PublicationOperation::NonQuery);
    throw 1;
  } catch (int) { }
  const auto metric = record->snapshot().at("description").nonquery;
  EXPECT_EQ(metric.calls, 3);
  EXPECT_EQ(metric.failures, 2);
}

TEST(PublicationProfile, DeferredCloseKeepsOriginalRecordAndStageAcrossThreads) {
  auto record = std::make_shared<PublicationProfileRecord>();
  std::shared_ptr<const PublicationProfileContext> captured;
  {
    PublicationProfileScope publication(record);
    PublicationStageScope stage("begin");
    PublicationOperationTimer submit(PublicationOperation::QuerySubmit);
    captured = submit.deferred_context();
    submit.finish(true);
  }
  auto unrelated = std::make_shared<PublicationProfileRecord>();
  {
    PublicationProfileScope other(unrelated);
    PublicationStageScope stage("unrelated");
    std::thread close([captured] {
      EXPECT_FALSE(active_publication_profile.record);
      PublicationOperationTimer timer(PublicationOperation::QueryClose, *captured);
      timer.finish(false);
    });
    close.join();
  }
  EXPECT_EQ(record->snapshot().at("begin").query_close.calls, 1);
  EXPECT_EQ(record->snapshot().at("begin").query_close.failures, 1);
  EXPECT_EQ(unrelated->snapshot().at("unrelated").query_close.calls, 0);
}

TEST(PublicationProfile, DeferredContextPinsRecordUntilLastCloseIsReleased) {
  std::weak_ptr<PublicationProfileRecord> weak;
  std::shared_ptr<const PublicationProfileContext> deferred;
  {
    auto record = std::make_shared<PublicationProfileRecord>();
    weak = record;
    PublicationProfileScope scope(record);
    PublicationStageScope stage("publish");
    PublicationOperationTimer submit(PublicationOperation::QuerySubmit);
    submit.finish(true);
    deferred = submit.deferred_context();
  }
  ASSERT_FALSE(weak.expired());
  {
    PublicationOperationTimer close(PublicationOperation::QueryClose, *deferred);
    close.finish(true);
  }
  EXPECT_EQ(weak.lock()->snapshot().at("publish").query_close.calls, 1);
  deferred.reset();
  EXPECT_TRUE(weak.expired());
}

TEST(PublicationProfile, SharedRecordsAggregateConcurrentDeferredObservations) {
  auto record = std::make_shared<PublicationProfileRecord>();
  std::vector<std::thread> workers;
  for (unsigned i = 0; i < 4; ++i)
    workers.emplace_back([record] {
      for (unsigned j = 0; j < 1000; ++j)
        record->observe("payload", PublicationOperation::QueryClose, j % 2, 7);
    });
  for (auto& worker : workers) worker.join();
  const auto metric = record->snapshot().at("payload").query_close;
  EXPECT_EQ(metric.calls, 4000);
  EXPECT_EQ(metric.failures, 2000);
  EXPECT_EQ(metric.nanoseconds, 28000);
}

TEST(PublicationProfile, FetchMethodCallsRemainSeparateFromSubmitAndClose) {
  PublicationProfileRecord record;
  record.observe("payload", PublicationOperation::QuerySubmit, true, 3);
  record.observe("payload", PublicationOperation::NonQuery, true, 5);
  record.observe("payload", PublicationOperation::Tablet, true, 7);
  record.observe("payload", PublicationOperation::QueryClose, true, 11);
  record.observe("payload", PublicationOperation::QueryFetch, true, 13);
  record.observe("payload", PublicationOperation::QueryFetch, false, 17);
  const auto stage = record.snapshot().at("payload");
  EXPECT_EQ(stage.query_submit.nanoseconds, 3);
  EXPECT_EQ(stage.nonquery.nanoseconds, 5);
  EXPECT_EQ(stage.tablet.nanoseconds, 7);
  EXPECT_EQ(stage.query_close.calls, 1);
  EXPECT_EQ(stage.query_close.nanoseconds, 11);
  EXPECT_EQ(stage.query_fetch.calls, 2);
  EXPECT_EQ(stage.query_fetch.failures, 1);
  EXPECT_EQ(stage.query_fetch.nanoseconds, 30);
  EXPECT_NE(record.to_json().find("\"query_fetch\":{\"calls\":2,\"failures\":1,\"nanoseconds\":30}"),
            std::string::npos);
}

TEST(PublicationProfile, JsonIsStableEscapesStageNamesAndHasNoSqlFields) {
  PublicationProfileRecord record;
  record.observe("z", PublicationOperation::Tablet, true, 11);
  record.observe("a\"\n\\", PublicationOperation::QuerySubmit, false, 5);
  record.observe_stage("z", 20);
  const auto json = record.to_json();
  EXPECT_EQ(json, record.to_json());
  EXPECT_LT(json.find("a\\\"\\u000a\\\\"), json.find("\"z\""));
  EXPECT_NE(json.find("\"wall_nanoseconds\":20"), std::string::npos);
  EXPECT_NE(json.find("\"calls\":1,\"failures\":1,\"nanoseconds\":5"), std::string::npos);
  EXPECT_EQ(json.find("sql"), std::string::npos);
  EXPECT_EQ(json.find("credential"), std::string::npos);
}

TEST(PublicationProfile, ConstructionAndNestedSdkMetricsDoNotChangeApiTotals) {
  PublicationProfileRecord record;
  record.observe("payload", PublicationOperation::TabletBuild, true, 3);
  record.observe("payload", PublicationOperation::ObjectEncode, false, 5);
  record.observe("payload", PublicationOperation::SessionAcquire, true, 7);
  record.observe("payload", PublicationOperation::SdkInsert, true, 11);
  record.observe("payload", PublicationOperation::Tablet, true, 20);
  const auto value = record.snapshot().at("payload");
  EXPECT_EQ(value.tablet_build.calls, 1);
  EXPECT_EQ(value.tablet_build.nanoseconds, 3);
  EXPECT_EQ(value.object_encode.failures, 1);
  EXPECT_EQ(value.object_encode.nanoseconds, 5);
  EXPECT_EQ(value.session_acquire.nanoseconds, 7);
  EXPECT_EQ(value.sdk_insert.nanoseconds, 11);
  EXPECT_EQ(value.tablet.nanoseconds, 20);
  EXPECT_EQ(value.query_submit.calls, 0);
  EXPECT_NE(record.to_json().find("\"object_encode\":{\"calls\":1,\"failures\":1,\"nanoseconds\":5}"),
    std::string::npos);
}

TEST(PublicationProfile, DetailTimersKeepParentStageAndRecordFailureExactlyOnce) {
  auto record = std::make_shared<PublicationProfileRecord>();
  PublicationProfileScope scope(record);
  PublicationStageScope stage("payload");
  {
    PublicationDetailTimer read("payload_source_read");
    EXPECT_EQ(active_publication_profile.stage, "payload");
    read.finish(true);
    read.finish(false);
  }
  try {
    PublicationDetailTimer read("payload_source_read");
    throw 1;
  } catch (int) { }
  const auto values = record->snapshot();
  ASSERT_EQ(values.size(), 1);
  const auto& metric = values.at("payload").details.at("payload_source_read");
  EXPECT_EQ(metric.calls, 2);
  EXPECT_EQ(metric.failures, 1);
  EXPECT_EQ(values.at("payload").tablet.calls, 0);
}

TEST(PublicationProfile, DetailTimerCapturesOriginalPublicationAndStage) {
  auto outer = std::make_shared<PublicationProfileRecord>();
  auto inner = std::make_shared<PublicationProfileRecord>();
  {
    PublicationProfileScope scope(outer);
    PublicationStageScope stage("payload");
    PublicationDetailTimer work("encode");
    {
      PublicationProfileScope nested(inner);
      PublicationStageScope other("unrelated");
      work.finish(true);
    }
  }
  EXPECT_EQ(outer->snapshot().at("payload").details.at("encode").calls, 1);
  EXPECT_TRUE(inner->snapshot().at("unrelated").details.empty());
  EXPECT_FALSE(active_publication_profile.record);
}

TEST(PublicationProfile, DisabledDetailDoesNotInstallContextAndJsonEscapesNames) {
  {
    PublicationDetailTimer detail("disabled");
    detail.finish(true);
    EXPECT_FALSE(active_publication_profile.record);
    EXPECT_TRUE(active_publication_profile.stage.empty());
  }
  PublicationProfileRecord record;
  record.observe_detail("payload", "a\"\n\\", true, 13);
  record.observe_detail("payload", "z", false, 17);
  const auto json = record.to_json();
  EXPECT_NE(json.find("\"details\":{\"a\\\"\\u000a\\\\\":{\"calls\":1,\"failures\":0,\"nanoseconds\":13}"),
    std::string::npos);
  EXPECT_LT(json.find("a\\\"\\u000a\\\\"), json.find("\"z\""));
}
