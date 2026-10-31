#include "iotdbfs/query/training/TrainingQueryModel.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

namespace {
using namespace iotdbfs;

TrainingViewSpec view() {
  TrainingViewSpec result;
  result.view_id = "forecast";
  result.experiment_id = "experiment-a";
  result.snapshot_fingerprint = "stable-snapshot";
  result.input_channels = {{"target", 1, "y"}, {"target", 0, "x"}};
  result.target_channels = {{"target", 0, "x"}};
  result.input_length = 2;
  result.target_length = 1;
  result.stride = 1;
  result.split = {"train", {0, 10}, {0, 10}};
  return result;
}

TrainingViewSpec standardized_view() {
  auto result = view();
  result.transform.kind = TrainingTransformKind::Standardize;
  result.transform.artifact_id = "train-statistics-v1";
  // Artifact serialization order does not change the semantic identity.
  result.transform.statistics = {
    {"target", 0, 1.0, 2.0}, {"target", 1, 100.0, 1.0}};
  return result;
}

} // namespace

TEST(TrainingViewTransform, IdentityLeavesValuesAndMasksUntouched) {
  const auto selected = view();
  WindowSample sample;
  sample.input_values = {101, 1, 999, 2};
  sample.input_validity = {1, 1, 0, 1};
  sample.target_values = {3};
  sample.target_validity = {1};
  const auto original = sample;
  EXPECT_TRUE(validate_training_transform(selected).ok());
  EXPECT_TRUE(apply_training_view_transform(selected, sample).ok());
  EXPECT_EQ(sample.input_values, original.input_values);
  EXPECT_EQ(sample.input_validity, original.input_validity);
  EXPECT_EQ(sample.target_values, original.target_values);
  EXPECT_EQ(sample.target_validity, original.target_validity);
}

TEST(TrainingViewTransform, StandardizesBothRolesAndPreservesNullMask) {
  const auto selected = standardized_view();
  const std::vector<float> decoded_base{101, 1, 999, 2};
  WindowSample sample;
  sample.input_values = decoded_base;
  sample.input_validity = {1, 1, 0, 1};
  sample.target_values = {3};
  sample.target_validity = {1};
  ASSERT_TRUE(validate_training_transform(selected).ok());
  ASSERT_TRUE(apply_training_view_transform(selected, sample).ok());
  EXPECT_EQ(sample.input_values, (std::vector<float>{1, 0, 999, 0.5f}));
  EXPECT_EQ(sample.target_values, (std::vector<float>{1}));
  EXPECT_EQ(sample.input_validity,
    (std::vector<std::uint8_t>{1, 1, 0, 1}));
  EXPECT_EQ(sample.target_validity, (std::vector<std::uint8_t>{1}));
  EXPECT_EQ(decoded_base, (std::vector<float>{101, 1, 999, 2}));
}

TEST(TrainingViewTransform, StableIdentityBindsSemanticsButNotExperiment) {
  auto first = standardized_view();
  auto second = first;
  second.experiment_id = "experiment-b";
  std::reverse(second.transform.statistics.begin(),
    second.transform.statistics.end());
  EXPECT_EQ(training_view_identity(first), training_view_identity(second));
  EXPECT_EQ(training_sample_id(first.snapshot_fingerprint,
      training_view_identity(first), "series", 4),
    training_sample_id(second.snapshot_fingerprint,
      training_view_identity(second), "series", 4));

  second.transform.statistics.front().mean += 1;
  EXPECT_NE(training_view_identity(first), training_view_identity(second));
  second = first;
  second.transform.artifact_id = "train-statistics-v2";
  EXPECT_NE(training_view_identity(first), training_view_identity(second));
  second = first;
  std::reverse(second.input_channels.begin(), second.input_channels.end());
  EXPECT_NE(training_view_identity(first), training_view_identity(second));
  second = first;
  second.input_length += 1;
  EXPECT_NE(training_view_identity(first), training_view_identity(second));
  second = first;
  second.missing.null_value = MissingTreatment::Mask;
  EXPECT_NE(training_view_identity(first), training_view_identity(second));
  EXPECT_NE(training_view_identity(first), training_view_identity(view()));
}

TEST(TrainingViewTransform, RejectsMissingDuplicateAndNonfiniteStatistics) {
  auto selected = standardized_view();
  selected.transform.statistics.pop_back();
  EXPECT_EQ(validate_training_transform(selected).code(), QueryError::InvalidQuery);
  selected = standardized_view();
  selected.transform.statistics[1] = selected.transform.statistics[0];
  EXPECT_EQ(validate_training_transform(selected).code(), QueryError::InvalidQuery);
  selected = standardized_view();
  selected.transform.statistics[0].stddev = 0;
  EXPECT_EQ(validate_training_transform(selected).code(), QueryError::InvalidQuery);
  selected = standardized_view();
  selected.transform.statistics[0].mean =
    std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(validate_training_transform(selected).code(), QueryError::InvalidQuery);
  selected = view();
  selected.transform.artifact_id = "unexpected";
  EXPECT_EQ(validate_training_transform(selected).code(), QueryError::InvalidQuery);
}

TEST(TrainingViewTransform, OverflowFailsWithoutPartialMutation) {
  auto selected = standardized_view();
  selected.transform.statistics[0].stddev = 1e-300;
  WindowSample sample;
  sample.input_values = {101, 1, 102, 2};
  sample.input_validity = {1, 1, 1, 1};
  sample.target_values = {3};
  sample.target_validity = {1};
  const auto before_input = sample.input_values;
  const auto before_target = sample.target_values;
  EXPECT_EQ(apply_training_view_transform(selected, sample).code(),
    QueryError::CorruptPayload);
  EXPECT_EQ(sample.input_values, before_input);
  EXPECT_EQ(sample.target_values, before_target);
}

namespace {
void expect_equal_bits(const std::vector<float>& actual,
    const std::vector<float>& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  if (!actual.empty())
    EXPECT_EQ(std::memcmp(actual.data(), expected.data(),
      actual.size() * sizeof(float)), 0);
}

void expect_compiled_matches(const TrainingViewSpec& selected,
    const WindowSample& original) {
  const auto compiled = CompiledTrainingTransform::compile(selected);
  ASSERT_TRUE(compiled.ok()) << compiled.status().message();
  auto reference = original;
  auto optimized = original;
  const auto expected = apply_training_view_transform(selected, reference);
  const auto actual = compiled.value().apply(optimized);
  EXPECT_EQ(actual.code(), expected.code());
  EXPECT_EQ(actual.phase(), expected.phase());
  EXPECT_EQ(actual.message(), expected.message());
  expect_equal_bits(optimized.input_values, reference.input_values);
  expect_equal_bits(optimized.target_values, reference.target_values);
  EXPECT_EQ(optimized.input_validity, reference.input_validity);
  EXPECT_EQ(optimized.target_validity, reference.target_validity);
  if (!actual.ok()) {
    expect_equal_bits(optimized.input_values, original.input_values);
    expect_equal_bits(optimized.target_values, original.target_values);
  }
}
} // namespace

TEST(TrainingViewTransform, CompiledMatchesReferenceWithPermutedChannelsAndNulls) {
  std::mt19937 random(713);
  auto selected = standardized_view();
  selected.input_channels.clear();
  selected.target_channels.clear();
  selected.transform.statistics.clear();
  for (std::uint64_t i = 0; i < 37; ++i) {
    TrainingChannelRef channel{"field-" + std::to_string(i % 3), i / 3, ""};
    selected.input_channels.push_back(channel);
    if (i % 2) selected.target_channels.push_back(channel);
    selected.transform.statistics.push_back({channel.field_id, channel.ordinal,
      static_cast<double>(i) / 13.0, static_cast<double>(i + 1) / 7.0});
  }
  for (unsigned repeat = 0; repeat < 12; ++repeat) {
    std::shuffle(selected.input_channels.begin(), selected.input_channels.end(), random);
    std::shuffle(selected.target_channels.begin(), selected.target_channels.end(), random);
    std::shuffle(selected.transform.statistics.begin(), selected.transform.statistics.end(), random);
    WindowSample sample;
    auto fill = [&](std::vector<float>& values, std::vector<std::uint8_t>& validity,
        std::size_t count) {
      for (std::size_t i = 0; i < count; ++i) {
        values.push_back(static_cast<float>(static_cast<int>(random() % 100000) - 50000) / 127);
        validity.push_back(i % 5 ? 1 : 0);
        if (!validity.back()) values.back() = std::numeric_limits<float>::quiet_NaN();
      }
    };
    fill(sample.input_values, sample.input_validity, 11 * selected.input_channels.size());
    fill(sample.target_values, sample.target_validity, 3 * selected.target_channels.size());
    expect_compiled_matches(selected, sample);
  }
}

TEST(TrainingViewTransform, CompiledPreservesShapeMaskAndFiniteFailureOrderAtomically) {
  const auto selected = standardized_view();
  WindowSample valid;
  valid.input_values = {101, 1, 102, 2};
  valid.input_validity = {1, 1, 1, 1};
  valid.target_values = {3};
  valid.target_validity = {1};
  for (unsigned scenario = 0; scenario < 9; ++scenario) {
    auto sample = valid;
    switch (scenario) {
      case 0: sample.input_validity.pop_back(); break;
      case 1: sample.input_values.pop_back(); sample.input_validity.pop_back(); break;
      case 2: sample.input_validity[3] = 2; break;
      case 3: sample.target_validity[0] = 2; break;
      case 4: sample.target_values[0] = std::numeric_limits<float>::infinity(); break;
      case 5: sample.input_values[0] = std::numeric_limits<float>::quiet_NaN(); break;
      // Earlier corrupt input wins over later invalid masks/target shapes.
      case 6: sample.input_values[0] = std::numeric_limits<float>::infinity();
        sample.input_validity[1] = 2; break;
      case 7: sample.input_values[0] = std::numeric_limits<float>::infinity();
        sample.target_validity.clear(); break;
      case 8: sample.input_values[0] = -0.0f; break;
    }
    expect_compiled_matches(selected, sample);
    // Identity intentionally does not validate the sample; preserve that API.
    expect_compiled_matches(view(), sample);
  }
  auto overflow = selected;
  overflow.transform.statistics[0].stddev = 1e-300;
  expect_compiled_matches(overflow, valid);
}

TEST(TrainingViewTransform, CompiledPlanOwnsStatisticsAndHandlesEmptyRoles) {
  auto selected = standardized_view();
  const auto compiled = CompiledTrainingTransform::compile(selected);
  ASSERT_TRUE(compiled.ok());
  EXPECT_GT(compiled.value().estimated_dynamic_bytes(), 0u);
  selected.transform.statistics.clear();
  WindowSample sample;
  sample.input_values = {101, 1}; sample.input_validity = {1, 1};
  ASSERT_TRUE(compiled.value().apply(sample).ok());
  EXPECT_EQ(sample.input_values, (std::vector<float>{1, 0}));
  selected = standardized_view();
  selected.target_channels.clear(); selected.target_length = 0;
  expect_compiled_matches(selected, sample);
  selected.input_channels.clear(); selected.transform.statistics.clear();
  expect_compiled_matches(selected, WindowSample{});
  expect_compiled_matches(selected, sample); // Nonempty data for no channels.
  const auto identity = CompiledTrainingTransform::compile(view());
  ASSERT_TRUE(identity.ok());
  EXPECT_EQ(identity.value().estimated_dynamic_bytes(), 0u);
}

TEST(TrainingViewTransform, CompilationUsesThePublicStatisticsValidation) {
  for (unsigned scenario = 0; scenario < 7; ++scenario) {
    auto selected = standardized_view();
    switch (scenario) {
      case 0: selected.transform.statistics.pop_back(); break;
      case 1: selected.transform.statistics[1] = selected.transform.statistics[0]; break;
      case 2: selected.transform.statistics[0].stddev = 0; break;
      case 3: selected.transform.statistics[0].mean = std::numeric_limits<double>::infinity(); break;
      case 4: selected.transform.artifact_id.clear(); break;
      case 5: selected.transform.kind = TrainingTransformKind::Identity; break;
      case 6: selected.transform.kind = static_cast<TrainingTransformKind>(42); break;
    }
    const auto expected = validate_training_transform(selected);
    const auto compiled = CompiledTrainingTransform::compile(selected);
    ASSERT_FALSE(compiled.ok());
    EXPECT_EQ(compiled.status().code(), expected.code());
    EXPECT_EQ(compiled.status().message(), expected.message());
  }
}
