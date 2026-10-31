#include "iotdbfs/MetadataQueryEvaluator.h"

#include <gtest/gtest.h>

using namespace iotdbfs;

namespace {
MetadataConfig config() {
  const auto result = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.experiment.source=experimentId\n"
    "metadata.field.experiment.type=string\n"
    "metadata.field.experiment.aliases=exp\n"
    "metadata.field.experiment.xattr=user.grib.experiment\n"
    "metadata.field.experiment.queryable=true\n"
    "metadata.field.level.source=level\n"
    "metadata.field.level.type=int64\n"
    "metadata.field.level.xattr=user.grib.level\n"
    "metadata.field.level.queryable=true\n"
    "metadata.field.hidden.source=hidden\n"
    "metadata.field.hidden.type=bool\n");
  EXPECT_TRUE(result.ok());
  return result.value();
}
}

TEST(MetadataQueryEvaluator, ParsesAliasesAndMatchesTypedValues) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  MetadataMap values{{"experiment", MetadataValue("Storm")},
    {"level", MetadataValue(std::int64_t(850))}};
  const auto result = evaluator.evaluate("exp=storm, level=850", values);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.value());
}

TEST(MetadataQueryEvaluator, RejectsUnknownAndNonQueryableFields) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  EXPECT_FALSE(evaluator.parse("unknown=x").ok());
  EXPECT_FALSE(evaluator.parse("hidden=true").ok());
}

TEST(MetadataQueryEvaluator, RejectsMalformedOrWrongTypeValues) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  EXPECT_FALSE(evaluator.parse("level=850.0").ok());
  EXPECT_FALSE(evaluator.parse("level=").ok());
  EXPECT_FALSE(evaluator.parse("level=1=2").ok());
}

TEST(MetadataQueryEvaluator, MissingMetadataDoesNotMatch) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  MetadataMap values{{"experiment", MetadataValue("storm")}};
  const auto result = evaluator.evaluate("level=850", values);
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(result.value());
}

TEST(MetadataQueryEvaluator, ScalarPredicatesUseSetMembership) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  MetadataMap values{
    {"experiment", MetadataValue(MetadataStringSet{"analysis", "Storm"})},
    {"level", MetadataValue(MetadataInt64Set{500, 850})},
  };
  const auto present = evaluator.evaluate("exp=storm,level=500", values);
  ASSERT_TRUE(present.ok()) << present.error();
  EXPECT_TRUE(present.value());
  const auto absent = evaluator.evaluate("exp=rain,level=500", values);
  ASSERT_TRUE(absent.ok()) << absent.error();
  EXPECT_FALSE(absent.value());
}

TEST(MetadataQueryEvaluator, RejectsUnvalidatedOrEmptyPredicates) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  MetadataMap values{{"hidden", MetadataValue(true)},
    {"level", MetadataValue(std::int64_t(850))}};
  EXPECT_FALSE(evaluator.matches(values, {{"hidden", MetadataValue(true)}}));
  EXPECT_FALSE(evaluator.matches(values, {}));
}

TEST(MetadataQueryEvaluator, CanonicalizesAliasesAndTrimsEveryTerm) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  const auto parsed = evaluator.parse(" \tEXP = Storm \r\n, LEVEL = -850 ");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  ASSERT_EQ(parsed.value().size(), 2u);
  EXPECT_EQ(parsed.value()[0].field_id, "experiment");
  EXPECT_EQ(std::get<std::string>(parsed.value()[0].expected.value()), "Storm");
  EXPECT_EQ(parsed.value()[1].field_id, "level");
  EXPECT_EQ(std::get<std::int64_t>(parsed.value()[1].expected.value()), -850);
}

TEST(MetadataQueryEvaluator, AllTermsMustMatchWithoutImplicitTypeConversion) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  const MetadataMap values{{"experiment", MetadataValue("Storm")},
                          {"level", MetadataValue(std::int64_t{850})}};
  for (const std::string expression : {"exp=rain,level=850", "exp=storm,level=500",
                                      "level=850,level=500"}) {
    const auto result = evaluator.evaluate(expression, values);
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_FALSE(result.value()) << expression;
  }
  const auto result = evaluator.evaluate("level=850", {{"level", MetadataValue(850.0)}});
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_FALSE(result.value());
}

TEST(MetadataQueryEvaluator, RejectsEmptyMalformedAndOverflowingTerms) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  for (const std::string expression : {"", " \t\n", "level", "=850", ",level=850",
      "level=850,", "level=850,,exp=x", "level=+1", "level=1junk",
      "level=9223372036854775808", "level=-9223372036854775809"}) {
    SCOPED_TRACE(expression);
    const auto parsed = evaluator.parse(expression);
    EXPECT_FALSE(parsed.ok());
    EXPECT_FALSE(parsed.error().empty());
    const auto evaluated = evaluator.evaluate(expression, {});
    EXPECT_FALSE(evaluated.ok());
    EXPECT_EQ(evaluated.error(), parsed.error());
  }
  for (const std::string raw : {"-9223372036854775808", "9223372036854775807"})
    EXPECT_TRUE(evaluator.parse("level=" + raw).ok()) << raw;
}

TEST(MetadataQueryEvaluator, BoundsQueryBytesStringBytesAndPredicateCount) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  EXPECT_TRUE(evaluator.parse("exp=" + std::string(4096, 'a')).ok());
  EXPECT_FALSE(evaluator.parse("exp=" + std::string(4097, 'a')).ok());
  EXPECT_FALSE(evaluator.parse("exp=" + std::string("\xff")).ok());
  const std::string expression = "level=1";
  EXPECT_TRUE(evaluator.parse(expression + std::string(8192 - expression.size(), ' ')).ok());
  EXPECT_FALSE(evaluator.parse(expression + std::string(8193 - expression.size(), ' ')).ok());
  std::string terms;
  for (int i = 0; i < 32; ++i) terms += (i ? ",level=1" : "level=1");
  const auto parsed = evaluator.parse(terms);
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  EXPECT_EQ(parsed.value().size(), 32u);
  const MetadataMap values{{"level", MetadataValue(std::int64_t{1})}};
  EXPECT_TRUE(evaluator.matches(values, parsed.value()));
  EXPECT_FALSE(evaluator.parse(terms + ",level=1").ok());
  auto too_many = parsed.value();
  too_many.push_back(too_many.front());
  EXPECT_FALSE(evaluator.matches(values, too_many));
}

TEST(MetadataQueryEvaluator, RejectsForgedNoncanonicalAndWrongTypePredicates) {
  const auto cfg = config();
  MetadataQueryEvaluator evaluator(cfg);
  const MetadataMap values{{"experiment", MetadataValue("storm")},
                          {"level", MetadataValue(std::int64_t{850})}};
  EXPECT_FALSE(evaluator.matches(values, {{"exp", MetadataValue("storm")}}));
  EXPECT_FALSE(evaluator.matches(values, {{"LEVEL", MetadataValue(std::int64_t{850})}}));
  EXPECT_FALSE(evaluator.matches(values, {{"level", MetadataValue(850.0)}}));
  EXPECT_FALSE(evaluator.matches(values, {{"unknown", MetadataValue("storm")}}));
}

TEST(MetadataQueryEvaluator, ParsesBooleanAndFiniteDoublePredicates) {
  const auto cfg = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.flag.source=flag\nmetadata.field.flag.type=bool\n"
    "metadata.field.flag.xattr=user.weather.flag\nmetadata.field.flag.queryable=true\n"
    "metadata.field.real.source=real\nmetadata.field.real.type=double\n"
    "metadata.field.real.xattr=user.weather.real\nmetadata.field.real.queryable=true\n");
  ASSERT_TRUE(cfg.ok()) << cfg.error();
  MetadataQueryEvaluator evaluator(cfg.value());
  for (const std::string raw : {"true", "TRUE", "1"}) {
    const auto result = evaluator.evaluate("flag=" + raw, {{"flag", MetadataValue(true)}});
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_TRUE(result.value());
  }
  for (const std::string raw : {"false", "FaLsE", "0"}) {
    const auto result = evaluator.evaluate("flag=" + raw, {{"flag", MetadataValue(false)}});
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_TRUE(result.value());
  }
  const auto real = evaluator.evaluate("real=-1.25e2", {{"real", MetadataValue(-125.0)}});
  ASSERT_TRUE(real.ok()) << real.error();
  EXPECT_TRUE(real.value());
  for (const std::string raw : {"nan", "inf", "-inf", "1e999", "1.2junk"})
    EXPECT_FALSE(evaluator.parse("real=" + raw).ok()) << raw;
  for (const std::string raw : {"yes", "no", "2", "truejunk"})
    EXPECT_FALSE(evaluator.parse("flag=" + raw).ok()) << raw;
}

TEST(MetadataQueryEvaluator, DisablesAmbiguousAliasesWithoutDisablingUniqueNames) {
  const auto cfg = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.a.source=sourceA\nmetadata.field.a.type=string\n"
    "metadata.field.a.aliases=shared,onlyA\nmetadata.field.a.xattr=user.weather.a\n"
    "metadata.field.a.queryable=true\n"
    "metadata.field.b.source=sourceB\nmetadata.field.b.type=string\n"
    "metadata.field.b.aliases=SHARED,onlyB\nmetadata.field.b.xattr=user.weather.b\n"
    "metadata.field.b.queryable=true\n");
  ASSERT_TRUE(cfg.ok()) << cfg.error();
  MetadataQueryEvaluator evaluator(cfg.value());
  EXPECT_FALSE(evaluator.parse("shared=x").ok());
  EXPECT_FALSE(evaluator.parse("SHARED=x").ok());
  const auto parsed = evaluator.parse("onlyA=x,onlyB=y");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const MetadataMap values{{"a", MetadataValue("x")}, {"b", MetadataValue("y")}};
  EXPECT_TRUE(evaluator.matches(values, parsed.value()));
  EXPECT_FALSE(evaluator.matches(values, {{"shared", MetadataValue("x")}}));
  EXPECT_FALSE(evaluator.parse("sourceA=x").ok());
}
