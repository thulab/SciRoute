#include "iotdbfs/formats/hdf5/Hdf5QueryMode.h"
#include <gtest/gtest.h>

using namespace iotdbfs;

TEST(Hdf5QueryMode, ExactlyTwoExecutionModesAndIndexedDefault) {
  ASSERT_TRUE(parse_hdf5_query_configuration({}).ok());
  EXPECT_EQ(parse_hdf5_query_configuration({}).value(), Hdf5QueryMode::IoTDBIndexed);
  for (const auto mode : {Hdf5QueryMode::NativeHdf5, Hdf5QueryMode::IoTDBIndexed}) {
    const std::string value = hdf5_query_mode_name(mode);
    auto parsed = parse_hdf5_query_configuration({{"query.hdf5.mode", value}});
    ASSERT_TRUE(parsed.ok());
    EXPECT_EQ(parsed.value(), mode);
  }
}

TEST(Hdf5QueryMode, RemovedCatalogPropertyAlwaysFailsWithoutAliases) {
  for (const std::string value : {"strict", "substructure-index", "native-hdf5", ""}) {
    auto parsed = parse_hdf5_query_configuration({{"query.hdf5.catalog_mode", value},
      {"query.hdf5.mode", "iotdb-indexed"}});
    ASSERT_FALSE(parsed.ok());
    EXPECT_NE(parsed.error().find("was removed"), std::string::npos);
  }
}

TEST(Hdf5QueryMode, RejectsHistoricalAndUnknownExecutionNames) {
  for (const std::string value : {"strict", "substructure-index", "typed", "native",
      "catalog-native-fuse", "catalog-native-local", "", "NATIVE-HDF5", "iotdb-indexed "})
    EXPECT_FALSE(parse_hdf5_query_mode(value).ok()) << value;
}
