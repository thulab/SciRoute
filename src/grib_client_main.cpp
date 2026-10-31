#include "iotdbfs/CrossFileClient.h"
#include "iotdbfs/formats/grib/EcCodesRuntime.h"
#include "iotdbfs/FeatureConfig.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/Logging.h"

#include <iostream>
#include <string>

int main(int argc, char** argv) {
  std::string eccodes_message;
  (void)iotdbfs::configure_eccodes_runtime(
    argc > 0 ? argv[0] : nullptr, &eccodes_message);
  if (!iotdbfs::load_grib_extension_enabled_from_default_config(true)) {
    std::cerr << "grib_client requires GRIB extension support, but "
              << iotdbfs::kGribExtensionEnabledKey << "=false\n";
    return 2;
  }
  set_logging_level(LogLevel::WARN);
  return run_cross_file_client(argc, argv,
    IoTDBConfig{"127.0.0.1", 6667, "root", "root", "dfs", 0},
    "/mnt/dfs", 4);
}
