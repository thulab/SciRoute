#include <iostream>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <string>

// namespace fs handled by Filesystem.h

int main() {
  const char* env_src = std::getenv("GRIB_SRC");
  std::string src = env_src && *env_src
    ? std::string(env_src)
    : std::string("/root/qxj-data/GRIB-Test-1/")
      + "KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  std::string dst = "/mnt/dfs/show.grib2";

  if (!fs::exists(src)) {
    std::cerr << "❌ GRIB source file not found: " << src << std::endl;
    std::cerr << "   You can set GRIB_SRC=/path/to/file.grib2" << std::endl;
    return 2;
  }

  std::string cmd = "cp '" + src + "' '" + dst + "'";

  std::cout << "📤 Executing system command:\n" << cmd << std::endl;

  int ret = system(cmd.c_str());
  if (ret == 0)
    std::cout << "✅ Copy command executed successfully." << std::endl;
  else
    std::cerr << "❌ Copy command failed, exit code = " << ret << std::endl;

  return ret;
}
