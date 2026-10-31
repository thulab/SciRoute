#pragma once

#include <cstddef>
#include <string>

#include "iotdbfs/IoTDBConnector.h"

int run_semantic_grep_client(int argc, char** argv,
  const IoTDBConfig& default_cfg,
  const std::string& default_mount_point,
  size_t default_pool_size);
