#pragma once

#include <string>

namespace iotdbfs {

// Configure ecCodes runtime data paths before the first ecCodes handle is made.
// Returns true if a usable definitions path was found or already configured.
bool configure_eccodes_runtime(const char* argv0 = nullptr,
  std::string* message = nullptr);

} // namespace iotdbfs
