#pragma once

#include <string>

namespace tools {

// Status codes returned by write_grib_file(). Zero indicates success.
enum class GribWriterStatus {
  Success = 0,
  InvalidArgument = 1,
  SourceNotFound = 2,
  SourceNotRegular = 3,
  SourceOpenFailed = 4,
  SourceReadError = 5,
  NotAGribFile = 6,
  EnsureTargetDirFailed = 7,
  TargetOpenFailed = 8,
  TargetWriteFailed = 9,
  TargetFinalizeFailed = 10,
};

// Copy a GRIB file from source_path to target_path with basic validation.
// Returns a GribWriterStatus value (cast to int) indicating success/failure.
int write_grib_file(const std::string& source_path,
  const std::string& target_path);

// Translate a GribWriterStatus into a human-readable description.
const char* grib_writer_status_message(GribWriterStatus status);

} // namespace tools
