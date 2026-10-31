#pragma once

#include <chrono>
#include <string>
#include <string_view>

inline long long now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
    .count();
}

enum class LogLevel { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3, OFF = 4 };

// Parse textual log levels (INFO/WARN/...) into enum values.
LogLevel logging_level_from_string(std::string_view lvl);
// Convert enum values back to their uppercase string names.
std::string logging_level_to_string(LogLevel lvl);
// Change the global logging level threshold.
void set_logging_level(LogLevel lvl);
// Query the global logging level threshold.
LogLevel get_logging_level();

// Configure the log output directory; creates a combined log and per-level logs.
void init_logging(const std::string& base_dir);

// Core logging routine used by helper macros to emit a line.
void log_common(const char* lvl, const char* func, int line,
  const std::string& msg);

#ifndef LOG_DEBUG
#define LOG_DEBUG(m) log_common("DEBUG", __FUNCTION__, __LINE__, (m))
#endif
#ifndef LOG_INFO
#define LOG_INFO(m) log_common("INFO", __FUNCTION__, __LINE__, (m))
#endif
#ifndef LOG_WARN
#define LOG_WARN(m) log_common("WARN", __FUNCTION__, __LINE__, (m))
#endif
#ifndef LOG_WARNING
#define LOG_WARNING(m) LOG_WARN(m)
#endif
#ifndef LOG_ERROR
#define LOG_ERROR(m) log_common("ERROR", __FUNCTION__, __LINE__, (m))
#endif
