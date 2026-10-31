#include "iotdbfs/Logging.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

namespace {

std::string g_log_base_dir = "logs";
std::mutex g_log_mutex;
std::atomic<LogLevel> g_log_level{ LogLevel::INFO };

struct FileCloser {
  void operator()(std::FILE* fp) const {
    if (fp)
      std::fclose(fp);
  }
};

using FilePtr = std::unique_ptr<std::FILE, FileCloser>;
std::unordered_map<std::string, FilePtr> g_level_files;

std::string to_lower_copy(const std::string& in) {
  std::string out;
  out.reserve(in.size());
  for (char ch : in)
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  return out;
}

std::FILE* ensure_file_locked(const std::string& key,
  const fs::path& file_path) {
  auto it = g_level_files.find(key);
  if (it != g_level_files.end())
    return it->second.get();

  std::error_code ec;
  fs::create_directories(file_path.parent_path(), ec);
  std::FILE* fp = std::fopen(file_path.string().c_str(), "a");
  if (!fp)
    return nullptr;
  g_level_files.emplace(key, FilePtr(fp));
  return fp;
}

std::FILE* ensure_level_file_locked(const std::string& level) {
  const std::string level_lower = to_lower_copy(level);
  fs::path level_dir = fs::path(g_log_base_dir) / level_lower;
  return ensure_file_locked(
    "level:" + level_lower, level_dir / (level_lower + ".log"));
}

std::FILE* ensure_combined_file_locked() {
  return ensure_file_locked(
    "combined", fs::path(g_log_base_dir) / "iotdb_fs.log");
}

void write_line(std::FILE* fp, const std::string& line) {
  if (!fp)
    return;
  std::fprintf(fp, "%s\n", line.c_str());
  std::fflush(fp);
}

const char* canonical_level_name(const char* lvl) {
  LogLevel parsed = logging_level_from_string(
    lvl ? std::string_view(lvl) : std::string_view());
  switch (parsed) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARN:
    return "WARN";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::OFF:
    return "OFF";
  }
  return "INFO";
}

} // namespace

void init_logging(const std::string& base_dir) {
  std::lock_guard<std::mutex> lk(g_log_mutex);
  std::string dir = base_dir.empty() ? "logs" : base_dir;
  if (dir == g_log_base_dir)
    return;
  g_level_files.clear();
  g_log_base_dir = std::move(dir);
}

void set_logging_level(LogLevel lvl) {
  g_log_level.store(lvl, std::memory_order_relaxed);
}

LogLevel get_logging_level() {
  return g_log_level.load(std::memory_order_relaxed);
}

std::string logging_level_to_string(LogLevel lvl) {
  switch (lvl) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARN:
    return "WARN";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::OFF:
    return "OFF";
  }
  return "INFO";
}

LogLevel logging_level_from_string(std::string_view lvl) {
  std::string up(lvl.begin(), lvl.end());
  std::transform(up.begin(), up.end(), up.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
    });
  if (up == "DEBUG" || up == "TRACE")
    return LogLevel::DEBUG;
  if (up == "INFO")
    return LogLevel::INFO;
  if (up == "WARN" || up == "WARNING")
    return LogLevel::WARN;
  if (up == "ERR" || up == "ERROR")
    return LogLevel::ERROR;
  if (up == "OFF" || up == "NONE")
    return LogLevel::OFF;
  return LogLevel::INFO;
}

void log_common(const char* lvl, const char* func, int line,
  const std::string& msg) {
  LogLevel msg_level = logging_level_from_string(lvl ? std::string_view(lvl) : std::string_view());
  if (static_cast<int>(msg_level) < static_cast<int>(get_logging_level()))
    return;
  const char* level_name = canonical_level_name(lvl);

  time_t t = time(nullptr);
  struct tm tmv;
  localtime_r(&t, &tmv);
  char ts[32];
  strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

  long long ms = now_ms() % 1000;
  std::ostringstream oss;
  oss << "[IOTDB-FS][" << level_name << "][" << ts << '.'
      << std::setw(3) << std::setfill('0') << ms << "][" << func
      << ':' << line << "] " << msg;
  const std::string line_str = oss.str();

  std::lock_guard<std::mutex> lk(g_log_mutex);
  std::fprintf(stderr, "%s\n", line_str.c_str());
  write_line(ensure_combined_file_locked(), line_str);
  write_line(ensure_level_file_locked(level_name), line_str);
}
