#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include "iotdbfs/Filesystem.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// namespace fs handled by Filesystem.h

struct Config {
  std::string mysql_host = "127.0.0.1";
  int mysql_port = 3306;
  std::string mysql_user = "root";
  std::string mysql_pass;
  std::string mysql_db = "iotdb_fs_perf";
  std::string mysql_bin = "mysql";
  uint64_t mysql_max_packet = 1ull << 30;
  std::string blob_table = "blob";
  std::string single_path;
  fs::path paths_file;
  std::string path_prefix;
  int num_tasks = 50;
  int concurrency = 5;
  bool verbose = false;
};

namespace {

std::string shell_quote(const std::string& s) {
  std::string quoted;
  quoted.reserve(s.size() + 2);
  quoted.push_back('\'');
  for (char ch : s) {
    if (ch == '\'')
      quoted += "'\\''";
    else
      quoted.push_back(ch);
  }
  quoted.push_back('\'');
  return quoted;
}

bool parse_int(const std::string& s, int& out) {
  if (s.empty()) return false;
  try {
    long v = std::stol(s);
    if (v <= 0 || v > std::numeric_limits<int>::max())
      return false;
    out = static_cast<int>(v);
    return true;
  }
  catch (...) {
    return false;
  }
}

bool parse_u64(const std::string& s, uint64_t& out) {
  if (s.empty()) return false;
  try {
    unsigned long long v = std::stoull(s);
    if (v == 0)
      return false;
    out = static_cast<uint64_t>(v);
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  Config def;
  std::cout << "用法: " << prog
    << " [--host=HOST] [--port=PORT] [--user=USER] [--pass=PASS]\n"
    << "             [--db=DB] [--mysql-bin=PATH] [--max-packet=BYTES] [--blob-table=NAME]\n"
    << "             [--path=PATH] [--paths-file=FILE] [--path-prefix=PREFIX]\n"
    << "             [--tasks=N] [--concurrency=N] [--verbose=true|false]\n"
    << "说明: 从 MySQL blob 表中并发读取整份 GRIB 文件，用于性能测试。\n";
}

bool parse_args(int argc, char* argv[], Config& cfg) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }
    if (arg.rfind("--", 0) != 0) {
      std::cerr << "未知参数: " << arg << "\n";
      return false;
    }
    auto eq = arg.find('=');
    if (eq == std::string::npos || eq <= 2) {
      std::cerr << "参数格式应为 --key=value: " << arg << "\n";
      return false;
    }
    std::string key = arg.substr(2, eq - 2);
    std::string value = arg.substr(eq + 1);

    if (key == "host") {
      cfg.mysql_host = value;
    }
    else if (key == "port") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 port 数值: " << value << "\n";
        return false;
      }
      cfg.mysql_port = v;
    }
    else if (key == "user") {
      cfg.mysql_user = value;
    }
    else if (key == "pass") {
      cfg.mysql_pass = value;
    }
    else if (key == "db" || key == "database") {
      cfg.mysql_db = value;
    }
    else if (key == "mysql-bin" || key == "mysql") {
      cfg.mysql_bin = value;
    }
    else if (key == "max-packet" || key == "max-allowed-packet") {
      uint64_t v = 0;
      if (!parse_u64(value, v)) {
        std::cerr << "无效的 max-packet 数值: " << value << "\n";
        return false;
      }
      cfg.mysql_max_packet = v;
    }
    else if (key == "blob-table") {
      cfg.blob_table = value;
    }
    else if (key == "path") {
      cfg.single_path = value;
    }
    else if (key == "paths-file") {
      cfg.paths_file = value;
    }
    else if (key == "path-prefix") {
      cfg.path_prefix = value;
    }
    else if (key == "tasks") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 tasks 数值: " << value << "\n";
        return false;
      }
      cfg.num_tasks = v;
    }
    else if (key == "concurrency" || key == "threads") {
      int v = 0;
      if (!parse_int(value, v)) {
        std::cerr << "无效的 concurrency 数值: " << value << "\n";
        return false;
      }
      cfg.concurrency = v;
    }
    else if (key == "verbose") {
      cfg.verbose = (value == "1" || value == "true" || value == "yes");
    }
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }
  return true;
}

std::string escape_sql(const std::string& s) {
  std::string out;
  out.reserve(s.size() * 2);
  for (char ch : s) {
    if (ch == '\'')
      out += "''";
    else
      out.push_back(ch);
  }
  return out;
}

std::string quote_identifier(const std::string& name) {
  std::string out;
  out.reserve(name.size() + 2);
  out.push_back('`');
  for (char ch : name) {
    if (ch == '`')
      out += "``";
    else
      out.push_back(ch);
  }
  out.push_back('`');
  return out;
}

void strip_mysql_password_warning(std::string& text) {
  const std::string warn_nl = "mysql: [Warning] Using a password on the command line interface can be insecure.\n";
  const std::string warn = "mysql: [Warning] Using a password on the command line interface can be insecure.";
  size_t pos = std::string::npos;
  while ((pos = text.find(warn_nl)) != std::string::npos)
    text.erase(pos, warn_nl.size());
  while ((pos = text.find(warn)) != std::string::npos)
    text.erase(pos, warn.size());
}

bool run_mysql_text(const Config& cfg, const std::string& sql, std::string& output) {
  std::ostringstream cmd;
  cmd << shell_quote(cfg.mysql_bin)
    << " --batch --silent --raw --skip-column-names"
    << " --host=" << shell_quote(cfg.mysql_host)
    << " --port=" << cfg.mysql_port
    << " --user=" << shell_quote(cfg.mysql_user);
  if (!cfg.mysql_pass.empty())
    cmd << " --password=" << shell_quote(cfg.mysql_pass);
  cmd << " --max_allowed_packet=" << cfg.mysql_max_packet;
  cmd << " " << shell_quote(cfg.mysql_db);
  cmd << " -e " << shell_quote(sql) << " 2>&1";

  FILE* pipe = popen(cmd.str().c_str(), "r");
  if (!pipe) {
    std::cerr << "[ERR] 无法启动 mysql 命令: " << cmd.str() << "\n";
    return false;
  }
  output.clear();
  std::array<char, 4096> buffer{};
  while (true) {
    size_t n = fread(buffer.data(), 1, buffer.size(), pipe);
    if (n > 0)
      output.append(buffer.data(), n);
    if (n < buffer.size()) {
      if (feof(pipe))
        break;
      if (ferror(pipe)) {
        output.append("[read error]");
        break;
      }
    }
    if (n == 0)
      break;
  }
  int ret = pclose(pipe);
  if (ret != 0) {
    std::cerr << "[ERR] mysql 命令执行失败 (返回值 " << ret << ")\n";
    std::cerr << "      SQL: " << sql << "\n";
    if (!output.empty())
      std::cerr << "      输出: " << output << "\n";
    return false;
  }
  strip_mysql_password_warning(output);
  return true;
}

bool fetch_blob_size(const Config& cfg, const std::string& path, uint64_t& size_out) {
  std::ostringstream sql;
  sql << "SELECT OCTET_LENGTH(data) FROM " << quote_identifier(cfg.blob_table)
      << " WHERE path='" << escape_sql(path) << "' LIMIT 1;";
  std::string output;
  if (!run_mysql_text(cfg, sql.str(), output))
    return false;
  if (output.empty()) {
    std::cerr << "[ERR] 未找到路径对应的记录: " << path << "\n";
    return false;
  }
  std::istringstream iss(output);
  std::string line;
  if (!std::getline(iss, line)) {
    std::cerr << "[ERR] 解析 OCTET_LENGTH 输出失败。\n";
    return false;
  }
  try {
    size_out = std::stoull(line);
  }
  catch (...) {
    std::cerr << "[ERR] 无法将 \"" << line << "\" 解析为大小数值。\n";
    return false;
  }
  return true;
}

struct TaskInfo {
  std::string path;
  uint64_t expected_size = 0;
};

bool load_paths_from_file(const Config& cfg, std::vector<TaskInfo>& out) {
  std::ifstream fin(cfg.paths_file);
  if (!fin.is_open()) {
    std::cerr << "[ERR] 无法打开路径文件: " << cfg.paths_file << "\n";
    return false;
  }
  std::string line;
  while (std::getline(fin, line)) {
    // 去除行尾回车与首尾空白
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
      line.pop_back();
    size_t begin = line.find_first_not_of(" \t");
    if (begin == std::string::npos)
      continue;
    size_t end = line.find_last_not_of(" \t");
    std::string path = line.substr(begin, end - begin + 1);
    uint64_t sz = 0;
    if (!fetch_blob_size(cfg, path, sz))
      return false;
    out.push_back(TaskInfo{ path, sz });
  }
  if (out.empty()) {
    std::cerr << "[ERR] 路径文件为空: " << cfg.paths_file << "\n";
    return false;
  }
  return true;
}

bool load_recent_paths(const Config& cfg, size_t limit, std::vector<TaskInfo>& out) {
  std::ostringstream sql;
  sql << "SELECT path,size FROM " << quote_identifier(cfg.blob_table);
  if (!cfg.path_prefix.empty()) {
    sql << " WHERE path LIKE '" << escape_sql(cfg.path_prefix) << "%'";
  }
  sql << " ORDER BY mtime DESC LIMIT " << limit << ";";
  std::string output;
  if (!run_mysql_text(cfg, sql.str(), output))
    return false;

  std::istringstream iss(output);
  std::string line;
  while (std::getline(iss, line)) {
    if (line.empty())
      continue;
    auto tab = line.find('\t');
    if (tab == std::string::npos) {
      std::cerr << "[WARN] 无法解析行: " << line << "\n";
      continue;
    }
    std::string path = line.substr(0, tab);
    std::string size_str = line.substr(tab + 1);
    try {
      uint64_t sz = std::stoull(size_str);
      out.push_back(TaskInfo{ path, sz });
    }
    catch (...) {
      std::cerr << "[WARN] 无法解析大小: " << size_str << " (path=" << path << ")\n";
    }
  }
  if (out.empty()) {
    std::cerr << "[ERR] 未从数据库获取到 blob 路径，请确认表中有数据。\n";
    return false;
  }
  return true;
}

bool fetch_blob_stream(const Config& cfg, const std::string& path,
  uint64_t expected_size, uint64_t& bytes_read) {
  std::ostringstream sql;
  sql << "SELECT data FROM " << quote_identifier(cfg.blob_table)
      << " WHERE path='" << escape_sql(path) << "' LIMIT 1;";

  std::ostringstream cmd;
  cmd << shell_quote(cfg.mysql_bin)
      << " --batch --raw --binary-mode --skip-column-names"
      << " --host=" << shell_quote(cfg.mysql_host)
      << " --port=" << cfg.mysql_port
      << " --user=" << shell_quote(cfg.mysql_user);
  if (!cfg.mysql_pass.empty())
    cmd << " --password=" << shell_quote(cfg.mysql_pass);
  cmd << " --max_allowed_packet=" << cfg.mysql_max_packet;
  cmd << " " << shell_quote(cfg.mysql_db)
      << " -e " << shell_quote(sql.str());

  FILE* pipe = popen(cmd.str().c_str(), "r");
  if (!pipe) {
    std::cerr << "[ERR] 无法启动 mysql 读取命令。\n";
    return false;
  }

  bytes_read = 0;
  int last_byte = -1;
  std::array<unsigned char, 1 << 15> buffer{};
  while (true) {
    size_t n = fread(buffer.data(), 1, buffer.size(), pipe);
    if (n > 0) {
      bytes_read += static_cast<uint64_t>(n);
      last_byte = buffer[n - 1];
    }
    if (n < buffer.size()) {
      if (feof(pipe))
        break;
      if (ferror(pipe)) {
        std::cerr << "[ERR] 读取 mysql 输出失败。\n";
        pclose(pipe);
        return false;
      }
    }
    if (n == 0)
      break;
  }
  int ret = pclose(pipe);
  if (ret != 0) {
    std::cerr << "[ERR] mysql 命令执行失败 (返回值 " << ret << ") path=" << path << "\n";
    return false;
  }

  if (expected_size != std::numeric_limits<uint64_t>::max()
    && bytes_read == expected_size + 1 && last_byte == '\n') {
    bytes_read -= 1; // 去除 mysql 输出行末换行
  }

  if (expected_size != std::numeric_limits<uint64_t>::max()
    && bytes_read != expected_size) {
    std::cerr << "[ERR] 读取字节数与预期不符，path=" << path
      << " expected=" << expected_size << " actual=" << bytes_read << "\n";
    return false;
  }
  if (bytes_read == 0) {
    std::cerr << "[ERR] 读取结果为空，path=" << path << "\n";
    return false;
  }
  return true;
}

} // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  if (cfg.num_tasks <= 0 || cfg.concurrency <= 0) {
    std::cerr << "[ERR] tasks / concurrency 必须为正整数。\n";
    return 1;
  }

  std::vector<TaskInfo> base_paths;
  if (cfg.paths_file != fs::path{}) {
    if (!load_paths_from_file(cfg, base_paths))
      return 1;
  }
  if (!cfg.single_path.empty()) {
    uint64_t sz = 0;
    if (!fetch_blob_size(cfg, cfg.single_path, sz))
      return 1;
    base_paths.push_back(TaskInfo{ cfg.single_path, sz });
  }
  if (base_paths.empty()) {
    if (!load_recent_paths(cfg, static_cast<size_t>(cfg.num_tasks), base_paths))
      return 1;
  }

  std::vector<TaskInfo> tasks;
  tasks.reserve(static_cast<size_t>(cfg.num_tasks));
  for (int i = 0; i < cfg.num_tasks; ++i) {
    const TaskInfo& info = base_paths[static_cast<size_t>(i) % base_paths.size()];
    tasks.push_back(info);
  }

  std::cout << "MySQL: " << cfg.mysql_host << ":" << cfg.mysql_port
    << " DB=" << cfg.mysql_db << "\n";
  std::cout << "blob 表: " << cfg.blob_table
    << "  操作次数: " << cfg.num_tasks
    << "  并发数: " << cfg.concurrency << "\n";
  if (!cfg.path_prefix.empty())
    std::cout << "路径前缀过滤: " << cfg.path_prefix << "\n";

  std::atomic<int> next_index{ 0 };
  std::atomic<int> success_cnt{ 0 };
  std::atomic<int> fail_cnt{ 0 };
  std::atomic<uint64_t> sum_bytes{ 0 };
  std::atomic<uint64_t> sum_latency_ns{ 0 };
  std::mutex print_mu;

  const auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(cfg.concurrency);
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&, t]() {
      while (true) {
        int idx = next_index.fetch_add(1, std::memory_order_relaxed);
        if (idx >= cfg.num_tasks)
          break;

        const TaskInfo& task = tasks[static_cast<size_t>(idx)];
        const auto op_begin = std::chrono::steady_clock::now();

        uint64_t bytes = 0;
        bool ok = fetch_blob_stream(cfg, task.path, task.expected_size, bytes);
        const auto op_end = std::chrono::steady_clock::now();
        uint64_t op_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());

        if (ok) {
          success_cnt.fetch_add(1, std::memory_order_relaxed);
          sum_bytes.fetch_add(bytes, std::memory_order_relaxed);
          sum_latency_ns.fetch_add(op_ns, std::memory_order_relaxed);
          if (cfg.verbose) {
            std::lock_guard<std::mutex> lk(print_mu);
            std::cout << "[TASK " << idx << "] path=" << task.path
              << " bytes=" << bytes
              << " latency=" << std::fixed << std::setprecision(3)
              << (op_ns / 1e6) << " ms\n";
          }
        }
        else {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lk(print_mu);
          std::cout << "[TASK " << idx << "] 读取失败 path=" << task.path << "\n";
        }
      }
      });
  }

  for (auto& th : workers)
    th.join();

  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  const double seconds = ms / 1000.0;
  const double mib = sum_bytes.load() / (1024.0 * 1024.0);
  const double throughput_mibs = seconds > 0 ? (mib / seconds) : 0.0;

  std::cout << "\n====== MySQL 整文件读取统计 ======\n";
  std::cout << "成功: " << success_cnt.load() << "  失败: " << fail_cnt.load() << "\n";
  std::cout << "总耗时: " << ms << " ms (" << std::fixed << std::setprecision(3)
    << seconds << " s)\n";
  std::cout << "读取总量: " << sum_bytes.load() << " bytes ("
    << std::fixed << std::setprecision(2) << mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2)
    << throughput_mibs << " MiB/s\n";
  if (success_cnt.load() > 0) {
    double avg_latency_ms =
      static_cast<double>(sum_latency_ns.load()) / 1e6 / success_cnt.load();
    std::cout << "平均任务耗时: " << std::fixed << std::setprecision(3)
      << avg_latency_ms << " ms\n";
  }

  if (fail_cnt.load() > 0) {
    std::cerr << "[WARN] 存在失败任务，请检查 MySQL 连接、权限或数据完整性。\n";
  }
  return (fail_cnt.load() == 0) ? 0 : 2;
}

