#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <new>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

// namespace fs handled by Filesystem.h

struct Config {
  fs::path src_file = "/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  int num_tasks = 100;
  int concurrency = 10;
};

namespace {

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

void print_usage(const char* prog) {
  Config def;
  std::cout << "用法: " << prog << " [--src=FILE] [--tasks=N] [--concurrency=N]\n"
            << "默认: src=" << def.src_file << " tasks=" << def.num_tasks
            << " concurrency=" << def.concurrency << "\n"
            << "说明: 始终顺序加载整个文件，无分块、无随机读取\n";
}

bool parse_args(int argc, char** argv, Config& cfg) {
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

    if (key == "src" || key == "source") {
      cfg.src_file = value;
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
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }
  return true;
}

} // namespace

static std::mutex g_print_mutex;

static bool read_file_cat_style(const fs::path& src, std::vector<char>& buffer,
  const uint64_t expected_bytes, uint64_t& out_bytes) {
  const int fd = ::open(src.c_str(), O_RDONLY);
  if (fd < 0) {
    std::lock_guard<std::mutex> lk(g_print_mutex);
    std::cerr << "[ERR] 无法打开文件: " << src << " : " << std::strerror(errno) << '\n';
    return false;
  }

  out_bytes = 0;
  while (true) {
    const ssize_t n = ::read(fd, buffer.data(), buffer.size());
    if (n > 0) {
      out_bytes += static_cast<uint64_t>(n);
      continue;
    }
    if (n == 0) {
      ::close(fd);
      if (out_bytes != expected_bytes) {
        std::lock_guard<std::mutex> lk(g_print_mutex);
        std::cerr << "[ERR] 提前到达 EOF: 期望 " << expected_bytes
                  << " bytes，实际 " << out_bytes << " bytes。\n";
        return false;
      }
      return true;
    }
    if (errno == EINTR) {
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(g_print_mutex);
      std::cerr << "[ERR] 读取失败: " << std::strerror(errno) << '\n';
    }
    ::close(fd);
    return false;
  }
}

static std::vector<std::string> expand_level_values(const fs::path& original) {
  std::vector<std::string> levels;
  std::string s = original.string();
  const std::string key = "level=";
  std::size_t pos = s.find(key);
  if (pos == std::string::npos) {
    return levels;
  }
  pos += key.size();
  std::size_t end = pos;
  while (end < s.size()) {
    char c = s[end];
    if (std::isdigit(static_cast<unsigned char>(c)) || c == '-' || c == '+')
      ++end;
    else
      break;
  }
  if (end == pos)
    return levels;
  levels.reserve((950 - 50) / 50 + 1);
  for (int lvl = 50; lvl <= 950; lvl += 50)
    levels.emplace_back(std::to_string(lvl));
  return levels;
}

static fs::path rewrite_level_value(const fs::path& original,
  const std::string& level_value) {
  std::string s = original.string();
  const std::string key = "level=";
  std::size_t pos = s.find(key);
  if (pos == std::string::npos)
    return original;
  pos += key.size();
  std::size_t end = pos;
  while (end < s.size()) {
    char c = s[end];
    if (std::isdigit(static_cast<unsigned char>(c)) || c == '-' || c == '+')
      ++end;
    else
      break;
  }
  return fs::path(s.substr(0, pos) + level_value + s.substr(end));
}

int main(int argc, char** argv) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  if (cfg.src_file.empty()) {
    std::cerr << "[ERR] 未指定源文件。\n";
    return 1;
  }

  const fs::path& src_file = cfg.src_file;
  std::vector<std::string> level_values = expand_level_values(src_file);
  const bool level_random = !level_values.empty();

  std::vector<fs::path> task_paths;
  std::vector<uint64_t> expected_sizes;
  task_paths.reserve(static_cast<size_t>(cfg.num_tasks));

  if (level_random) {
    bool warned_zero = false;
    std::vector<uint64_t> level_sizes(level_values.size(), 0);
    std::vector<bool> level_valid(level_values.size(), false);
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<size_t> dist(0, level_values.size() - 1);

    for (int task = 0; task < cfg.num_tasks; ++task) {
      size_t idx = dist(rng);
      fs::path variant = rewrite_level_value(src_file, level_values[idx]);
      if (!level_valid[idx]) {
        if (!fs::exists(variant)) {
          std::cerr << "[ERR] 源文件不存在: " << variant << "\n";
          return 1;
        }
        uintmax_t sz = 0;
        try { sz = fs::file_size(variant); }
        catch (const std::exception& e) {
          std::cerr << "[ERR] 获取源文件大小失败: " << e.what()
            << " (" << variant << ")\n";
          return 1;
        }
        if (sz == 0 && !warned_zero) {
          std::cerr << "[WARN] 检测到大小为 0 的 level 变体，测试可能没有意义。\n";
          warned_zero = true;
        }
        if (sz > std::numeric_limits<size_t>::max()) {
          std::cerr << "[ERR] 文件过大，无法在当前架构下全部加载到内存: "
            << variant << "\n";
          return 1;
        }
        level_sizes[idx] = static_cast<uint64_t>(sz);
        level_valid[idx] = true;
      }
      task_paths.push_back(variant);
      expected_sizes.push_back(level_sizes[idx]);
    }
  }
  else {
    if (!fs::exists(src_file)) {
      std::cerr << "[ERR] 源文件不存在: " << src_file << "\n";
      return 1;
    }
    uintmax_t src_size = 0;
    try { src_size = fs::file_size(src_file); }
    catch (const std::exception& e) {
      std::cerr << "[ERR] 获取源文件大小失败: " << e.what() << "\n";
      return 1;
    }

    if (src_size == 0) {
      std::cerr << "[WARN] 源文件大小为 0，测试可能没有意义。\n";
    }

    if (src_size > std::numeric_limits<size_t>::max()) {
      std::cerr << "[ERR] 文件过大，无法在当前架构下全部加载到内存。\n";
      return 1;
    }

    const uint64_t sz64 = static_cast<uint64_t>(src_size);
    task_paths.assign(static_cast<size_t>(cfg.num_tasks), src_file);
    expected_sizes.assign(task_paths.size(), sz64);
  }

  if (task_paths.empty()) {
    std::cerr << "[ERR] 未生成任何读取任务。\n";
    return 1;
  }

  cfg.num_tasks = static_cast<int>(task_paths.size());

  const uint64_t ref_size = expected_sizes.front();
  std::cout << "源文件: " << src_file;
  if (level_random)
    std::cout << "  (level 随机采样于 [50..950], step=50)";
  std::cout << "  大小: " << ref_size << " bytes\n";
  std::cout << "任务总数: " << cfg.num_tasks << "  并发数: " << cfg.concurrency << '\n';
  std::cout << "读取方式: 顺序整文件加载\n";

  std::atomic<int> next_index{ 0 };
  std::atomic<int> success_cnt{ 0 };
  std::atomic<int> fail_cnt{ 0 };
  std::atomic<uint64_t> sum_bytes{ 0 };
  std::atomic<bool> fatal_error{ false };
  std::atomic<uint64_t> sum_latency_ns{ 0 };

  const auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.concurrency));

  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&]() {
      std::vector<char> buffer(1 << 20); // 1 MiB 类似 cat 的顺序读缓冲
      if (buffer.empty()) buffer.resize(4096);

      for (;;) {
        if (fatal_error.load(std::memory_order_relaxed))
          break;

        int i = next_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= cfg.num_tasks)
          break;

        const size_t idx = static_cast<size_t>(i);
        const fs::path& task_path = task_paths[idx];
        const uint64_t expected = expected_sizes[idx];

        const auto op_begin = std::chrono::steady_clock::now();

        uint64_t bytes_read = 0;
        if (!read_file_cat_style(task_path, buffer, expected, bytes_read)) {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
          sum_bytes.fetch_add(bytes_read, std::memory_order_relaxed);
          {
            std::lock_guard<std::mutex> lk(g_print_mutex);
            std::cerr << "[TASK " << i << "] (" << task_path << ") 读取未完成，期望 "
                      << expected << " bytes，实际 " << bytes_read << " bytes。\n";
          }
          fatal_error.store(true, std::memory_order_relaxed);
          break;
        }

        const auto op_end = std::chrono::steady_clock::now();
        uint64_t op_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());

        sum_bytes.fetch_add(bytes_read, std::memory_order_relaxed);
        success_cnt.fetch_add(1, std::memory_order_relaxed);
        sum_latency_ns.fetch_add(op_ns, std::memory_order_relaxed);

        {
          std::lock_guard<std::mutex> lk(g_print_mutex);
          std::cout << "[TASK " << i << "] (" << task_path << ") 全量读取完成，bytes="
            << bytes_read << '\n';
        }
      }
    });
  }

  for (auto& th : workers) th.join();

  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  const double seconds = ms / 1000.0;
  const double mib = sum_bytes.load() / (1024.0 * 1024.0);
  const double throughput_mibs = seconds > 0 ? (mib / seconds) : 0.0;

  std::cout << "\n====== 查询吞吐统计 ======\n";
  std::cout << "成功: " << success_cnt.load() << "  失败: " << fail_cnt.load() << "\n";
  std::cout << "总耗时: " << ms << " ms (" << std::fixed << std::setprecision(3) << seconds << " s)\n";
  std::cout << "读取总量: " << sum_bytes.load() << " bytes ("
            << std::fixed << std::setprecision(2) << mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2) << throughput_mibs << " MiB/s\n";
  const int ok = success_cnt.load();
  if (ok > 0) {
    const double avg_latency_ms =
      static_cast<double>(sum_latency_ns.load()) / 1e6 / static_cast<double>(ok);
    std::cout << "平均查询延迟(成功任务): " << std::fixed << std::setprecision(3)
      << avg_latency_ms << " ms\n";
  }

  if (fail_cnt.load() > 0 || fatal_error.load()) {
    std::cerr << "[WARN] 存在失败或异常终止的任务，请检查日志。\n";
  }

  return (fail_cnt.load() == 0 && !fatal_error.load()) ? 0 : 2;
}
