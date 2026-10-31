#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

// namespace fs handled by Filesystem.h

static constexpr size_t kReadBufferBytes = 20ull * 1024 * 1024;

struct Config {
  std::vector<fs::path> src_files{
    "/mnt/dfs/paper_0_KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2"
  };
  int num_tasks = 100;
  int concurrency = 10;
};

namespace {

bool parse_positive_int(const std::string& text, int& value) {
  if (text.empty()) {
    return false;
  }

  try {
    const long parsed = std::stol(text);
    if (parsed <= 0 || parsed > std::numeric_limits<int>::max()) {
      return false;
    }
    value = static_cast<int>(parsed);
    return true;
  }
  catch (...) {
    return false;
  }
}

void print_usage(const char* prog) {
  const Config defaults;
  std::cout
    << "用法: " << prog << " [--src=FILE]... [--tasks=N] [--concurrency=N]\n"
    << "默认值:\n"
    << "  src=" << defaults.src_files.front() << "\n"
    << "  tasks=" << defaults.num_tasks << "\n"
    << "  concurrency=" << defaults.concurrency << "\n"
    << "说明: 可重复传入多个 --src，单个任务会按给定顺序依次完整读取这些文件。\n";
}

bool parse_args(int argc, char* argv[], Config& cfg) {
  bool saw_custom_src = false;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    }

    if (arg.rfind("--", 0) != 0) {
      std::cerr << "未知参数: " << arg << "\n";
      return false;
    }

    const auto eq = arg.find('=');
    if (eq == std::string::npos || eq <= 2) {
      std::cerr << "参数格式应为 --key=value: " << arg << "\n";
      return false;
    }

    const std::string key = arg.substr(2, eq - 2);
    const std::string value = arg.substr(eq + 1);

    if (key == "src" || key == "source" || key == "file") {
      if (!saw_custom_src) {
        cfg.src_files.clear();
        saw_custom_src = true;
      }
      cfg.src_files.emplace_back(value);
    }
    else if (key == "tasks") {
      if (!parse_positive_int(value, cfg.num_tasks)) {
        std::cerr << "无效的 tasks 数值: " << value << "\n";
        return false;
      }
    }
    else if (key == "concurrency" || key == "threads") {
      if (!parse_positive_int(value, cfg.concurrency)) {
        std::cerr << "无效的 concurrency 数值: " << value << "\n";
        return false;
      }
    }
    else {
      std::cerr << "未知参数: " << key << "\n";
      return false;
    }
  }

  return true;
}

std::mutex g_print_mutex;

bool read_file_cat_style(const fs::path& src, std::vector<char>& buffer,
  uint64_t expected_bytes, uint64_t& out_bytes) {
  const int fd = ::open(src.c_str(), O_RDONLY);
  if (fd < 0) {
    std::lock_guard<std::mutex> lock(g_print_mutex);
    std::cerr << "[ERR] 无法打开文件: " << src << " : " << std::strerror(errno) << "\n";
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
        std::lock_guard<std::mutex> lock(g_print_mutex);
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
      std::lock_guard<std::mutex> lock(g_print_mutex);
      std::cerr << "[ERR] 读取失败: " << std::strerror(errno) << "\n";
    }
    ::close(fd);
    return false;
  }
}

bool read_sources_cat_style(const std::vector<fs::path>& src_files,
  const std::vector<uint64_t>& expected_bytes_per_file,
  std::vector<char>& buffer, uint64_t& out_bytes) {
  out_bytes = 0;
  for (size_t i = 0; i < src_files.size(); ++i) {
    uint64_t file_bytes = 0;
    if (!read_file_cat_style(src_files[i], buffer, expected_bytes_per_file[i], file_bytes)) {
      out_bytes += file_bytes;
      return false;
    }
    out_bytes += file_bytes;
  }
  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg)) {
    print_usage(argv[0]);
    return 1;
  }

  if (cfg.src_files.empty()) {
    std::cerr << "[ERR] 未指定待读取文件。\n";
    return 1;
  }

  std::vector<uint64_t> expected_bytes_per_file;
  expected_bytes_per_file.reserve(cfg.src_files.size());
  uint64_t expected_bytes = 0;
  for (const auto& src_file : cfg.src_files) {
    if (src_file.empty()) {
      std::cerr << "[ERR] 存在空的待读取文件路径。\n";
      return 1;
    }
    if (!fs::exists(src_file)) {
      std::cerr << "[ERR] 文件不存在: " << src_file << "\n";
      return 1;
    }

    uintmax_t src_size = 0;
    try {
      src_size = fs::file_size(src_file);
    }
    catch (const std::exception& e) {
      std::cerr << "[ERR] 获取文件大小失败: " << src_file
                << " : " << e.what() << "\n";
      return 1;
    }

    if (src_size == 0) {
      std::cerr << "[WARN] 文件大小为 0，测试结果可能没有意义: "
                << src_file << "\n";
    }

    if (src_size > std::numeric_limits<size_t>::max()) {
      std::cerr << "[ERR] 文件过大，无法在当前架构下完整读取: "
                << src_file << "\n";
      return 1;
    }

    expected_bytes_per_file.push_back(static_cast<uint64_t>(src_size));
    expected_bytes += static_cast<uint64_t>(src_size);
  }

  std::cout << "读取文件数: " << cfg.src_files.size() << "\n";
  for (size_t i = 0; i < cfg.src_files.size(); ++i) {
    std::cout << "源文件[" << i << "]: " << cfg.src_files[i]
              << " (" << expected_bytes_per_file[i] << " bytes)\n";
  }
  std::cout << "单任务读取总量: " << expected_bytes << " bytes"
            << "  任务总数: " << cfg.num_tasks
            << "  并发数: " << cfg.concurrency << "\n";

  std::atomic<int> next_task{0};
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};
  std::atomic<uint64_t> total_bytes{0};
  std::atomic<uint64_t> total_latency_ns{0};
  std::atomic<bool> fatal_error{false};

  const auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.concurrency));
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&]() {
      std::vector<char> buffer(kReadBufferBytes);
      if (buffer.empty()) {
        buffer.resize(4096);
      }

      for (;;) {
        if (fatal_error.load(std::memory_order_relaxed)) {
          break;
        }

        const int task_index = next_task.fetch_add(1, std::memory_order_relaxed);
        if (task_index >= cfg.num_tasks) {
          break;
        }

        uint64_t bytes_read = 0;
        const auto op_begin = std::chrono::steady_clock::now();
        if (!read_sources_cat_style(
              cfg.src_files, expected_bytes_per_file, buffer, bytes_read)) {
          fail_count.fetch_add(1, std::memory_order_relaxed);
          total_bytes.fetch_add(bytes_read, std::memory_order_relaxed);
          fatal_error.store(true, std::memory_order_relaxed);
          {
            std::lock_guard<std::mutex> lock(g_print_mutex);
            std::cerr << "[ERR] 任务 " << task_index << " 读取未完成，期望 "
                      << expected_bytes << " bytes，实际 " << bytes_read << " bytes。\n";
          }
          break;
        }

        const auto op_end = std::chrono::steady_clock::now();
        const uint64_t latency_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());

        success_count.fetch_add(1, std::memory_order_relaxed);
        total_bytes.fetch_add(bytes_read, std::memory_order_relaxed);
        total_latency_ns.fetch_add(latency_ns, std::memory_order_relaxed);

      }
    });
  }

  for (auto& worker : workers) {
    worker.join();
  }

  const auto end = std::chrono::steady_clock::now();
  const auto elapsed_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  const double elapsed_seconds = elapsed_ms / 1000.0;
  const double total_mib = total_bytes.load() / (1024.0 * 1024.0);
  const double throughput_mib_s =
    elapsed_seconds > 0.0 ? total_mib / elapsed_seconds : 0.0;

  std::cout << "结果:\n";
  std::cout << "成功任务: " << success_count.load() << "\n";
  std::cout << "失败任务: " << fail_count.load() << "\n";
  std::cout << "总耗时: " << elapsed_ms << " ms (" << std::fixed << std::setprecision(3)
            << elapsed_seconds << " s)\n";
  std::cout << "读取总量: " << total_bytes.load() << " bytes (" << std::fixed
            << std::setprecision(2) << total_mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2) << throughput_mib_s
            << " MiB/s\n";

  if (success_count.load() > 0) {
    const double avg_latency_ms =
      static_cast<double>(total_latency_ns.load()) / 1e6 / success_count.load();
    std::cout << "平均查询延迟(成功任务): " << std::fixed << std::setprecision(3)
              << avg_latency_ms << " ms\n";
  }

  if (fail_count.load() > 0 || fatal_error.load()) {
    std::cerr << "[WARN] 存在失败或异常终止的任务，请检查读取路径和挂载状态。\n";
    return 2;
  }

  return 0;
}
