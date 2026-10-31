#include <atomic>
#include <chrono>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

// namespace fs handled by Filesystem.h

struct Config {
  fs::path src_file = "/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  fs::path dst_dir = "/mnt/dfs/concurrent4";
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
  std::cout << "用法: " << prog << " [--src=FILE] [--dst=DIR] [--tasks=N] [--concurrency=N]\n"
            << "默认值: src=" << Config{}.src_file << " dst=" << Config{}.dst_dir
            << " tasks=" << Config{}.num_tasks << " concurrency=" << Config{}.concurrency << '\n';
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

    if (key == "src" || key == "source") {
      cfg.src_file = value;
    }
    else if (key == "dst" || key == "dir" || key == "dest") {
      cfg.dst_dir = value;
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

std::string shell_quote(const fs::path& path) {
  const std::string s = path.string();
  std::string quoted;
  quoted.reserve(s.size() + 2);
  quoted.push_back('\'');
  for (char ch : s) {
    if (ch == '\'') {
      quoted += "'\\''";
    }
    else {
      quoted.push_back(ch);
    }
  }
  quoted.push_back('\'');
  return quoted;
}

bool copy_with_cp(const fs::path& src, const fs::path& dst) {
  const std::string command = "cp " + shell_quote(src) + " " + shell_quote(dst);
  int ret = std::system(command.c_str());
  if (ret != 0) {
    std::cerr << "[ERR] cp 命令失败: " << command << " (返回值 " << ret << ")\n";
    return false;
  }
  return true;
}

} // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  // const fs::path dst_dir = "/data/qxj-data/concurrent3";

  // 准备目标目录
  std::error_code ec;
  if (!fs::exists(cfg.dst_dir) && !fs::create_directories(cfg.dst_dir, ec) && ec) {
    std::cerr << "[ERR] 创建目标目录失败: " << cfg.dst_dir << " : " << ec.message() << "\n";
    return 1;
  }

  if (!fs::exists(cfg.src_file)) {
    std::cerr << "[ERR] 源文件不存在: " << cfg.src_file << "\n";
    return 1;
  }

  uintmax_t src_size = 0;
  try {
    src_size = fs::file_size(cfg.src_file);
  }
  catch (const std::exception& e) {
    std::cerr << "[ERR] 获取源文件大小失败: " << e.what() << "\n";
    return 1;
  }

  std::cout << "源文件: " << cfg.src_file << "  大小: " << src_size << " bytes\n";
  std::cout << "目标目录: " << cfg.dst_dir << "\n";
  std::cout << "任务总数: " << cfg.num_tasks << "  并发数: " << cfg.concurrency << "\n";

  std::atomic<int> next_index{ 0 };
  std::atomic<int> success_cnt{ 0 };
  std::atomic<int> fail_cnt{ 0 };

  const auto t0 = std::chrono::steady_clock::now();

  // 启动线程
  std::vector<std::thread> workers;
  workers.reserve(cfg.concurrency);
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&]() {
      for (;;) {
        int i = next_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= cfg.num_tasks) break;

        fs::path dst_path = cfg.dst_dir / ("a_" + std::to_string(i) + ".grib2");
        bool ok = copy_with_cp(cfg.src_file, dst_path);
        if (ok) {
          success_cnt.fetch_add(1, std::memory_order_relaxed);
        }
        else {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
        }
      }
      });
  }

  // 等待完成
  for (auto& th : workers) th.join();

  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  // 统计
  const uint64_t total_bytes = static_cast<uint64_t>(success_cnt.load()) * static_cast<uint64_t>(src_size);
  const double seconds = ms / 1000.0;
  const double mib = total_bytes / (1024.0 * 1024.0);
  const double throughput_mibs = seconds > 0 ? (mib / seconds) : 0.0;

  std::cout << "\n====== 统计结果 ======\n";
  std::cout << "成功: " << success_cnt.load() << "  失败: " << fail_cnt.load() << "\n";
  std::cout << "总耗时: " << ms << " ms (" << std::fixed << std::setprecision(3) << seconds << " s)\n";
  std::cout << "写入总量: " << total_bytes << " bytes (" << std::fixed << std::setprecision(2) << mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2) << throughput_mibs << " MiB/s\n";

  if (fail_cnt.load() > 0) {
    std::cerr << "[WARN] 存在失败任务，请检查错误日志与目标磁盘/权限/配额。\n";
  }

  return fail_cnt.load() == 0 ? 0 : 2;
}
