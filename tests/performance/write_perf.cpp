#include <atomic>
#include <chrono>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

// namespace fs handled by Filesystem.h

struct Config {
  enum class Mode { Cp, DdFsync };
  fs::path src_file = "/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2";
  fs::path target_dir = "/mnt/dfs";
  int num_tasks = 100;
  int concurrency = 10;
  std::string prefix = "paper_";
  Mode mode = Mode::Cp;
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
    << "用法: " << prog
    << " [--src=FILE] [--target-dir=DIR] [--tasks=N] [--concurrency=N] [--prefix=NAME]\n"
    << "默认值:\n"
    << "  src=" << defaults.src_file << "\n"
    << "  target-dir=" << defaults.target_dir << "\n"
    << "  tasks=" << defaults.num_tasks << "\n"
    << "  concurrency=" << defaults.concurrency << "\n"
    << "  prefix=" << defaults.prefix << "\n"
    << "  mode=cp\n";
}

bool parse_args(int argc, char* argv[], Config& cfg) {
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

    if (key == "src" || key == "source") {
      cfg.src_file = value;
    }
    else if (key == "target-dir" || key == "target" || key == "dst" || key == "dir") {
      cfg.target_dir = value;
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
    else if (key == "prefix") {
      cfg.prefix = value;
    }
    else if (key == "mode") {
      if (value == "cp") {
        cfg.mode = Config::Mode::Cp;
      }
      else if (value == "dd_fsync" || value == "dd-fsync") {
        cfg.mode = Config::Mode::DdFsync;
      }
      else {
        std::cerr << "无效的 mode: " << value << "\n";
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

std::string shell_quote(const std::string& text) {
  std::string quoted;
  quoted.reserve(text.size() + 2);
  quoted.push_back('\'');
  for (char ch : text) {
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
  const std::string command =
    "cp -- " + shell_quote(src.string()) + " " + shell_quote(dst.string());
  const int ret = std::system(command.c_str());
  if (ret != 0) {
    std::cerr << "[ERR] cp 命令失败: " << command << " (返回值 " << ret << ")\n";
    return false;
  }
  return true;
}

bool copy_with_dd_fsync(const fs::path& src, const fs::path& dst) {
  const std::string command =
    "dd if=" + shell_quote(src.string()) +
    " of=" + shell_quote(dst.string()) +
    " bs=1M conv=fsync status=none";
  const int ret = std::system(command.c_str());
  if (ret != 0) {
    std::cerr << "[ERR] dd 命令失败: " << command << " (返回值 " << ret << ")\n";
    return false;
  }
  return true;
}

fs::path build_output_path(const fs::path& target_dir, const fs::path& src_file,
  const std::string& prefix, int task_index) {
  const std::string stem = src_file.stem().string();
  const std::string ext = src_file.extension().string();
  const std::string numbered_name =
    prefix + stem + "_" + std::to_string(task_index) + ext;
  return target_dir / numbered_name;
}

}  // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg)) {
    print_usage(argv[0]);
    return 1;
  }

  if (!fs::exists(cfg.src_file)) {
    std::cerr << "[ERR] 源文件不存在: " << cfg.src_file << "\n";
    return 1;
  }

  std::error_code ec;
  if (!fs::exists(cfg.target_dir) && !fs::create_directories(cfg.target_dir, ec)) {
    std::cerr << "[ERR] 创建目标目录失败: " << cfg.target_dir << " : " << ec.message() << "\n";
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

  std::cout << "源文件: " << cfg.src_file << "\n";
  std::cout << "目标目录: " << cfg.target_dir << "\n";
  std::cout << "文件大小: " << src_size << " bytes"
            << "  任务总数: " << cfg.num_tasks
            << "  并发数: " << cfg.concurrency << "\n";

  std::atomic<int> next_task{0};
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};

  const auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  workers.reserve(cfg.concurrency);
  for (int t = 0; t < cfg.concurrency; ++t) {
    workers.emplace_back([&]() {
      for (;;) {
        const int task_index = next_task.fetch_add(1, std::memory_order_relaxed);
        if (task_index >= cfg.num_tasks) {
          break;
        }

        const fs::path dst_path = build_output_path(
          cfg.target_dir, cfg.src_file, cfg.prefix, task_index);
        const bool ok = cfg.mode == Config::Mode::DdFsync
          ? copy_with_dd_fsync(cfg.src_file, dst_path)
          : copy_with_cp(cfg.src_file, dst_path);
        if (ok) {
          success_count.fetch_add(1, std::memory_order_relaxed);
        }
        else {
          fail_count.fetch_add(1, std::memory_order_relaxed);
        }
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
  const uint64_t total_bytes =
    static_cast<uint64_t>(success_count.load()) * static_cast<uint64_t>(src_size);
  const double total_mib = total_bytes / (1024.0 * 1024.0);
  const double throughput_mib_s =
    elapsed_seconds > 0.0 ? total_mib / elapsed_seconds : 0.0;
  const double files_per_second =
    elapsed_seconds > 0.0 ? success_count.load() / elapsed_seconds : 0.0;

  std::cout << "结果:\n";
  std::cout << "成功任务: " << success_count.load() << "\n";
  std::cout << "失败任务: " << fail_count.load() << "\n";
  std::cout << "总耗时: " << elapsed_ms << " ms (" << std::fixed << std::setprecision(3)
            << elapsed_seconds << " s)\n";
  std::cout << "写入总量: " << total_bytes << " bytes (" << std::fixed << std::setprecision(2)
            << total_mib << " MiB)\n";
  std::cout << "平均吞吐: " << std::fixed << std::setprecision(2) << throughput_mib_s
            << " MiB/s\n";
  std::cout << "文件写入速率: " << std::fixed << std::setprecision(2) << files_per_second
            << " files/s\n";

  if (fail_count.load() > 0) {
    std::cerr << "[WARN] 存在失败任务，请检查目标目录权限、挂载状态和磁盘空间。\n";
    return 2;
  }

  return 0;
}
