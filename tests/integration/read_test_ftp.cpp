#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// namespace fs handled by Filesystem.h

struct Config {
  std::string ftp_url;
  std::string ftp_user;
  std::string ftp_pass;
  int num_tasks = 50;
  int concurrency = 5;
  bool verbose = false;
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

void print_usage(const char* prog) {
  std::cout << "用法: " << prog
            << " --ftp-url=URL [--ftp-user=USER] [--ftp-pass=PASS]\n"
               "             [--tasks=N] [--concurrency=N] [--verbose=true|false]\n"
               "说明: 对给定的 FTP URL (指向 GRIB2 文件) 执行并发整文件读取，统计吞吐与平均延迟。\n";
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

    if (key == "ftp-url") {
      cfg.ftp_url = value;
    }
    else if (key == "ftp-user") {
      cfg.ftp_user = value;
    }
    else if (key == "ftp-pass") {
      cfg.ftp_pass = value;
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

bool fetch_entire_file(const Config& cfg, uint64_t& bytes) {
  std::string cmd = "curl -sS --fail ";
  if (!cfg.ftp_user.empty() || !cfg.ftp_pass.empty()) {
    cmd += "--user " + shell_quote(cfg.ftp_user + ":" + cfg.ftp_pass) + " ";
  }
  cmd += shell_quote(cfg.ftp_url);

  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    std::cerr << "[ERR] 无法启动 curl 命令: " << cmd << "\n";
    return false;
  }
  bytes = 0;
  std::array<char, 1 << 15> buffer{};
  while (true) {
    size_t n = fread(buffer.data(), 1, buffer.size(), pipe);
    if (n > 0)
      bytes += static_cast<uint64_t>(n);
    if (n < buffer.size()) {
      if (feof(pipe))
        break;
      if (ferror(pipe)) {
        std::cerr << "[ERR] 读取 curl 输出失败\n";
        pclose(pipe);
        return false;
      }
    }
    if (n == 0)
      break;
  }
  int ret = pclose(pipe);
  if (ret != 0) {
    std::cerr << "[ERR] curl 命令失败 (" << ret << ")\n";
    return false;
  }
  return true;
}

} // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!parse_args(argc, argv, cfg))
    return 1;

  if (cfg.ftp_url.empty()) {
    std::cerr << "[ERR] 必须指定 --ftp-url\n";
    return 1;
  }
  if (cfg.num_tasks <= 0 || cfg.concurrency <= 0) {
    std::cerr << "[ERR] tasks / concurrency 必须为正整数\n";
    return 1;
  }

  std::cout << "FTP 目标文件: " << cfg.ftp_url << "\n";
  std::cout << "任务总数: " << cfg.num_tasks << "  并发数: " << cfg.concurrency << "\n";

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
        int i = next_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= cfg.num_tasks)
          break;

        const auto op_begin = std::chrono::steady_clock::now();
        uint64_t bytes = 0;
        bool ok = fetch_entire_file(cfg, bytes);
        const auto op_end = std::chrono::steady_clock::now();
        uint64_t op_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count());

        if (ok) {
          success_cnt.fetch_add(1, std::memory_order_relaxed);
          sum_bytes.fetch_add(bytes, std::memory_order_relaxed);
          sum_latency_ns.fetch_add(op_ns, std::memory_order_relaxed);
          if (cfg.verbose) {
            std::lock_guard<std::mutex> lk(print_mu);
            std::cout << "[TASK " << i << "] bytes=" << bytes
                      << " latency=" << std::fixed << std::setprecision(3)
                      << (op_ns / 1e6) << " ms\n";
          }
        }
        else {
          fail_cnt.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lk(print_mu);
          std::cout << "[TASK " << i << "] 失败\n";
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

  std::cout << "\n====== FTP 整文件读取统计 ======\n";
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
    std::cerr << "[WARN] 存在失败任务，请检查 FTP 权限或网络连通性。\n";
  }
  return (fail_cnt.load() == 0) ? 0 : 2;
}

