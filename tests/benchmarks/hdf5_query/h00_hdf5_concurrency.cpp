// H00 native-library concurrency diagnostic, not an IoTDB-FS benchmark.
// Same total H5Dread calls at each thread count; independent handles per
// worker.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <fstream>
#include <hdf5.h>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
struct Id {
  hid_t value;
  herr_t (*close)(hid_t);
  Id(hid_t v, herr_t (*c)(hid_t)) : value(v), close(c) {
    if (v < 0)
      throw std::runtime_error("HDF5 handle creation failed");
  }
  ~Id() { close(value); }
  operator hid_t() const { return value; }
  Id(const Id &) = delete;
};
void check(herr_t n) {
  if (n < 0)
    throw std::runtime_error("HDF5 call failed");
}
struct Trial {
  int threads;
  size_t reads;
  unsigned errors;
  double wall, cpu;
};
Trial trial(const std::string &path, int threads, size_t reads) {
  std::atomic<unsigned> errors{0}, ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> workers;
  for (int tid = 0; tid < threads; ++tid) {
    workers.emplace_back([&, tid] {
      try {
        check(H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr));
        Id file(H5Fopen(path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
        Id dataset(H5Dopen2(file, "/strain/Strain", H5P_DEFAULT), H5Dclose);
        Id space(H5Dget_space(dataset), H5Sclose);
        if (H5Sget_simple_extent_ndims(space) != 1)
          throw std::runtime_error("requires rank1 strain dataset");
        hsize_t extent = 0;
        check(H5Sget_simple_extent_dims(space, &extent, nullptr));
        const hsize_t start = 4090, count = 4096;
        if (extent < start + count)
          throw std::runtime_error("selection outside strain dataset");
        Id memory(H5Screate_simple(1, &count, nullptr), H5Sclose);
        check(H5Sselect_hyperslab(space, H5S_SELECT_SET, &start, nullptr,
                                  &count, nullptr));
        std::vector<double> data(count), oracle(count);
        check(H5Dread(dataset, H5T_NATIVE_DOUBLE, memory, space, H5P_DEFAULT,
                      oracle.data()));
        ++ready;
        while (!go.load())
          std::this_thread::yield();
        for (size_t i = tid; i < reads; i += threads) {
          check(H5Dread(dataset, H5T_NATIVE_DOUBLE, memory, space, H5P_DEFAULT,
                        data.data()));
          // Full equality check keeps output observable; validation cost is
          // included.
          if (data != oracle)
            throw std::runtime_error("repeated read values differ");
        }
      } catch (...) {
        ++errors;
        // Setup failure must not leave the coordinator waiting forever.
        if (!go.load())
          ++ready;
      }
    });
  }
  while (ready.load() != unsigned(threads))
    std::this_thread::yield();
  const auto cpu_start = std::clock();
  const auto start = std::chrono::steady_clock::now();
  go.store(true);
  for (auto &worker : workers)
    worker.join();
  const auto end = std::chrono::steady_clock::now();
  return {threads, reads, errors.load(),
          std::chrono::duration<double>(end - start).count(),
          double(std::clock() - cpu_start) / CLOCKS_PER_SEC};
}
} // namespace
int main(int argc, char **argv) {
  try {
    std::string path, output;
    size_t reads = 4096;
    for (int i = 1; i < argc; ++i) {
      std::string key = argv[i];
      if (++i == argc)
        throw std::runtime_error("missing option argument");
      if (key == "--file")
        path = argv[i];
      else if (key == "--output")
        output = argv[i];
      else if (key == "--reads")
        reads = std::stoull(argv[i]);
      else
        throw std::runtime_error("unknown option");
    }
    if (path.empty() || output.empty() || reads < 4 || reads > 65536)
      throw std::runtime_error(
          "requires --file H1.hdf5 --output JSON [--reads 4..65536]");
    check(H5PLset_loading_state(0));
    check(H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr));
    unsigned major = 0, minor = 0, release = 0;
    hbool_t safe = false;
    check(H5get_libversion(&major, &minor, &release));
    check(H5is_library_threadsafe(&safe));
    if (!safe)
      throw std::runtime_error("diagnostic requires a thread-safe build");
    std::vector<Trial> trials;
    // Rotate thread-count ordering; each worker warms its selection before
    // timing.
    for (int rep = 0; rep < 3; ++rep) {
      const int counts[] = {1, 2, 4};
      for (int k = 0; k < 3; ++k)
        trials.push_back(trial(path, counts[(k + rep) % 3], reads));
    }
    std::ofstream out(output);
    out << std::setprecision(12)
        << "{\"schema_version\":\"iotdbfs-hdf5-h00-concurrency/"
           "1\",\"library_version\":\""
        << major << '.' << minor << '.' << release
        << "\",\"threadsafe\":true,\"dataset\":\"/strain/"
           "Strain\",\"start\":[4090],\"count\":[4096],"
           "\"timing_scope\":\"warmed H5Dread plus value equality, worker "
           "close and join; excludes setup and warmup\","
           "\"cache_state\":\"warm native chunk cache and OS cache\","
           "\"interpretation\":\"diagnostic only; wall scaling does not "
           "establish API lock causality or backend performance\",\"trials\":[";
    unsigned errors = 0;
    for (size_t i = 0; i < trials.size(); ++i) {
      auto t = trials[i];
      errors += t.errors;
      if (i)
        out << ',';
      out << "{\"trial\":" << i / 3 << ",\"threads\":" << t.threads
          << ",\"total_reads\":" << t.reads
          << ",\"returned_bytes\":" << t.reads * 4096 * 8
          << ",\"errors\":" << t.errors << ",\"wall_seconds\":" << t.wall
          << ",\"process_cpu_seconds\":" << t.cpu << '}';
    }
    out << "],\"errors\":" << errors << "}\n";
    if (!out)
      throw std::runtime_error("cannot write report");
    return errors ? 1 : 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
