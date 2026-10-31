#include "iotdbfs/formats/grib/GribFileWriter.h"

#include <chrono>
#include "iotdbfs/Filesystem.h"
#include <iostream>
#include <string>

// namespace fs handled by Filesystem.h

namespace {

std::string usage(const char* prog) {
  return std::string("用法: ") + prog +
    " <源GRIB文件路径> [目标路径或目录]\n"
    "目标为挂载目录中的路径时，写入完成即可在挂载点直接访问。";
}

} // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << usage(argv[0]) << '\n';
    return static_cast<int>(tools::GribWriterStatus::InvalidArgument);
  }

  fs::path source = fs::u8path(argv[1]);
  if (!fs::exists(source)) {
    std::cerr << "源文件不存在: " << source << '\n';
    return static_cast<int>(tools::GribWriterStatus::SourceNotFound);
  }
  if (!fs::is_regular_file(source)) {
    std::cerr << "源文件不是普通文件: " << source << '\n';
    return static_cast<int>(tools::GribWriterStatus::SourceNotRegular);
  }

  fs::path raw_target = (argc >= 3)
    ? fs::u8path(argv[2])
    : source.filename();

  fs::path target = raw_target;
  if (target.empty())
    target = source.filename();

  std::cout << "写入 GRIB 文件:\n"
            << "  源文件: " << fs::absolute(source) << '\n'
            << "  目标:   " << fs::weakly_canonical(target) << '\n';

  int status = tools::write_grib_file(source.string(), target.string());
  if (status != static_cast<int>(tools::GribWriterStatus::Success)) {
    auto st = static_cast<tools::GribWriterStatus>(status);
    std::cerr << "写入失败: "
              << tools::grib_writer_status_message(st)
              << " (status=" << status << ")\n";
    return status;
  }

  std::cout << "写入成功。若目标位于挂载目录，例如 /mnt/gribfs，"
               "可立即在挂载点中查看该 GRIB 文件。\n";
  return 0;
}

