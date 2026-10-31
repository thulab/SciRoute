#include "iotdbfs/formats/grib/GribFileWriter.h"

#include "iotdbfs/Logging.h"
#include "iotdbfs/PathUtils.h"

#include <eccodes.h>

#include <chrono>
#include <cstdio>
#include "iotdbfs/Filesystem.h"
#include <fstream>
#include <vector>

namespace tools {

// namespace fs definition handled by Filesystem.h

namespace {

fs::path resolve_target_path(const fs::path& src, const fs::path& raw_target) {
  if (raw_target.empty())
    return src.filename();

  if (!raw_target.has_filename())
    return raw_target / src.filename();

  std::error_code ec;
  const bool target_exists = fs::exists(raw_target, ec);
  if (ec)
    return raw_target;

  if (target_exists) {
    ec.clear();
    if (fs::is_directory(raw_target, ec) && !ec)
      return raw_target / src.filename();
  }

  return raw_target;
}

fs::path make_temp_path(const fs::path& target) {
  const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
  fs::path tmp = target;
  tmp += ".tmp.";
  tmp += std::to_string(static_cast<long long>(now_ns));
  return tmp;
}

GribWriterStatus validate_grib_file(const fs::path& src) {
  FILE* fp = std::fopen(src.c_str(), "rb");
  if (!fp)
    return GribWriterStatus::SourceOpenFailed;
  int err = 0;
  codes_handle* handle = codes_handle_new_from_file(nullptr, fp,
    PRODUCT_GRIB, &err);
  if (!handle) {
    std::fclose(fp);
    return GribWriterStatus::NotAGribFile;
  }
  codes_handle_delete(handle);
  std::fclose(fp);
  return GribWriterStatus::Success;
}

void remove_if_exists(const fs::path& p) {
  std::error_code ec;
  fs::remove(p, ec);
}

std::string describe_status(GribWriterStatus st) {
  return grib_writer_status_message(st);
}

} // namespace

int write_grib_file(const std::string& source_path,
  const std::string& target_path) {
  if (source_path.empty() || target_path.empty()) {
    LOG_ERROR("write_grib_file: source or target path is empty");
    return static_cast<int>(GribWriterStatus::InvalidArgument);
  }

  fs::path src = fs::u8path(source_path);
  fs::path raw_target = fs::u8path(target_path);
  fs::path final_target = resolve_target_path(src, raw_target);

  std::error_code ec;
  if (!fs::exists(src, ec)) {
    LOG_ERROR("write_grib_file: source path not found: " + src.string());
    return static_cast<int>(GribWriterStatus::SourceNotFound);
  }
  if (ec) {
    LOG_ERROR("write_grib_file: error checking source path: " + src.string() +
      " : " + ec.message());
    return static_cast<int>(GribWriterStatus::SourceNotFound);
  }
  if (!fs::is_regular_file(src, ec) || ec) {
    LOG_ERROR("write_grib_file: source is not a regular file: " +
      src.string());
    return static_cast<int>(GribWriterStatus::SourceNotRegular);
  }

  if (!path_utils::has_grib_ext(src.string())) {
    LOG_WARN("write_grib_file: source file does not use a recognised GRIB "
      "extension: " + src.string());
  }

  GribWriterStatus validation = validate_grib_file(src);
  if (validation != GribWriterStatus::Success) {
    LOG_ERROR("write_grib_file: validation failed for source: " +
      src.string() + " (" + describe_status(validation) + ")");
    return static_cast<int>(validation);
  }

  fs::path target_dir = final_target.parent_path();
  if (!target_dir.empty()) {
    if (!fs::exists(target_dir, ec)) {
      if (!fs::create_directories(target_dir, ec) && ec) {
        LOG_ERROR("write_grib_file: failed to create target directory " +
          target_dir.string() + " : " + ec.message());
        return static_cast<int>(GribWriterStatus::EnsureTargetDirFailed);
      }
    }
    else if (ec) {
      LOG_ERROR("write_grib_file: failed to stat target directory " +
        target_dir.string() + " : " + ec.message());
      return static_cast<int>(GribWriterStatus::EnsureTargetDirFailed);
    }
  }

  std::ifstream in(src, std::ios::binary);
  if (!in.is_open()) {
    LOG_ERROR("write_grib_file: failed to open source for reading: " +
      src.string());
    return static_cast<int>(GribWriterStatus::SourceOpenFailed);
  }

  fs::path temp_path = make_temp_path(final_target);
  std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    LOG_ERROR("write_grib_file: failed to open target temp file for writing: " +
      temp_path.string());
    return static_cast<int>(GribWriterStatus::TargetOpenFailed);
  }

  std::vector<char> buffer(1 << 20);
  uintmax_t total_written = 0;
  while (true) {
    in.read(buffer.data(), buffer.size());
    std::streamsize got = in.gcount();
    if (got > 0) {
      out.write(buffer.data(), got);
      if (!out) {
        LOG_ERROR("write_grib_file: error writing to temp file " +
          temp_path.string());
        out.close();
        remove_if_exists(temp_path);
        return static_cast<int>(GribWriterStatus::TargetWriteFailed);
      }
      total_written += static_cast<uintmax_t>(got);
    }
    if (in.eof())
      break;
    if (in.fail()) {
      LOG_ERROR("write_grib_file: error while reading from source " +
        src.string());
      out.close();
      remove_if_exists(temp_path);
      return static_cast<int>(GribWriterStatus::SourceReadError);
    }
  }

  out.flush();
  if (!out) {
    LOG_ERROR("write_grib_file: failed to flush data to temp file " +
      temp_path.string());
    out.close();
    remove_if_exists(temp_path);
    return static_cast<int>(GribWriterStatus::TargetWriteFailed);
  }
  out.close();

  if (fs::exists(final_target, ec)) {
    if (ec) {
      LOG_ERROR("write_grib_file: failed to stat existing target " +
        final_target.string() + " : " + ec.message());
      remove_if_exists(temp_path);
      return static_cast<int>(GribWriterStatus::TargetFinalizeFailed);
    }
    ec.clear();
    const bool is_dir = fs::is_directory(final_target, ec);
    if (ec) {
      LOG_ERROR("write_grib_file: failed to inspect target path " +
        final_target.string() + " : " + ec.message());
      remove_if_exists(temp_path);
      return static_cast<int>(GribWriterStatus::TargetFinalizeFailed);
    }
    if (is_dir) {
      LOG_ERROR("write_grib_file: target path is a directory: " +
        final_target.string());
      remove_if_exists(temp_path);
      return static_cast<int>(GribWriterStatus::TargetFinalizeFailed);
    }
    ec.clear();
    if (!fs::remove(final_target, ec) && ec) {
      LOG_ERROR("write_grib_file: failed to remove existing target file " +
        final_target.string() + " : " + ec.message());
      remove_if_exists(temp_path);
      return static_cast<int>(GribWriterStatus::TargetFinalizeFailed);
    }
  }

  fs::rename(temp_path, final_target, ec);
  if (ec) {
    LOG_ERROR("write_grib_file: failed to move temp file into place: " +
      temp_path.string() + " -> " + final_target.string() + " : " +
      ec.message());
    remove_if_exists(temp_path);
    return static_cast<int>(GribWriterStatus::TargetFinalizeFailed);
  }

  LOG_INFO("write_grib_file: wrote GRIB file to " + final_target.string() +
    " (" + std::to_string(static_cast<long long>(total_written)) +
    " bytes)");
  return static_cast<int>(GribWriterStatus::Success);
}

const char* grib_writer_status_message(GribWriterStatus status) {
  switch (status) {
  case GribWriterStatus::Success:
    return "success";
  case GribWriterStatus::InvalidArgument:
    return "invalid argument";
  case GribWriterStatus::SourceNotFound:
    return "source not found";
  case GribWriterStatus::SourceNotRegular:
    return "source is not a regular file";
  case GribWriterStatus::SourceOpenFailed:
    return "failed to open source file";
  case GribWriterStatus::SourceReadError:
    return "failed to read source file";
  case GribWriterStatus::NotAGribFile:
    return "source is not a valid GRIB file";
  case GribWriterStatus::EnsureTargetDirFailed:
    return "failed to ensure target directory";
  case GribWriterStatus::TargetOpenFailed:
    return "failed to open target file";
  case GribWriterStatus::TargetWriteFailed:
    return "failed to write to target file";
  case GribWriterStatus::TargetFinalizeFailed:
    return "failed to finalise target file";
  default:
    return "unknown status";
  }
}

} // namespace tools
