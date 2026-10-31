#pragma once

#include "iotdbfs/Filesystem.h"

#include <cstdlib>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace test_support {

// Restore process state even when a fatal assertion exits a test early.
class ScopedEnvironment {
public:
  explicit ScopedEnvironment(std::string name) : name_(std::move(name)) {
    if (const char* value = std::getenv(name_.c_str())) original_ = value;
  }
  ~ScopedEnvironment() {
    if (original_) setenv(name_.c_str(), original_->c_str(), 1);
    else unsetenv(name_.c_str());
  }
  ScopedEnvironment(const ScopedEnvironment&) = delete;
  ScopedEnvironment& operator=(const ScopedEnvironment&) = delete;
  void set(const std::string& value) const {
    if (setenv(name_.c_str(), value.c_str(), 1) != 0)
      throw std::runtime_error("setenv failed");
  }
  void unset() const {
    if (unsetenv(name_.c_str()) != 0)
      throw std::runtime_error("unsetenv failed");
  }
private:
  std::string name_;
  std::optional<std::string> original_;
};

class TempDirectory {
public:
  TempDirectory() {
    std::string pattern = (fs::temp_directory_path() / "iotdbfs-ut-XXXXXX").string();
    const auto* result = mkdtemp(pattern.data());
    if (!result) throw std::runtime_error("mkdtemp failed");
    path_ = result;
  }
  ~TempDirectory() {
    std::error_code error;
    fs::remove_all(path_, error);
  }
  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;
  std::string file(const std::string& name) const { return (path_ / name).string(); }
  std::string write(const std::string& name, const std::string& content) const {
    const auto target = file(name);
    std::ofstream stream(target, std::ios::binary);
    stream << content;
    stream.close();
    if (!stream) throw std::runtime_error("test fixture write failed");
    return target;
  }
private:
  fs::path path_;
};

} // namespace test_support
