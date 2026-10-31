#pragma once

#include <string>

namespace iotdbfs::preflight {

enum class CheckStatus { Pass, Fail, Skip };

struct CheckResult {
  CheckStatus status;
  std::string check;
  std::string code;
  std::string message;
};

std::string status_name(CheckStatus status);
std::string to_json(const CheckResult& result);
std::string classify_iotdb_error(const std::string& message);
bool is_valid_identifier(const std::string& value);

} // namespace iotdbfs::preflight
