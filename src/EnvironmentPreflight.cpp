#include "iotdbfs/EnvironmentPreflight.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <vector>

namespace iotdbfs::preflight {
namespace {

std::string escape_json(const std::string& value) {
  std::ostringstream out;
  for (unsigned char c : value) {
    switch (c) {
    case '\\': out << "\\\\"; break;
    case '"': out << "\\\""; break;
    case '\b': out << "\\b"; break;
    case '\f': out << "\\f"; break;
    case '\n': out << "\\n"; break;
    case '\r': out << "\\r"; break;
    case '\t': out << "\\t"; break;
    default:
      if (c < 0x20) {
        const char* hex = "0123456789abcdef";
        out << "\\u00" << hex[c >> 4] << hex[c & 0x0f];
      } else {
        out << static_cast<char>(c);
      }
    }
  }
  return out.str();
}

std::string lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

std::string redact_sensitive(std::string value) {
  const std::vector<std::string> keys{
    "password", "passwd", "secret", "token", "credential"};
  for (const auto& key : keys) {
    std::string lowered = lower(value);
    size_t search = 0;
    while ((search = lowered.find(key, search)) != std::string::npos) {
      const size_t after_key = search + key.size();
      if (search > 0 && std::isalnum(static_cast<unsigned char>(lowered[search - 1]))) {
        search = after_key;
        continue;
      }
      size_t delimiter = after_key;
      while (delimiter < value.size() &&
          std::isspace(static_cast<unsigned char>(value[delimiter]))) ++delimiter;
      if (delimiter >= value.size() ||
          (value[delimiter] != '=' && value[delimiter] != ':')) {
        search = after_key;
        continue;
      }
      size_t start = delimiter + 1;
      while (start < value.size() &&
          std::isspace(static_cast<unsigned char>(value[start]))) ++start;
      size_t end = start;
      while (end < value.size() && !std::isspace(static_cast<unsigned char>(value[end])) &&
          value[end] != ',' && value[end] != ';' && value[end] != ')' && value[end] != ']') ++end;
      value.replace(start, end - start, "<redacted>");
      search = start + std::string("<redacted>").size();
      lowered = lower(value);
    }
  }
  return value;
}

} // namespace

std::string status_name(CheckStatus status) {
  switch (status) {
  case CheckStatus::Pass: return "pass";
  case CheckStatus::Fail: return "fail";
  case CheckStatus::Skip: return "skip";
  }
  return "fail";
}

std::string to_json(const CheckResult& result) {
  const std::string safe_message = redact_sensitive(result.message);
  std::ostringstream out;
  out << "{\"check\":\"" << escape_json(result.check)
      << "\",\"status\":\"" << status_name(result.status)
      << "\",\"code\":\"" << escape_json(result.code)
      << "\",\"message\":\"" << escape_json(safe_message) << "\"}";
  return out.str();
}

std::string classify_iotdb_error(const std::string& message) {
  const std::string text = lower(message);
  if (text.find("906") != std::string::npos ||
      text.find("schemaregion") != std::string::npos ||
      text.find("schema region") != std::string::npos)
    return "IOTDB_SCHEMA_REGION_UNAVAILABLE";
  if (text.find("auth") != std::string::npos ||
      text.find("password") != std::string::npos ||
      text.find("login") != std::string::npos ||
      text.find("unauthorized") != std::string::npos ||
      text.find("permission denied") != std::string::npos)
    return "IOTDB_AUTH_FAILED";
  if (text.find("connect") != std::string::npos ||
      text.find("socket") != std::string::npos ||
      text.find("network") != std::string::npos ||
      text.find("reset by peer") != std::string::npos)
    return "IOTDB_SESSION_FAILED";
  return "IOTDB_CHECK_FAILED";
}

bool is_valid_identifier(const std::string& value) {
  if (value.empty())
    return false;
  const auto letter = [](unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  };
  if (!letter(static_cast<unsigned char>(value.front())))
    return false;
  return std::all_of(value.begin() + 1, value.end(), [](unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c == '_';
  });
}

} // namespace iotdbfs::preflight
