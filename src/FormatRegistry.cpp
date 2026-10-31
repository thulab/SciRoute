#include "iotdbfs/FormatRegistry.h"

#include <algorithm>
#include <cctype>

namespace iotdbfs {
namespace {
std::string extension(const std::string& path) {
  const auto slash = path.find_last_of("/\\");
  const auto dot = path.find_last_of('.');
  if (dot == std::string::npos || (slash != std::string::npos && dot < slash))
    return {};
  std::string value = path.substr(dot + 1);
  std::transform(value.begin(), value.end(), value.begin(),
    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}
}

bool FormatRegistry::register_adapter(
  std::shared_ptr<const FormatAdapter> adapter,
  std::string* error) {
  if (!adapter) {
    if (error) *error = "format adapter must not be null";
    return false;
  }
  if (adapter->id().empty()) {
    if (error) *error = "format id must not be empty";
    return false;
  }
  if (std::any_of(adapters_.begin(), adapters_.end(), [&](const auto& item) {
      return item->id() == adapter->id();
    })) {
    if (error) *error = "format id already registered";
    return false;
  }
  adapters_.push_back(std::move(adapter));
  return true;
}

bool FormatRegistry::unregister_adapter(const std::string& id) {
  const auto it = std::remove_if(adapters_.begin(), adapters_.end(),
    [&](const auto& item) { return item->id() == id; });
  if (it == adapters_.end()) return false;
  adapters_.erase(it, adapters_.end());
  return true;
}

std::shared_ptr<const FormatAdapter> FormatRegistry::resolve_by_id(
  const std::string& id) const {
  const auto it = std::find_if(adapters_.begin(), adapters_.end(),
    [&](const auto& item) { return item->id() == id; });
  return it == adapters_.end() ? nullptr : *it;
}

std::shared_ptr<const FormatAdapter> FormatRegistry::resolve_for_path(
  const std::string& path) const {
  const auto ext = extension(path);
  for (const auto& adapter : adapters_) {
    const auto& extensions = adapter->extensions();
    if (std::any_of(extensions.begin(), extensions.end(), [&](auto value) {
        if (!value.empty() && value.front() == '.') value.erase(value.begin());
        std::transform(value.begin(), value.end(), value.begin(),
          [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value == ext;
      })) return adapter;
  }
  return {};
}

std::shared_ptr<const FormatAdapter> FormatRegistry::resolve_for_content(
  const FormatSource& source) const {
  for (const auto& adapter : adapters_) {
    if (adapter->matches(source))
      return adapter;
  }
  return {};
}

std::vector<std::string> FormatRegistry::ids() const {
  std::vector<std::string> result;
  result.reserve(adapters_.size());
  for (const auto& adapter : adapters_) result.push_back(adapter->id());
  return result;
}
} // namespace iotdbfs
