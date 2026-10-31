#pragma once

#include "iotdbfs/FormatAdapter.h"

#include <memory>
#include <string>
#include <vector>

namespace iotdbfs {

class FormatRegistry {
public:
  bool register_adapter(std::shared_ptr<const FormatAdapter> adapter,
    std::string* error = nullptr);
  bool unregister_adapter(const std::string& id);
  std::shared_ptr<const FormatAdapter> resolve_by_id(
    const std::string& id) const;
  std::shared_ptr<const FormatAdapter> resolve_for_path(
    const std::string& path) const;
  std::shared_ptr<const FormatAdapter> resolve_for_content(
    const FormatSource& source) const;
  std::vector<std::string> ids() const;

private:
  std::vector<std::shared_ptr<const FormatAdapter>> adapters_;
};

} // namespace iotdbfs
