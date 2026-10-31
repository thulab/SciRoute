#include "iotdbfs/FormatAdapter.h"

namespace iotdbfs {

bool FormatAdapter::matches(const FormatSource&) const { return false; }
FormatStatus FormatAdapter::validate_layout_artifact(const std::vector<std::uint8_t>&,
    std::uint64_t) const {
  return FormatStatus::failure("format adapter has no immutable layout validator");
}

FormatQueryPtr FormatAdapter::parse_query(const std::string&) const {
  return {};
}

bool FormatAdapter::can_handle_virtual_query(const std::string&) const {
  return false;
}

FormatVirtualAttr FormatAdapter::get_virtual_attr(long long,
  const std::string&, const FormatQuery&, const std::string&) const {
  return {};
}

FormatOpenResult FormatAdapter::open_virtual(long long,
  const std::string&, const FormatQuery&, const std::string&) const {
  return {};
}

std::optional<std::string> FormatAdapter::get_xattr(long long,
  const std::string&, const Inode&, const std::string&) const {
  return std::nullopt;
}

FormatXattrList FormatAdapter::list_xattrs(long long,
  const std::string&, const Inode&) const {
  return {};
}

} // namespace iotdbfs
