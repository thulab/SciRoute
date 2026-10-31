#pragma once

#include "iotdbfs/PathUtils.h"

#include <cerrno>
#include <cstdint>
#include <limits>
#include <string>

namespace fuse_semantics {

inline bool is_root_path(const std::string& path) {
  return path_utils::norm(path) == "/";
}

inline bool is_strict_descendant_path(const std::string& path,
  const std::string& ancestor) {
  const std::string npath = path_utils::norm(path);
  const std::string nancestor = path_utils::norm(ancestor);
  if (npath == nancestor)
    return false;
  if (nancestor == "/")
    return npath.size() > 1 && npath[0] == '/';
  return npath.size() > nancestor.size() &&
    npath.compare(0, nancestor.size(), nancestor) == 0 &&
    npath[nancestor.size()] == '/';
}

inline int rename_target_error(bool source_is_dir, bool target_exists,
  bool target_is_dir, bool target_dir_empty) {
  if (!target_exists)
    return 0;
  if (source_is_dir && !target_is_dir)
    return -ENOTDIR;
  if (!source_is_dir && target_is_dir)
    return -EISDIR;
  if (source_is_dir && target_is_dir && !target_dir_empty)
    return -ENOTEMPTY;
  return 0;
}

inline bool valid_readdir_offset(off_t offset) {
  return offset >= 0;
}

inline bool readdir_cookie(std::size_t index, off_t& cookie) {
  if (index == std::numeric_limits<std::size_t>::max() ||
    index + 1 > static_cast<std::size_t>(std::numeric_limits<off_t>::max()))
    return false;
  cookie = static_cast<off_t>(index + 1);
  return true;
}

inline bool checked_add_u64(std::uint64_t lhs, std::uint64_t rhs,
  std::uint64_t& out) {
  if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs)
    return false;
  out = lhs + rhs;
  return true;
}

inline bool fits_off_t(std::uint64_t value) {
  return value <= static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
}

} // namespace fuse_semantics
