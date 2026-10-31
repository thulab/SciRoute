#pragma once

#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/MetadataSerialization.h"
#include "Common.h"

#include <vector>

namespace iotdbfs {

// Parse the stable, format-neutral inode projection used by filesystem
// queries. Both format columns are nullable values in the schema, but are
// present in every projection. A malformed row is rejected instead of being
// filled with defaults.
inline bool parse_inode_fields(const std::vector<Field>& fields, Inode& inode) {
  if (fields.size() != 15 || !fields[0].longV || !fields[1].stringV ||
    !fields[2].stringV || !fields[3].stringV || !fields[4].boolV ||
    !fields[5].longV || !fields[6].intV || !fields[7].intV ||
    !fields[8].intV || !fields[9].intV || !fields[10].longV ||
    !fields[11].longV || !fields[12].longV)
    return false;

  inode.row_time = fields[0].longV.value();
  inode.path = fields[1].stringV.value();
  inode.parent = fields[2].stringV.value();
  inode.name = fields[3].stringV.value();
  inode.is_dir = fields[4].boolV.value();
  inode.size = fields[5].longV.value();
  inode.mode = fields[6].intV.value();
  inode.uid = fields[7].intV.value();
  inode.gid = fields[8].intV.value();
  inode.nlink = fields[9].intV.value();
  inode.ctime = fields[10].longV.value();
  inode.mtime = fields[11].longV.value();
  inode.atime = fields[12].longV.value();
  if (!fields[13].stringV && (fields[13].longV || fields[13].intV ||
      fields[13].doubleV || fields[13].boolV))
    return false;
  if (!fields[14].stringV && (fields[14].longV || fields[14].intV ||
      fields[14].doubleV || fields[14].boolV))
    return false;
  inode.format_id = fields[13].stringV ? fields[13].stringV.value() : "";
  inode.metadata = fields[14].stringV ? fields[14].stringV.value() : "";
  if (!inode.metadata.empty()) {
    const auto metadata = deserialize_metadata(inode.metadata);
    if (!metadata.ok()) return false;
    // Byte pass-through versions have no format identity but still persist
    // the canonical empty typed document. Actual typed fields require an
    // adapter identity so their schema and semantics remain attributable.
    if (inode.format_id.empty() && !metadata.value().empty()) return false;
  }
  return true;
}

} // namespace iotdbfs
