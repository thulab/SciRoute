#pragma once
#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/storage/versioning/VersionDirectory.h"

namespace iotdbfs::versioning {
struct NamespaceRecord {
  Inode inode;
  StructuredBlobManifest manifest;
  // File metadata is hydrated through these immutable references. Only
  // directories carry inline metadata in namespace-record/3.
  ContentId descriptor_id;
  ContentId seal_id;
};
Bytes encode_namespace_record(const NamespaceRecord&);
NamespaceRecord decode_namespace_record(const Bytes&);
} // namespace iotdbfs::versioning
