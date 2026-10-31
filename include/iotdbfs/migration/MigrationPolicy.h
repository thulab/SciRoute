#pragma once

#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/StructuredBlobStorage.h"

#include <string>

namespace iotdbfs {

enum class MigrationKind {
  Candidate,
  HistoricalVersion,
  AlreadyIndexed,
  RequiresReingestion,
  Unsupported,
  OtherFormat,
  NotFormat,
  Error,
};

// Format-owned inspection. The common service owns source selection, hashing,
// publication and recovery. Partial queryability does not imply eligibility.
struct MigrationInspection {
  MigrationKind kind{MigrationKind::Error};
  std::string detail;
  std::string profile_json{"{}"};
  bool partially_queryable{false};
  // For candidates, digest of the newly derived layout; for indexed sources,
  // digest of the fully validated persisted layout (including all pages).
  std::string layout_sha256;
};

class ReingestionPolicy {
public:
  virtual ~ReingestionPolicy() = default;
  virtual std::string adapter_id() const = 0;
  virtual std::string layout_protocol() const = 0;
  // A format policy may explicitly rebuild an obsolete indexed version from
  // its immutable original bytes. inspect() still has to certify Candidate;
  // this flag alone never makes a current or foreign layout eligible.
  virtual bool supports_indexed_reingestion() const { return false; }
  virtual MigrationInspection inspect(const StructuredBlobReadHandle& source,
    const FormatRegistry& formats) const = 0;
};

std::string migration_json_quote(const std::string& text);
std::string migration_sha256(const StructuredBlobBytes& bytes);
bool migration_valid_sha256(const std::string& text);
const char* migration_kind_name(MigrationKind kind);

} // namespace iotdbfs
