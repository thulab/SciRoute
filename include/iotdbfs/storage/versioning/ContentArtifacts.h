#pragma once

#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/StructuredBlobStorage.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace iotdbfs::versioning {

inline constexpr const char* kContentRules = "iotdbfs-content-build/3";
inline constexpr std::size_t kContentChunkBytes = 60 * 1024;
std::string content_profile_key();
Bytes content_profile();

struct ContentBinding { ContentId descriptor_id, seal_id; };
struct FileDescriptor {
  std::string format;
  std::uint64_t payload_size{0}, layout_size{0};
  ContentId metadata_id, layout_id, proof_id;
  ContentId payload_root;
};
struct FormatProof {
  std::string rules{kContentRules}, format, layout_protocol, layout_digest;
  ContentId layout_id, catalog_id;
  std::uint64_t payload_size{0}, layout_size{0}, catalog_size{0};
  std::uint64_t records{0}, attributes{0};
  bool directory_complete{false}, attributes_complete{false};
};
struct VersionSeal {
  NamespaceIdentity identity;
  StructuredBlobVersionKey key;
  std::string format;
  std::uint64_t payload_size{0};
  ContentId descriptor_id, proof_id;
  MetadataMap legacy_envelope; // Only the five exact-version dataset_index fields.
};
Bytes encode_file_descriptor(const FileDescriptor&);
FileDescriptor decode_file_descriptor(const Bytes&);
Bytes encode_format_proof(const FormatProof&);
FormatProof decode_format_proof(const Bytes&);
Bytes encode_version_seal(const VersionSeal&);
VersionSeal decode_version_seal(const Bytes&);

struct VerifiedFileContent {
  ContentBinding binding;
  FileDescriptor descriptor;
  FormatProof proof;
  VersionSeal seal;
  MetadataMap metadata; // Reconstituted public manifest, including outer envelope.
};
// Called only on a cold identity before a proof is published. Implemented by
// the format adapter, not by the namespace or query core. Empty format/layout
// byte pass-through is handled by ContentArtifacts itself.
using LayoutValidator = std::function<StructuredBlobStatus(
  const std::string& format, const Bytes& layout, std::uint64_t payload_size)>;

struct ContentArtifactStats {
  std::uint64_t object_reads{0}, object_cache_hits{0}, hash_bytes{0};
  std::uint64_t metadata_parses{0}, catalog_parses{0}, layout_validations{0};
  std::uint64_t cache_bytes{0}, cache_entries{0}, evictions{0};
};

// One owner/namespace. A single bounded LRU accounts for encoded objects and
// successful metadata/catalog/proof results. A hit is never authorization.
// Cache locks never enclose Owner/Transaction calls, validators or remote I/O;
// concurrent cold misses may duplicate bounded validation work.
class ContentArtifacts {
public:
  explicit ContentArtifacts(std::shared_ptr<Owner>, LayoutValidator,
    std::size_t cache_bytes = 64ULL * 1024 * 1024);
  ~ContentArtifacts();
  ContentArtifacts(const ContentArtifacts&) = delete;
  ContentArtifacts& operator=(const ContentArtifacts&) = delete;

  ContentBinding build(Transaction&, const StructuredBlobManifest&,
    const Bytes& layout, const std::optional<DatasetCatalogArtifact>& catalog,
    const ContentId& payload_root);
  std::shared_ptr<const VerifiedFileContent> verify(const ContentBinding&,
    const StructuredBlobVersionKey&, const std::optional<std::string>& format,
    std::uint64_t payload_size, std::uint64_t layout_size,
    bool deep = false, std::uint64_t* shared_metadata_bytes = nullptr) const;
  Bytes read_layout(const VerifiedFileContent&, std::uint64_t offset,
    std::uint64_t length) const;
  std::shared_ptr<const DatasetCatalogArtifact> catalog(const VerifiedFileContent&) const;
  std::string layout_identity(const VerifiedFileContent&) const;
  ContentArtifactStats stats() const;
  void clear_cache();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace iotdbfs::versioning
