#include "iotdbfs/storage/versioning/detail/PayloadBlockFlights.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"

#include "SessionDataSet.h"
#include "TableSession.h"
#include "iotdbfs/InodeRowParser.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/IoTDBDatasetIndexValidation.h"
#include "iotdbfs/IoTDBPublishedCatalogSql.h"
#include "iotdbfs/Logging.h"
#include "iotdbfs/MetadataSetIndex.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/PublicationProfile.h"
#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/frontend/CatalogSourceAuthorization.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryDomainCache.h"
#include "iotdbfs/formats/hdf5/Hdf5GlobalAttributeIndex.h"
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/storage/versioning/NamespaceSelection.h"
#include <cctype>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <map>
#include <set>
#include <type_traits>

namespace iotdbfs {
namespace {

// Time only the DataSet method, including any result fetch it performs.
// Preserve the original return type and propagate failures unchanged.
template<class DataSet>
bool publication_has_next(const DataSet& rows) {
  if (!active_publication_profile.record) return rows->hasNext();
  PublicationOperationTimer timer(PublicationOperation::QueryFetch);
  try {
    const auto result = rows->hasNext();
    timer.finish(true);
    return result;
  } catch (...) {
    timer.finish(false);
    throw;
  }
}

template<class DataSet>
auto publication_next(const DataSet& rows) -> decltype(rows->next()) {
  if (!active_publication_profile.record) return rows->next();
  PublicationOperationTimer timer(PublicationOperation::QueryFetch);
  try {
    decltype(auto) result = rows->next();
    timer.finish(true);
    return result;
  } catch (...) {
    timer.finish(false);
    throw;
  }
}

constexpr const char* kInodeProjection =
  "time,path,parent,name,is_dir,size,mode,uid,gid,nlink,ctime,mtime,atime,"
  "format_id,metadata";

// Build-only ablation switch. Clients cannot alter authorization query shape.
constexpr bool kCombineExactCatalogAncestors = true;

std::string escape_sql(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size() + 8);
  for (const char c : value) {
    if (c == '\'') escaped += "''";
    else escaped.push_back(c);
  }
  return escaped;
}

StructuredBlobStatus backend_failure(std::string message) {
  return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
    std::move(message));
}

StructuredBlobStatus validate_manifest_write_bounds(const StructuredBlobManifest& next) {
  if (next.revision > static_cast<std::uint64_t>(std::numeric_limits<long long>::max()) ||
      next.payload_size > static_cast<std::uint64_t>(std::numeric_limits<long long>::max()) ||
      next.layout_size > static_cast<std::uint64_t>(std::numeric_limits<long long>::max()))
    return backend_failure("structured blob manifest value exceeds INT64");
  return StructuredBlobStatus::success();
}

template<typename T>
StructuredBlobResult<T> backend_failure_result(std::string message) {
  return StructuredBlobResult<T>::failure(StructuredBlobError::BackendFailure,
    std::move(message));
}

StructuredBlobStatus validate_shared_payload_ranges(std::uint64_t size,
    const StructuredBlobPayloadRanges& ranges) {
  if (ranges.empty() || ranges.size() > kMaxPayloadRangesPerBatch)
    return StructuredBlobStatus::failure(StructuredBlobError::RangeError,
      "payload batch range count is outside the supported bounds");
  std::uint64_t total = 0;
  for (const auto& range : ranges) {
    if (!range.length || range.offset > size || range.length > size - range.offset ||
        range.length > kMaxPayloadBytesPerBatch - total)
      return StructuredBlobStatus::failure(StructuredBlobError::RangeError,
        "payload batch range is outside the published payload or batch budget");
    total += range.length;
  }
  return StructuredBlobStatus::success();
}

std::string payload_source_identity(const StructuredBlobVersionKey& key) {
  return std::to_string(key.path.size()) + ":" + key.path + ":" + std::to_string(key.version);
}

void validate_native_file_binding(const NativeFileLocation& location,
    const versioning::VerifiedFileContent& verified) {
  if (!(location.namespace_identity == verified.seal.identity) ||
      !(location.key == verified.seal.key) ||
      location.descriptor_id != verified.binding.descriptor_id ||
      location.seal_id != verified.binding.seal_id ||
      location.payload_root != verified.descriptor.payload_root ||
      location.size != verified.descriptor.payload_size || verified.descriptor.format != "hdf5")
    throw std::runtime_error("native file locator differs from verified exact content");
}

NativeFileLocation native_file_identity(const versioning::VerifiedFileContent& verified) {
  NativeFileLocation location;
  location.namespace_identity = verified.seal.identity;
  location.key = verified.seal.key;
  location.descriptor_id = verified.binding.descriptor_id;
  location.seal_id = verified.binding.seal_id;
  location.payload_root = verified.descriptor.payload_root;
  location.size = verified.descriptor.payload_size;
  return location;
}

// Process-issued content capability. It deliberately owns artifact services,
// never its backend, so a copied manifest cannot create a backend reference cycle.
class OwnerPublishedPayloadReader final : public StructuredBlobPayloadReader {
public:
  OwnerPublishedPayloadReader(std::uint64_t issuer, const StructuredBlobManifest& manifest,
      std::shared_ptr<versioning::Owner> owner,
      std::shared_ptr<versioning::ContentArtifacts> content,
      std::shared_ptr<versioning::PayloadArtifacts> payload,
      std::shared_ptr<const versioning::VerifiedFileContent> verified,
      std::shared_ptr<NativeFileStore> native_store,
      std::optional<NativeFileLocation> native_location)
    : issuer_(issuer), key_(manifest.key), revision_(manifest.revision),
      owner_(std::move(owner)), content_(std::move(content)), payload_(std::move(payload)),
      verified_(std::move(verified)), native_store_(std::move(native_store)),
      native_location_(std::move(native_location)) {}
  bool matches(std::uint64_t issuer, const StructuredBlobManifest& manifest) const {
    return issuer == issuer_ && manifest.key == key_ && manifest.revision == revision_ &&
      manifest.published && manifest.phase == StructuredBlobPhase::Published &&
      manifest.payload_size == verified_->descriptor.payload_size &&
      manifest.layout_size == verified_->descriptor.layout_size &&
      manifest.format_id.value_or("") == verified_->descriptor.format;
  }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    using Result = StructuredBlobResult<StructuredBlobPayloadBatch>;
    try {
      (void)owner_->view(); // A frozen owner cannot be hidden by a pinned root.
      const auto valid = validate_shared_payload_ranges(verified_->descriptor.payload_size, ranges);
      if (!valid.ok()) return Result::failure(valid.code(), valid.message());
      return Result::success(payload_->read_ranges(verified_->descriptor.payload_root,
        verified_->descriptor.payload_size, ranges, payload_source_identity(key_)));
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobPayloadBatch>(error.what()); }
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      std::uint64_t offset, std::uint64_t length) const override {
    try {
      (void)owner_->view();
      return StructuredBlobResult<StructuredBlobBytes>::success(content_->read_layout(*verified_, offset, length));
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobBytes>(error.what()); }
  }
  StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const override {
    using Result = StructuredBlobResult<std::optional<std::string>>;
    try {
      (void)owner_->view();
      require_query_metadata("layout-demand:" + verified_->seal.identity.namespace_id +
        verified_->seal.identity.incarnation + ":" + std::to_string(key_.path.size()) + ":" + key_.path + ":" +
        std::to_string(key_.version) + ":" + verified_->descriptor.layout_id, verified_->descriptor.layout_size);
      return Result::success(content_->layout_identity(*verified_));
    } catch (const std::exception& error) { return backend_failure_result<std::optional<std::string>>(error.what()); }
  }
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    return bind_native_file({});
  }
  StructuredBlobResult<std::optional<std::string>> native_file_cache_identity() const override {
    using Result = StructuredBlobResult<std::optional<std::string>>;
    try {
      const auto head = owner_->view().head;
      if (!native_store_ || !native_location_)
        return Result::failure(StructuredBlobError::InvalidState,
          "exact version has no configured native filesystem binding; explicit binding is required");
      validate_native_file_binding(*native_location_, *verified_);
      auto location = native_store_->cache_identity(*native_location_);
      if (!location.ok()) return Result::failure(location.status().code(), location.status().message());
      auto bytes = versioning::encode_head(head);
      std::string identity(bytes.begin(), bytes.end());
      identity += location.value();
      return Result::success(std::move(identity));
    } catch (const std::exception& error) {
      return backend_failure_result<std::optional<std::string>>(error.what());
    }
  }
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
      const std::shared_ptr<const NativeFileDescriptor>& cached) const override {
    return bind_native_file(cached);
  }
private:
  StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> bind_native_file(
      const std::shared_ptr<const NativeFileDescriptor>& cached) const {
    using Result = StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>;
    try {
      (void)owner_->view();
      if (!native_store_ || !native_location_)
        return Result::failure(StructuredBlobError::InvalidState,
          "exact version has no configured native filesystem binding; explicit binding is required");
      validate_native_file_binding(*native_location_, *verified_);
      auto opened = cached ? native_store_->reuse(*native_location_, cached) :
        native_store_->open(*native_location_);
      if (!opened.ok()) return opened;
      return Result::success(opened.value()->with_read_guard([owner = owner_] {
        try {
          (void)owner->view(); // Same frozen-owner check as database range reads.
          return StructuredBlobStatus::success();
        } catch (const std::exception& error) { return backend_failure(error.what()); }
      }));
    } catch (const std::exception& error) {
      return backend_failure_result<std::shared_ptr<const NativeFileHandle>>(error.what());
    }
  }
  const std::uint64_t issuer_;
  const StructuredBlobVersionKey key_;
  const std::uint64_t revision_;
  std::shared_ptr<versioning::Owner> owner_;
  std::shared_ptr<versioning::ContentArtifacts> content_;
  std::shared_ptr<versioning::PayloadArtifacts> payload_;
  std::shared_ptr<const versioning::VerifiedFileContent> verified_;
  std::shared_ptr<NativeFileStore> native_store_;
  const std::optional<NativeFileLocation> native_location_;
};

StructuredBlobResult<StructuredBlobManifest> not_found_result(
  std::string message) {
  return StructuredBlobResult<StructuredBlobManifest>::failure(
    StructuredBlobError::NotFound, std::move(message));
}

bool legacy_execute_nonquery(const std::string& sql) {
  try {
    return IoTDBConnector::instance().executeNonQuery(sql);
  } catch (const std::exception& error) {
    LOG_ERROR("structured blob SQL failed: " +
      redact_sql_for_log(sql) + " => " + error.what());
    return false;
  }
}

std::shared_ptr<SessionDataSet> legacy_execute_query(const std::string& sql, bool metadata = true) {
  // The API duration includes pool acquisition and initial query submission;
  // subsequent SessionDataSet fetch/close time is measured by read observers.
  if (metadata) iotdb_profile_counters.metadata_query_calls.fetch_add(1, std::memory_order_relaxed);
  std::optional<IoTDBProfileTimer> timer;
  if (metadata) timer.emplace(iotdb_profile_counters.metadata_query_nanoseconds);
  try {
    return IoTDBConnector::instance().executeQuery(sql);
  } catch (const std::exception& error) {
    LOG_ERROR("structured blob query failed: " +
      redact_sql_for_log(sql) + " => " + error.what());
    return nullptr;
  }
}

std::string key_predicate(const StructuredBlobVersionKey& key) {
  return "time=" + std::to_string(key.version) + " AND path='" +
    escape_sql(path_utils::norm(key.path)) + "'";
}

int phase_value(StructuredBlobPhase phase) {
  return static_cast<int>(phase);
}

std::optional<StructuredBlobPhase> parse_phase(int value) {
  if (value < phase_value(StructuredBlobPhase::PayloadOpen) ||
      value > phase_value(StructuredBlobPhase::Published))
    return std::nullopt;
  return static_cast<StructuredBlobPhase>(value);
}

} // namespace

namespace {
const std::set<std::string> kPhysicalTables = {"inode", "blob_payload", "blob_layout",
  "blob_metadata_set", "blob_version_identity", "blob_dataset_catalog",
  "blob_dataset_entry", "blob_dataset_attribute"};
std::string current_format_key(const Inode& inode) {
  return inode.path + std::string(1, '\0') + inode.format_id;
}
// Identity/permission reads must not dereference content before authorization.
// File metadata is deliberately absent here; content readers use the verified
// manifest path after they have selected and authorized an exact version.
versioning::NamespaceRecord namespace_identity(const versioning::Bytes& bytes) {
  auto record = versioning::decode_namespace_record(bytes);
  if (!record.inode.is_dir) {
    record.inode.metadata = "{}";
    record.manifest.metadata_materialized = false;
  }
  return record;
}
}

struct VersionDirectorySnapshot {
  versioning::View view;
  std::map<StructuredBlobVersionKey, versioning::NamespaceRecord> records;
  std::vector<StructuredBlobVersionKey> sources;
  std::map<std::string, StructuredBlobVersionKey> latest;
  std::uint64_t logical_bytes{0};
  std::vector<std::pair<std::string,std::uint64_t>> evidence;
  QueryMetadataTrace directory_fees;
};

struct HotCatalogBuild {
  std::shared_ptr<HotQueryCache> pool;
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  CatalogSelection selection;
  std::vector<StructuredBlobManifest> manifests;
  std::vector<std::shared_ptr<const DatasetCatalogArtifact>> catalogs;
  QueryMetadataTrace selection_fees, manifest_fees, catalog_fees;
  std::uint64_t catalog_bytes{0};
  bool cacheable{true};
  std::unique_ptr<ScopedQueryMetadataTrace> capture;
  void begin() {
    if(!cacheable)return;
    try {capture=std::make_unique<ScopedQueryMetadataTrace>("",[this](std::uint64_t bytes){
      if(!reservation->grow(bytes))throw std::bad_alloc();
    },QueryMetadataTracePolicy::CacheStable);}catch(...){cacheable=false;}
  }
  void end(QueryMetadataTrace& fees) {
    if(!capture)return;
    if(capture->failed())cacheable=false;
    if(cacheable)fees=capture->release();
    capture.reset();
  }
};

namespace {
using NamespaceEvidence = std::pair<std::string,std::uint64_t>;
struct HotCandidateDomain;
// One child's allocation is refundable independently of its resident backing.
// The reservation itself keeps retired backing bytes and its domain slot live.
struct HotChildCharge {
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  std::mutex mutex;
  std::uint64_t bytes{0};
  ~HotChildCharge(){if(bytes)reservation->shrink(bytes);}
  static std::shared_ptr<HotChildCharge> make(
      const std::shared_ptr<HotQueryCache::Reservation>& reservation,std::uint64_t bytes) {
    if(!reservation||!reservation->grow(bytes))return {};
    try {
      auto result=std::make_shared<HotChildCharge>();result->reservation=reservation;
      result->bytes=bytes;return result;
    }catch(...){reservation->shrink(bytes);throw;}
  }
  bool grow(std::uint64_t amount) {
    // Pool pressure may retire another Allocation backed by this child. Let
    // that refund run before taking the short child-accounting mutex.
    if(!reservation->grow(amount))return false;
    std::lock_guard<std::mutex> lock(mutex);
    bytes+=amount;return true;
  }
  void shrink(std::uint64_t amount) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    if(amount>bytes)return;
    reservation->shrink(amount);bytes-=amount;
  }
};
NamespaceEvidence namespace_evidence(const versioning::NamespaceIdentity& identity,
    versioning::Table table,const std::string& key,const versioning::Bytes& bytes) {
  return {"namespace:"+identity.namespace_id+identity.incarnation+":"+
    std::to_string(static_cast<unsigned>(table))+":"+std::to_string(key.size())+":"+key+":"+
    versioning::content_id("query-record","iotdbfs-query-record/1",bytes),key.size()+bytes.size()};
}
void observe_namespace_evidence(const NamespaceEvidence& evidence) {
  require_query_metadata(evidence.first,evidence.second);
}
void context_charge(std::uint64_t& used, std::uint64_t bytes) {
  if (used > versioning::kMetadataBytes || bytes > versioning::kMetadataBytes-used)
    throw versioning::Error(versioning::ErrorCode::Limit, "read context metadata budget exceeded");
  used += bytes;
}
QueryStatus context_failure(const std::exception& error) {
  const auto* typed = dynamic_cast<const versioning::Error*>(&error);
  return QueryStatus::failure(dynamic_cast<const QueryMetadataBudgetExceeded*>(&error) ||
    (typed && typed->code()==versioning::ErrorCode::Limit)
    ? QueryError::ResourceExhausted : QueryError::ReadFailure, QueryPhase::Catalog, error.what());
}
bool same_content_binding(const versioning::NamespaceRecord& a, const versioning::NamespaceRecord& b) {
  return a.manifest.key==b.manifest.key && !b.inode.is_dir &&
    a.manifest.format_id==b.manifest.format_id && a.manifest.revision==b.manifest.revision &&
    a.manifest.payload_size==b.manifest.payload_size && a.manifest.layout_size==b.manifest.layout_size &&
    a.descriptor_id==b.descriptor_id && a.seal_id==b.seal_id;
}
class DirectoryExecutionLease final : public QueryExecutionLease {
public:
  DirectoryExecutionLease(std::shared_ptr<versioning::Owner> owner,
      std::shared_ptr<const VersionDirectorySnapshot> snapshot,std::shared_ptr<const void> hot_pin={})
    : hot_pin_(std::move(hot_pin)),owner_(std::move(owner)), snapshot_(std::move(snapshot)), token_(owner_->acquire_execution_lease()) {}
private:
  std::shared_ptr<const void> hot_pin_;
  std::shared_ptr<versioning::Owner> owner_;
  std::shared_ptr<const VersionDirectorySnapshot> snapshot_;
  std::shared_ptr<void> token_;
};
class DirectoryAdmission final : public QueryAdmission {
public:
  DirectoryAdmission(std::shared_ptr<versioning::Owner> owner,
      std::shared_ptr<const VersionDirectorySnapshot> snapshot,std::shared_ptr<const void> hot_pin={})
    : hot_pin_(std::move(hot_pin)),owner_(std::move(owner)), snapshot_(std::move(snapshot)) {}
  ~DirectoryAdmission() override {
    gate_.reset(); // Large prior facts must be released after admission ends.
    retired_facts_.reset();previous_facts_.reset();
  }
  void complete(std::unique_ptr<versioning::Owner::Admission> gate,
      std::shared_ptr<const void> retired,std::shared_ptr<const void> previous) noexcept {
    gate_=std::move(gate);retired_facts_=std::move(retired);previous_facts_=std::move(previous);
  }
  std::shared_ptr<QueryExecutionLease> execution_lease() override {
    if (!lease_) lease_=std::make_shared<DirectoryExecutionLease>(owner_,snapshot_,hot_pin_);
    return lease_;
  }
private:
  std::shared_ptr<const void> hot_pin_;
  std::shared_ptr<versioning::Owner> owner_;
  std::shared_ptr<const VersionDirectorySnapshot> snapshot_;
  std::unique_ptr<versioning::Owner::Admission> gate_;
  std::shared_ptr<const void> retired_facts_,previous_facts_;
  std::shared_ptr<QueryExecutionLease> lease_;
};
}

class VersionDirectoryReadContext final : public QueryReadContext {
  // The domain charge must outlive snapshot/facts and all their containers.
  mutable std::shared_ptr<const void> retained_charge_;
  mutable std::shared_ptr<HotQueryCache::Reservation> hot_reservation_;
  struct Facts {
    versioning::View view;
    std::vector<versioning::NamespaceRecord> records;
    std::map<std::string,std::optional<Inode>> parents;
    std::vector<NamespaceEvidence> record_evidence;
    std::map<std::string,NamespaceEvidence> parent_evidence;
  };
  struct HotProof {
    std::shared_ptr<HotQueryCache::Reservation> reservation;
    versioning::Head head;
    std::shared_ptr<const PreparedQueryMetadataTrace> fees;
    // A summary holds this proof's fee trace (and therefore its Proof charge).
    // Drop that optional cache owner when the last proof pin disappears; live
    // ledger/summary pins continue to retain their unchanged reservation.
    mutable std::weak_ptr<HotQueryCache::RetainedSlot> summary_slot;
    ~HotProof(){if(auto slot=summary_slot.lock())slot->clear();}
  };
  struct HotProofSlot {
    std::shared_ptr<HotQueryCache::RetainedSlot> proof;
    bool building{false};
    std::shared_ptr<HotQueryCache::RetainedSlot> summary;
    bool summary_building{false},summary_attempted{false};
  };
  // A child charge refunds just the optional plan's bytes. It pins the same
  // shared 64 MiB pool as its proof, including after eviction, without owning
  // a proof, context or domain and therefore cannot create a reference cycle.
  struct HotSummaryCharge {
    static constexpr std::uint64_t wrapper_bytes=512;
    std::shared_ptr<HotQueryCache::Reservation> reservation;
    std::uint64_t bytes{wrapper_bytes};
    ~HotSummaryCharge(){if(bytes)reservation->shrink(bytes);}
    void grow(std::uint64_t amount) {
      if(!reservation->grow(amount))throw std::bad_alloc();
      bytes+=amount;
    }
    void shrink(std::uint64_t amount) noexcept {
      reservation->shrink(amount);bytes-=amount;
    }
  };
  struct HotProofBuild {
    const VersionDirectoryReadContext* context;
    std::string principal;
    ~HotProofBuild(){if(context)context->finish_hot_proof(principal);}
  };
public:
  VersionDirectoryReadContext(std::weak_ptr<const IoTDBStructuredBlobBackend> backend,
      std::shared_ptr<const VersionDirectorySnapshot> snapshot)
    : backend_(std::move(backend)), snapshot_(std::move(snapshot)) {}
  ~VersionDirectoryReadContext() override {
    // Empty but still-accounted cache slots need not match the live Proof
    // count. Remove one key while its management charge is still pinned,
    // without allocating a second container during destruction.
    while(!hot_proofs_.empty()) {
      auto found=hot_proofs_.begin();
      auto proof=std::move(found->second.proof),summary=std::move(found->second.summary);
      hot_proofs_.erase(found);
      if(!hot_reference_only_) {
        // A completed result shares only this optional slot. Retiring its
        // planning owner need not keep an obsolete Proof alive until TTL.
        if(summary)summary->clear();
        if(proof)proof->clear();
      }
    }
  }
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>& candidates) const override {
    using Result=QueryResult<std::shared_ptr<const QueryReadContext>>;
    try {
      if (selected_) return Result::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Catalog,"a frozen context cannot drop candidates"));
      auto result=std::make_shared<VersionDirectoryReadContext>(backend_,snapshot_);
      auto facts=std::make_shared<Facts>();facts->view=snapshot_->view;
      std::set<StructuredBlobVersionKey> seen;
      std::uint64_t used=0;
      for (const auto& m:candidates) {
        const auto it=snapshot_->records.find(m.key);
        if (it==snapshot_->records.end() || it->second.inode.is_dir || !seen.insert(m.key).second ||
            m.format_id!=it->second.manifest.format_id || m.revision!=it->second.manifest.revision ||
            m.payload_size!=it->second.manifest.payload_size || m.layout_size!=it->second.manifest.layout_size)
          return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"candidate is not from captured namespace"));
        const auto& r=it->second;facts->records.push_back(r);
        const auto encoded=versioning::encode_namespace_record(r);
        context_charge(used,encoded.size());
        facts->record_evidence.push_back(namespace_evidence(facts->view.head.identity,
          versioning::Table::Exact,versioning::exact_key(m.key.path,m.key.version),encoded));
        observe_namespace_evidence(facts->record_evidence.back());
        result->sources_.push_back({m.key.path,m.key.version,m.format_id.value_or("")});
        const auto key=versioning::exact_key(m.key.path,m.key.version);
        for (auto table:{versioning::Table::Exact,versioning::Table::Permission,versioning::Table::Liveness})
          result->dependencies_.push_back(std::string(1,static_cast<char>(table))+key);
        for (auto path=path_utils::parent_of(m.key.path);;path=path_utils::parent_of(path)) {
          if (!facts->parents.count(path)) {
            const auto parent=snapshot_->latest.find(path);
            facts->parents[path]=parent==snapshot_->latest.end()?std::optional<Inode>{}:
              std::optional<Inode>{snapshot_->records.at(parent->second).inode};
            if(parent!=snapshot_->latest.end()) {
              const auto encoded=versioning::encode_namespace_record(snapshot_->records.at(parent->second));
              context_charge(used,encoded.size());
              auto evidence=namespace_evidence(facts->view.head.identity,versioning::Table::Current,path,encoded);
              observe_namespace_evidence(evidence);facts->parent_evidence.emplace(path,std::move(evidence));
            }
            result->dependencies_.push_back(std::string(1,static_cast<char>(versioning::Table::Current))+path);
          }
          if (path=="/") break;
        }
      }
      std::sort(result->dependencies_.begin(),result->dependencies_.end());
      result->dependencies_.erase(std::unique(result->dependencies_.begin(),result->dependencies_.end()),result->dependencies_.end());
      if (result->dependencies_.size()>versioning::kScanEntries)
        throw versioning::Error(versioning::ErrorCode::Limit,"read context dependency limit exceeded");
      for (const auto& key:result->dependencies_) {
        context_charge(used,key.size());
        require_query_metadata("dependency:"+facts->view.head.identity.namespace_id+
          facts->view.head.identity.incarnation+":"+key,key.size());
      }
      result->base_bytes_=used;result->facts_=std::move(facts);result->selected_=true;
      return Result::success(std::move(result));
    } catch (const std::exception& error) {return Result::failure(context_failure(error));}
  }
  const std::vector<QuerySourceReference>& sources() const override {return sources_;}
  const versioning::Head& captured_head() const {return snapshot_->view.head;}
  void enable_hot_proofs(const std::shared_ptr<HotQueryCache>& pool,
      std::shared_ptr<HotQueryCache::Reservation> reservation,bool parent_backing=true) const {
    std::lock_guard<std::mutex> lock(mutex_);
    hot_pool_=pool;hot_reservation_=std::move(reservation);hot_parent_backing_=parent_backing;
  }
  void bind_hot_fee_batches(std::shared_ptr<const PreparedQueryMetadataTrace> selection,
      std::shared_ptr<const PreparedQueryMetadataTrace> manifests,
      std::shared_ptr<const PreparedQueryMetadataTrace> catalogs) const {
    std::string principal;
    std::shared_ptr<const HotProof> proof;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // One frozen context belongs to one immutable domain. No request can
      // replace the fee identities of an already bound domain.
      if(hot_fee_batches_[0])return;
      hot_fee_batches_={std::move(selection),std::move(manifests),std::move(catalogs)};
      hot_candidate_fees_=false;
      for(const auto& entry:hot_proofs_)if(entry.second.proof) {
        proof=std::static_pointer_cast<const HotProof>(entry.second.proof->acquire());
        if(proof){principal=entry.first;break;}
      }
    }
    if(proof)(void)hot_fee_summary(principal,proof);
  }
  void bind_candidate_selection_fees(
      std::shared_ptr<const PreparedQueryMetadataTrace> selection) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if(hot_fee_batches_[0])return;
    hot_fee_batches_[0]=std::move(selection);hot_candidate_fees_=true;
  }
  void bind_domain_metadata_fees(
      std::shared_ptr<const PreparedQueryMetadataTrace> adapter) const noexcept override {
    // A published adapter artifact contributes its already accounted immutable
    // batch. No index, request, principal or ledger is retained through here.
    try {
      if(!adapter)return;
      std::string principal;
      std::shared_ptr<const HotProof> proof;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!hot_candidate_fees_||!hot_fee_batches_[0]||hot_fee_batches_[1])return;
        hot_fee_batches_[1]=std::move(adapter);
        for(const auto& entry:hot_proofs_)if(entry.second.proof) {
          proof=std::static_pointer_cast<const HotProof>(entry.second.proof->acquire());
          if(proof){principal=entry.first;break;}
        }
      }
      if(proof)(void)hot_fee_summary(principal,proof);
    }catch(...) {} // Optional summary bookkeeping cannot change discovery.
  }
  void arm_domain_metadata_summary() const noexcept override {
    try {arm_hot_fee_summary();}catch(...) {}
  }
  void arm_hot_fee_summary() const {
    std::shared_ptr<const PreparedQueryMetadataStageSummary> summary;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Selection has no principal argument. A representative plan can advance
      // only through its exact shared selection batch. admit() re-arms the
      // actual principal's plan before its proof, after fresh head validation.
      for(const auto& entry:hot_proofs_)if(entry.second.summary) {
        summary=std::static_pointer_cast<const PreparedQueryMetadataStageSummary>(entry.second.summary->acquire());
        if(summary)break;
      }
    }
    if(summary&&arm_query_metadata_stage_summary(summary))
      query_profile_count("hot_query.fee_summary_arms");
  }
  std::uint64_t hot_resident_estimate() const {
    // Conservative transitive accounting: snapshot, selected Facts, permission
    // hints and dependency/source strings. Shared objects are overcharged.
    std::uint64_t bytes=32768;
    for(const auto& item:snapshot_->records)
      bytes+=4096+8*(versioning::encode_namespace_record(item.second).size()+item.first.path.capacity());
    for(const auto& key:dependencies_)bytes+=256+4*key.capacity();
    return bytes;
  }
  // Only identities fully covered by the captured candidate set can borrow
  // this backing. Negative selections need their original catalog lookup.
  bool covers_exact_view(const SourceSelector& selector) const {
    if(!selected_||selector.file_predicate||selector.paths.empty()||
       selector.versions.kind!=VersionSelectionKind::Exact||!selector.versions.exact||
       selector.formats.adapters!=AdapterSelectionKind::Listed||
       selector.formats.include_byte_passthrough||selector.formats.adapter_ids.size()!=1)return false;
    for(const auto& scope:selector.paths.normalized_paths()) {
      if(scope.kind!=PathScopeKind::Exact)return false;
      const StructuredBlobVersionKey key{scope.path,*selector.versions.exact};
      if(!contains_frozen_source(key))return false;
      const auto found=snapshot_->records.find(key);
      if(found==snapshot_->records.end()||found->second.inode.is_dir||
         found->second.manifest.format_id!=selector.formats.adapter_ids.front())return false;
    }
    return true;
  }
  std::shared_ptr<const HotCandidateDomain> exact_view(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found=exact_views_.find(key);
    return found==exact_views_.end()?std::shared_ptr<const HotCandidateDomain>{}:
      std::static_pointer_cast<const HotCandidateDomain>(found->second->acquire());
  }
  void prune_exact_views() const {
    // Slot management remains charged even after its payload leaves. Move the
    // final slot owners out before destroying their keys and management bytes.
    std::array<std::shared_ptr<HotQueryCache::RetainedSlot>,64> retired;
    std::size_t count=0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for(auto it=exact_views_.begin();it!=exact_views_.end();) {
        if(!it->second->present()) {retired[count++]=std::move(it->second);it=exact_views_.erase(it);}
        else ++it;
      }
    }
  }
  bool can_retain_exact_view() const {
    prune_exact_views();
    std::lock_guard<std::mutex> lock(mutex_);return exact_views_.size()<64;
  }
  bool retain_exact_view(std::string key,std::shared_ptr<const HotCandidateDomain> value) const {
    std::shared_ptr<HotQueryCache> pool;
    {std::lock_guard<std::mutex> lock(mutex_);pool=hot_pool_.lock();}
    if(!pool)return false;
    auto slot=pool->retain(std::move(value),HotQueryCache::Retention::ExactView,1024+4*key.capacity());
    if(!slot)return false;
    prune_exact_views();
    std::lock_guard<std::mutex> lock(mutex_);
    if(exact_views_.size()>=64)return false;
    return exact_views_.emplace(std::move(key),std::move(slot)).second;
  }
  std::shared_ptr<HotQueryCache::Reservation> backing_reservation() const {
    std::lock_guard<std::mutex> lock(mutex_);return hot_reservation_;
  }
  std::shared_ptr<const VersionDirectoryReadContext> share_backing(
      const VersionDirectoryReadContext& parent,std::shared_ptr<const void> charge) const {
    if(!selected_||!(snapshot_->view.head==parent.snapshot_->view.head))return {};
    // Compare all narrow records, including ancestors. The cold selection has
    // already recorded its own complete fees and selected-source ordering.
    for(const auto& item:snapshot_->records) {
      const auto found=parent.snapshot_->records.find(item.first);
      if(found==parent.snapshot_->records.end()||
         versioning::encode_namespace_record(found->second)!=versioning::encode_namespace_record(item.second))return {};
    }
    auto result=std::make_shared<VersionDirectoryReadContext>(backend_,parent.snapshot_);
    std::lock_guard<std::mutex> lock(mutex_);
    result->sources_=sources_;result->dependencies_=dependencies_;result->base_bytes_=base_bytes_;
    result->selected_=true;result->facts_=facts_;result->retained_charge_=std::move(charge);
    return result;
  }
  std::shared_ptr<const QueryReadContext> result_reference() const noexcept override {
    try {
      std::shared_ptr<HotQueryCache> pool;
      std::shared_ptr<const Facts> facts;
      std::vector<std::pair<std::string,std::shared_ptr<HotQueryCache::RetainedSlot>>> proofs;
      std::uint64_t bytes=36864;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        pool=hot_pool_.lock();facts=facts_;
        proofs.reserve(hot_proofs_.size());
        for(const auto& proof:hot_proofs_)if(proof.second.proof&&proof.second.proof->present()) {
          bytes+=1024+4*proof.first.capacity();
          proofs.emplace_back(proof.first,proof.second.proof);
        }
      }
      if(!pool||!selected_||!facts)return {};
      for(const auto& source:sources_) {
        const auto& record=snapshot_->records.at({source.path,source.version});
        bytes+=8192+16*(versioning::encode_namespace_record(record).size()+source.path.capacity());
      }
      for(const auto& parent:facts->parents) {
        bytes+=4096+8*parent.first.capacity();
        const auto found=snapshot_->latest.find(parent.first);
        if(found!=snapshot_->latest.end())
          bytes+=8192+16*versioning::encode_namespace_record(snapshot_->records.at(found->second)).size();
      }
      for(const auto& dependency:dependencies_)bytes+=256+4*dependency.capacity();
      auto reservation=pool->reserve(HotQueryCache::Kind::Auxiliary,bytes);
      if(!reservation){query_profile_count("hot_query.result_reference_opt_outs");return {};}
      // Rebuild only the original selected identities and their ancestors. A
      // completed IPC result needs neither unrelated candidates in a shared
      // backing nor adapter indexes, selection traces or summary plans.
      auto snapshot=std::make_shared<VersionDirectorySnapshot>();snapshot->view=snapshot_->view;
      for(const auto& source:sources_) {
        const StructuredBlobVersionKey key{source.path,source.version};
        snapshot->sources.push_back(key);snapshot->records.emplace(key,snapshot_->records.at(key));
      }
      for(const auto& parent:facts->parents) {
        const auto found=snapshot_->latest.find(parent.first);
        if(found==snapshot_->latest.end())continue;
        snapshot->latest.emplace(parent.first,found->second);
        snapshot->records.emplace(found->second,snapshot_->records.at(found->second));
      }
      auto result=std::make_shared<VersionDirectoryReadContext>(backend_,std::move(snapshot));
      result->retained_charge_=reservation;result->hot_reservation_=reservation;
      result->sources_=sources_;result->dependencies_=dependencies_;
      result->base_bytes_=base_bytes_;result->selected_=true;result->facts_=std::move(facts);result->hot_pool_=pool;
      result->hot_reference_only_=true;
      // Any later result-only proof is ordinary cache data, not a protected
      // discovery-domain proof. Existing shared slots retain their priority.
      result->hot_parent_backing_=false;
      for(const auto& proof:proofs)
        result->hot_proofs_.emplace(proof.first,HotProofSlot{proof.second,false});
      query_profile_count("hot_query.result_references");
      return result;
    }catch(...){query_profile_count("hot_query.result_reference_opt_outs");return {};}
  }
  const versioning::NamespaceRecord& record(const StructuredBlobVersionKey& key) const {
    const auto found=std::find_if(sources_.begin(),sources_.end(),[&](const auto& source) {
      return source.path==key.path && source.version==key.version;
    });
    if (!selected_ || found==sources_.end())
      throw versioning::Error(versioning::ErrorCode::Invalid,"source is outside frozen context");
    return snapshot_->records.at(key);
  }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal& principal) const override;
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey& key) const override {
    using Result=QueryResult<StructuredBlobManifest>;
    try {
      const auto backend=backend_.lock();
      if (!backend || !selected_) return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"read context is unavailable"));
      (void)backend->owner_->view(); // Frozen owner is never hidden by a hot manifest.
      // This method is immutable content access, not an authorization grant.
      // Frontends admit the complete context before planning any candidate.
      if (!contains_frozen_source(key)) return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"version is outside frozen candidates"));
      const auto& record=snapshot_->records.at(key);
      auto verified=backend->verified_content_unlocked(record);
      auto value=record.manifest;value.metadata=verified->metadata;value.metadata_materialized=true;
      value.payload_reader=backend->make_payload_reader_unlocked(value,std::move(verified));
      return Result::success(std::move(value));
    } catch(const std::exception& error) {return Result::failure(context_failure(error));}
  }
private:
  std::shared_ptr<const PreparedQueryMetadataStageSummary> hot_fee_summary(
      const std::string& principal,const std::shared_ptr<const HotProof>& proof) const {
    // Collectors need each original observation. Defer rather than caching a
    // collector-specific refusal, so a later ordinary request may build once.
    if(active_query_metadata_trace)return {};
    std::array<std::shared_ptr<const PreparedQueryMetadataTrace>,3> batches;
    bool candidate=false;
    std::shared_ptr<HotQueryCache> pool;
    std::shared_ptr<HotQueryCache::RetainedSlot> proof_slot,retired_summary;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found=hot_proofs_.find(principal);
      if(found==hot_proofs_.end()||!found->second.proof||
         found->second.proof->acquire()!=proof)return {};
      proof_slot=found->second.proof;
      if(found->second.summary) {
        auto summary=std::static_pointer_cast<const PreparedQueryMetadataStageSummary>(found->second.summary->acquire());
        if(summary)return summary;
        // A successful plan may be rebuilt after eviction. An original build
        // refusal retains its previous attempt-once behavior.
        retired_summary=std::move(found->second.summary);found->second.summary_attempted=false;
      }
      if(found->second.summary_building||found->second.summary_attempted||!hot_fee_batches_[0])return {};
      // Candidate proofs often exist before QueryModule publishes its adapter
      // batch. Missing that batch is a deferred opportunity, never a refusal.
      if(hot_candidate_fees_&&!hot_fee_batches_[1])return {};
      candidate=hot_candidate_fees_;
      batches=hot_fee_batches_;pool=hot_pool_.lock();
      if(!pool)return {};
      // Optional builders never wait for one another and attempt at most once
      // for this immutable proof/domain. Quota/overflow refusal affects only
      // this optimization; ordinary replay and admission remain available.
      found->second.summary_building=true;found->second.summary_attempted=true;
    }
    query_profile_count("hot_query.fee_summary_build_attempts");
    std::shared_ptr<const PreparedQueryMetadataStageSummary> result;
    try {
      if(proof->reservation->grow(HotSummaryCharge::wrapper_bytes)) {
        std::shared_ptr<HotSummaryCharge> charge;
        try {
          charge=std::make_shared<HotSummaryCharge>();charge->reservation=proof->reservation;
        }catch(...) {proof->reservation->shrink(HotSummaryCharge::wrapper_bytes);throw;}
        // The argument vector and callback control objects fit in wrapper_bytes
        // and are charged before allocation. The compiler reserves every union
        // node before allocating it, then refunds all temporary nodes.
        if(candidate) {
          // Actual discovery order: selection, actual-principal authorization,
          // adapter metadata, final authorization. Repeated admissions of the
          // same proof return through the ledger's existing batch memo first.
          result=PreparedQueryMetadataStageSummary::compile(
            {batches[0],proof->fees,batches[1],proof->fees},
            [charge](std::uint64_t bytes){charge->grow(bytes);},
            [charge](std::uint64_t bytes){charge->shrink(bytes);},charge);
        }else {
          result=PreparedQueryMetadataStageSummary::compile(
            {batches[0],proof->fees,batches[1],batches[2],proof->fees},
            [charge](std::uint64_t bytes){charge->grow(bytes);},
            [charge](std::uint64_t bytes){charge->shrink(bytes);},charge);
        }
      }
    }catch(const std::bad_alloc&) {}
    auto slot=result?pool->retain(result,HotQueryCache::Retention::Derived):nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found=hot_proofs_.find(principal);
      if(found!=hot_proofs_.end()&&found->second.proof==proof_slot) {
        found->second.summary_building=false;
        if(proof_slot->present()) {
          proof->summary_slot=slot;
          found->second.summary=std::move(slot);
        }
      }
    }
    if(result)query_profile_count("hot_query.fee_summary_builds");
    else {pool->count(&HotQueryCacheStats::opt_outs);query_profile_count("hot_query.fee_summary_opt_outs");}
    return result;
  }
  void prune_hot_proofs() const {
    for(;;) {
      std::shared_ptr<HotQueryCache::RetainedSlot> retired_proof,retired_summary;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found=hot_proofs_.begin();
        while(found!=hot_proofs_.end()&&(found->second.building||found->second.summary_building||
              (found->second.proof&&found->second.proof->present())))++found;
        if(found==hot_proofs_.end())return;
        retired_proof=std::move(found->second.proof);retired_summary=std::move(found->second.summary);
        hot_proofs_.erase(found);
      }
      // Destroy the last slot/key charge outside the context mutex.
    }
  }
  void finish_hot_proof(const std::string& principal) const {
    std::shared_ptr<HotQueryCache::RetainedSlot> retired_proof,retired_summary;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found=hot_proofs_.find(principal);
      if(found!=hot_proofs_.end()) {
        found->second.building=false;
        if(!found->second.summary_building&&(!found->second.proof||!found->second.proof->present())) {
          retired_proof=std::move(found->second.proof);retired_summary=std::move(found->second.summary);
          hot_proofs_.erase(found);
        }
      }
    }
    hot_changed_.notify_all();
  }
  bool contains_frozen_source(const StructuredBlobVersionKey& key) const {
    if (!selected_) return false;
    try {
      // select() sorts these already charged dependencies for any candidate
      // order. Only selected sources have Exact entries; ancestors use Current.
      const auto dependency=std::string(1,static_cast<char>(versioning::Table::Exact))+
        versioning::exact_key(key.path,key.version);
      return std::binary_search(dependencies_.begin(),dependencies_.end(),dependency);
    } catch(const versioning::Error& error) {
      // A malformed key was previously just absent from sources_. Preserve
      // record's Invalid / manifest's StalePlan outside-context responses.
      if(error.code()==versioning::ErrorCode::Invalid) return false;
      throw;
    }
  }
  std::weak_ptr<const IoTDBStructuredBlobBackend> backend_;
  std::shared_ptr<const VersionDirectorySnapshot> snapshot_;
  std::vector<QuerySourceReference> sources_;
  std::vector<std::string> dependencies_;
  std::uint64_t base_bytes_{0};
  bool selected_{false};
  mutable std::mutex mutex_;
  mutable std::shared_ptr<const Facts> facts_;
  mutable std::weak_ptr<HotQueryCache> hot_pool_;
  mutable std::map<std::string,HotProofSlot> hot_proofs_;
  mutable std::array<std::shared_ptr<const PreparedQueryMetadataTrace>,3> hot_fee_batches_;
  mutable bool hot_candidate_fees_{false},hot_parent_backing_{true};
  bool hot_reference_only_{false};
  mutable std::condition_variable hot_changed_;
  mutable std::map<std::string,std::shared_ptr<HotQueryCache::RetainedSlot>> exact_views_;
};

QueryResult<std::shared_ptr<QueryAdmission>> VersionDirectoryReadContext::admit(const QueryPrincipal& principal) const {
  using Result=QueryResult<std::shared_ptr<QueryAdmission>>;
  try {
    const auto budget=current_query_metadata_budget();
    if(budget && !budget->status().ok()) return Result::failure(budget->status());
    const auto backend=backend_.lock();
    if (!backend || !selected_) return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"read context is unavailable"));
    std::shared_ptr<HotQueryCache> hot;
    std::shared_ptr<const void> hot_pin;
    {std::lock_guard<std::mutex> lock(mutex_);hot=hot_pool_.lock();
      hot_pin=retained_charge_?retained_charge_:hot_reservation_;}
    std::shared_ptr<HotProof> producing;
    std::unique_ptr<HotProofBuild> producing_guard;
    std::unique_ptr<ScopedQueryMetadataTrace> proof_trace;
    std::string principal_key;
    if(hot) try {
      const auto view=backend->owner_->view(); // Never hide frozen owners.
      if(view.head==snapshot_->view.head) {
        auto groups=principal.supplementary_groups;
        std::sort(groups.begin(),groups.end());groups.erase(std::unique(groups.begin(),groups.end()),groups.end());
        groups.erase(std::remove(groups.begin(),groups.end(),principal.gid),groups.end());
        principal_key=std::to_string(principal.uid)+":"+std::to_string(principal.gid)+":";
        for(auto group:groups)principal_key+=std::to_string(group)+",";
        prune_hot_proofs();
        for(;;) {
          std::shared_ptr<const HotProof> cached;
          std::shared_ptr<HotQueryCache::RetainedSlot> retired_proof,retired_summary;
          bool summary_inflight=false;
          {std::unique_lock<std::mutex> lock(mutex_);
            auto found=hot_proofs_.find(principal_key);
            if(found!=hot_proofs_.end()&&found->second.building) {
              hot_changed_.wait(lock,[&]{const auto it=hot_proofs_.find(principal_key);return it==hot_proofs_.end()||!it->second.building;});
              continue;
            }
            if(found!=hot_proofs_.end()) {
              if(found->second.proof)cached=std::static_pointer_cast<const HotProof>(found->second.proof->acquire());
              if(!cached) {
                summary_inflight=found->second.summary_building;
                if(!summary_inflight) {
                  retired_proof=std::move(found->second.proof);retired_summary=std::move(found->second.summary);
                  hot_proofs_.erase(found);
                }
              }
            }
          }
          if(summary_inflight)break; // Optional summary completion never blocks ordinary admission.
          if(cached&&cached->head==view.head) {
            if(auto summary=hot_fee_summary(principal_key,cached))
              if(arm_query_metadata_stage_summary(summary))query_profile_count("hot_query.fee_summary_arms");
            replay_query_metadata_trace(cached->fees);
            auto admission=std::make_shared<DirectoryAdmission>(backend->owner_,snapshot_,hot_pin);
            try {
              auto gate=backend->owner_->admit(cached->head);
              admission->complete(std::move(gate),cached,{});
              hot->count(&HotQueryCacheStats::proof_hits);
              query_profile_count("hot_query.proof_hits");query_profile_count("version_context.admissions");
              return Result::success(std::move(admission));
            }catch(const versioning::Error& error){if(error.code()!=versioning::ErrorCode::Conflict)throw;break;}
          }
          auto reservation=hot->reserve(HotQueryCache::Kind::Proof,
            2048+4*principal_key.capacity()+PreparedQueryMetadataTrace::management_bytes);
          if(!reservation){hot->count(&HotQueryCacheStats::opt_outs);break;}
          auto candidate=std::make_shared<HotProof>();candidate->head=view.head;candidate->reservation=std::move(reservation);
          auto guard=std::make_unique<HotProofBuild>();guard->context=this;guard->principal=principal_key;
          {std::lock_guard<std::mutex> lock(mutex_);
            const auto inserted=hot_proofs_.emplace(principal_key,HotProofSlot{{},true});
            if(!inserted.second){guard->context=nullptr;continue;}
          }
          producing=std::move(candidate);producing_guard=std::move(guard);
          proof_trace=std::make_unique<ScopedQueryMetadataTrace>("",[reservation=producing->reservation](std::uint64_t bytes){
            if(!reservation->grow(bytes))throw std::bad_alloc();
          },QueryMetadataTracePolicy::CacheStable);
          break;
        }
      }
    }catch(const std::bad_alloc&){proof_trace.reset();producing_guard.reset();producing.reset();hot->count(&HotQueryCacheStats::opt_outs);}
    for (unsigned attempt=0;attempt<3;++attempt) {
      std::shared_ptr<const Facts> previous;
      {std::lock_guard<std::mutex> lock(mutex_);previous=facts_;}
      const auto change=backend->owner_->check_dependencies(previous->view,dependencies_);
      auto facts=std::make_shared<Facts>(*previous);facts->view=change.current;
      for(const auto& evidence:facts->record_evidence) observe_namespace_evidence(evidence);
      for(const auto& evidence:facts->parent_evidence) observe_namespace_evidence(evidence.second);
      for(const auto& key:dependencies_) require_query_metadata("dependency:"+
        facts->view.head.identity.namespace_id+facts->view.head.identity.incarnation+":"+key,key.size());
      if (change.state!=versioning::DependencyState::Unchanged) {
        query_profile_count("version_context.dependency_rereads",dependencies_.size());
        std::uint64_t used=base_bytes_;
        facts->record_evidence.clear();
        for (auto& record:facts->records) {
          const auto key=versioning::exact_key(record.inode.path,record.inode.row_time);
          auto current=backend->owner_->get(change.current,versioning::Table::Exact,key);
          auto permission=backend->owner_->get(change.current,versioning::Table::Permission,key);
          auto live=backend->owner_->get(change.current,versioning::Table::Liveness,key);
          if (!current || !permission || *current!=*permission || !live || *live!=versioning::Bytes{1})
            return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"frozen source is no longer alive"));
          context_charge(used,current->size()+permission->size()+live->size());
          auto evidence=namespace_evidence(change.current.head.identity,versioning::Table::Exact,key,*current);
          observe_namespace_evidence(evidence);facts->record_evidence.push_back(std::move(evidence));
          observe_namespace_evidence(namespace_evidence(change.current.head.identity,versioning::Table::Permission,key,*permission));
          observe_namespace_evidence(namespace_evidence(change.current.head.identity,versioning::Table::Liveness,key,*live));
          auto fresh=namespace_identity(*current);
          if (!same_content_binding(record,fresh)) return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"frozen content binding changed"));
          record=std::move(fresh);
        }
        for (auto& parent:facts->parents) {
          auto current=backend->owner_->get(change.current,versioning::Table::Current,parent.first);
          if (current) {
            context_charge(used,current->size());parent.second=namespace_identity(*current).inode;
            auto evidence=namespace_evidence(change.current.head.identity,versioning::Table::Current,parent.first,*current);
            observe_namespace_evidence(evidence);facts->parent_evidence[parent.first]=std::move(evidence);
          } else {parent.second.reset();facts->parent_evidence.erase(parent.first);}
        }
      }
      // Check all candidates before reading any content, including candidates
      // that later fail an attribute/time predicate.
      std::set<std::string> checked_parents;
      for (std::size_t i=0;i<facts->records.size();++i) {
        const auto& inode=facts->records[i].inode;
        query_profile_count("version_context.source_permission_checks");
        if (check_query_posix_access(inode.mode,inode.uid,inode.gid,R_OK,principal.uid,principal.gid,principal.supplementary_groups)!=0)
          return Result::failure(QueryStatus::failure(QueryError::PermissionDenied,QueryPhase::Catalog,"source read permission denied",QueryErrorDetail{sources_[i]}));
        for (auto path=path_utils::parent_of(inode.path);;path=path_utils::parent_of(path)) {
          if (checked_parents.insert(path).second) {
            const auto& parent=facts->parents.at(path);
            query_profile_count("version_context.parent_permission_checks");
            if (!parent || !parent->is_dir) return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"source parent is unavailable",QueryErrorDetail{sources_[i]}));
            if (check_query_posix_access(parent->mode,parent->uid,parent->gid,X_OK,principal.uid,principal.gid,principal.supplementary_groups)!=0)
              return Result::failure(QueryStatus::failure(QueryError::PermissionDenied,QueryPhase::Catalog,"source parent search permission denied",QueryErrorDetail{sources_[i]}));
          }
          if (path=="/") break;
        }
      }
      std::uint64_t used=base_bytes_;
      for (const auto& record:facts->records) {
        (void)backend->content_artifacts_->verify({record.descriptor_id,record.seal_id},record.manifest.key,
          record.manifest.format_id,record.manifest.payload_size,record.manifest.layout_size,false,&used);
      }
      // Allocate the guard wrapper before admission; the guarded path only
      // swaps shared pointers. Keep both observed and replaced facts alive
      // until the gate ends, including when two callers update one context.
      auto admission=std::make_shared<DirectoryAdmission>(backend->owner_,snapshot_,hot_pin);
      std::shared_ptr<const HotProof> ready_proof;
      std::shared_ptr<HotQueryCache::RetainedSlot> ready_slot;
      if(producing&&facts->view.head==producing->head&&proof_trace&&!proof_trace->failed()) {
        try {
          // The batch can outlive this proof through a request ledger. Keep its
          // trace and management charge pinned until the final batch owner dies.
          producing->fees=std::make_shared<const PreparedQueryMetadataTrace>(
            proof_trace->release(),producing->reservation);
          ready_proof=producing;
          // Allocate/register optional cache ownership before the Owner gate.
          // Admission and execution keep their ordinary strong pins even when
          // pressure clears this slot before publication.
          ready_slot=hot->retain(ready_proof,hot_parent_backing_?
            HotQueryCache::Retention::ParentProof:HotQueryCache::Retention::Proof,
            1024+4*principal_key.capacity());
        }catch(const std::bad_alloc&){hot->count(&HotQueryCacheStats::opt_outs);}
        proof_trace.reset();
      }
      try {
        auto gate=backend->owner_->admit(change.current.head);
        std::shared_ptr<const Facts> retired;
        {std::lock_guard<std::mutex> lock(mutex_);retired=std::move(facts_);facts_=std::move(facts);
          if(ready_slot)hot_proofs_.at(principal_key).proof=ready_slot;
        }
        admission->complete(std::move(gate),std::move(retired),std::move(previous));
        if(ready_slot&&ready_slot->present()){hot->count(&HotQueryCacheStats::proof_builds);query_profile_count("hot_query.proof_builds");}
        query_profile_count("version_context.admissions");
        return Result::success(std::move(admission));
      } catch(const versioning::Error& error) {
        if(error.code()!=versioning::ErrorCode::Conflict) throw;
        query_profile_count("version_context.admission_retries");
      }
    }
    return Result::failure(QueryStatus::failure(QueryError::StalePlan,QueryPhase::Catalog,"namespace kept changing during admission"));
  } catch(const std::exception& error) {return Result::failure(context_failure(error));}
}

std::shared_ptr<const VersionDirectorySnapshot> IoTDBStructuredBlobBackend::parsed_directory_snapshot(const versioning::View& view) const {
  {std::lock_guard<std::mutex> lock(snapshot_mu_);
    if(snapshot_cache_ && snapshot_cache_->view.head==view.head) {
      for(const auto& evidence:snapshot_cache_->evidence) observe_namespace_evidence(evidence);
      replay_query_metadata_trace(snapshot_cache_->directory_fees);
      query_profile_count("version_context.directory_snapshot_hits");return snapshot_cache_;
    }
  }
  auto snapshot=std::make_shared<VersionDirectorySnapshot>();snapshot->view=view;
  ScopedQueryMetadataTrace directory_fees("directory/");
  for(const auto& item:owner_->scan(view,versioning::Table::Exact)) {
    context_charge(snapshot->logical_bytes,item.first.size()+item.second.size());
    snapshot->evidence.push_back(namespace_evidence(view.head.identity,versioning::Table::Exact,item.first,item.second));
    observe_namespace_evidence(snapshot->evidence.back());
    auto record=namespace_identity(item.second);
    if(item.first!=versioning::exact_key(record.inode.path,record.inode.row_time))
      throw versioning::Error(versioning::ErrorCode::Corrupt,"snapshot exact key differs from inode");
    const auto key=record.manifest.key;
    const auto latest=snapshot->latest.find(key.path);
    if(latest==snapshot->latest.end()||latest->second.version<key.version) snapshot->latest[key.path]=key;
    snapshot->records.emplace(key,std::move(record));
    query_profile_count("version_context.directory_record_reads");
  }
  context_charge(snapshot->logical_bytes,directory_fees.estimated_bytes());
  snapshot->directory_fees=directory_fees.release();
  // A racing old build cannot replace a newer current-root cache entry.
  {std::lock_guard<std::mutex> lock(snapshot_mu_);
    if(!snapshot_cache_||snapshot_cache_->view.head.sequence<=view.head.sequence) snapshot_cache_=snapshot;
  }
  query_profile_count("version_context.directory_snapshot_builds");return snapshot;
}

std::shared_ptr<const VersionDirectorySnapshot> IoTDBStructuredBlobBackend::version_snapshot(const SourceSelector& selector) const {
  const auto view=owner_->view(); // Cache hits cannot hide a frozen owner.
  const auto budget=current_query_metadata_budget();
  if(budget && !budget->status().ok()) throw QueryMetadataBudgetExceeded();
  auto snapshot=std::make_shared<VersionDirectorySnapshot>();snapshot->view=view;
  const auto observe=[&](versioning::Table table,const std::string& key,const versioning::Bytes& bytes) {
      context_charge(snapshot->logical_bytes,key.size()+bytes.size());
      observe_namespace_evidence(namespace_evidence(view.head.identity,table,key,bytes));
      query_profile_count("version_context.selected_record_reads");
    };
  const bool broad=selector.paths.empty() || std::any_of(selector.paths.begin(),selector.paths.end(),
    [](const PathScope& scope){return scope.kind==PathScopeKind::Subtree;});
  versioning::NamespaceSelection selected;
  if(broad) {
    const auto directory=parsed_directory_snapshot(view);
    selected=versioning::project_namespace(*owner_,view,selector,directory->records,observe);
    query_profile_count("version_context.directory_projections");
  } else {
    selected=versioning::select_namespace(*owner_,view,selector,observe);
  }
  for(auto& item:selected.sources) {
    snapshot->sources.push_back(item.first);
    snapshot->records.emplace(item.first,std::move(item.second));
  }
  for(auto& parent:selected.parents) if(parent.second) {
    const auto key=parent.second->manifest.key;
    snapshot->latest.emplace(parent.first,key);
    snapshot->records.emplace(key,std::move(*parent.second));
  }
  query_profile_count("version_context.selected_sources",snapshot->sources.size());
  query_profile_count("version_context.snapshot_builds");return snapshot;
}

std::string IoTDBStructuredBlobBackend::physical_sql(const std::string& sql) const {
  if (table_prefix_.empty()) return sql;
  std::string out;
  bool quoted = false;
  for (std::size_t i = 0; i < sql.size();) {
    const char c = sql[i];
    if (c == '\'') {
      out += c; ++i;
      if (quoted && i < sql.size() && sql[i] == '\'') { out += sql[i++]; continue; }
      quoted = !quoted; continue;
    }
    if (!quoted && (std::isalpha(static_cast<unsigned char>(c)) || c == '_')) {
      const auto start = i++;
      while (i < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) ++i;
      auto token = sql.substr(start, i-start);
      if (kPhysicalTables.count(token)) out += table_prefix_ + '_';
      out += token;
    } else { out += c; ++i; }
  }
  return out;
}
bool IoTDBStructuredBlobBackend::execute_nonquery(const std::string& sql) const {
  return legacy_execute_nonquery(physical_sql(sql));
}
std::shared_ptr<SessionDataSet> IoTDBStructuredBlobBackend::execute_query(
    const std::string& sql, bool metadata) const {
  return legacy_execute_query(physical_sql(sql), metadata);
}
bool IoTDBStructuredBlobBackend::insert_tablet(Tablet& tablet) const {
  if (!table_prefix_.empty() && kPhysicalTables.count(tablet.deviceId))
    tablet.deviceId = table_prefix_ + '_' + tablet.deviceId;
  return IoTDBConnector::instance().insertTablet(tablet);
}

bool IoTDBStructuredBlobBackend::configure_version_directory(const VersionDirectoryConfig& settings) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  if (owner_ || !table_prefix_.empty()) return false;
  try {
    auto config = IoTDBConnector::instance().configuration();
    if ((config.host != "localhost" && config.host != "127.0.0.1") ||
        settings.owner_directory.empty() || settings.owner_directory.front() != '/' ||
        !std::all_of(config.db.begin(), config.db.end(), [](unsigned char c) { return !std::isupper(c); }))
      return false;
    config.host = "127.0.0.1";
    const std::string scope = config.host + ':' + std::to_string(config.port) + '/' + config.db + '/' + settings.table_prefix;
    const versioning::Bytes scope_bytes(scope.begin(), scope.end());
    const auto scope_id = versioning::content_id("owner-scope", "iotdbfs-owner-scope/1", scope_bytes);
    std::string error;
    auto exclusion = NamespaceWriterLock::acquire("/tmp/iotdbfs-v1-" + scope_id + ".lock", &error);
    if (!exclusion) throw std::runtime_error(error);
    versioning::IoTDBVersionStore::Options store_options;
    store_options.reader_pool_size = settings.reader_pool_size;
    auto store = std::make_shared<versioning::IoTDBVersionStore>(
      config, settings.table_prefix, store_options);
    auto journal = std::make_shared<versioning::FileJournal>(settings.owner_directory);
    if (settings.initialize) store->create_schema();
    else store->verify_capabilities();
    versioning::OwnerConfig owner_options;
    owner_options.coalesce_payload_reads = settings.payload_read_coalescing;
    owner_options.payload_cache_bytes = settings.payload_cache_bytes;
    auto owner = std::make_shared<versioning::Owner>(store, journal, owner_options);
    if (settings.initialize) owner->initialize({versioning::random_uuid(), versioning::random_uuid()},
      {{versioning::Table::Current, versioning::content_profile_key(), versioning::content_profile()}});
    else owner->recover();
    const auto view = owner->view();
    const auto profile = owner->get(view, versioning::Table::Current, versioning::content_profile_key());
    if (!profile || *profile != versioning::content_profile())
      throw std::runtime_error("V04 content/payload profile missing or unsupported; implicit namespace upgrade is forbidden");
    auto artifacts = std::make_shared<versioning::ContentArtifacts>(owner, settings.artifact_validator);
    auto payload_artifacts = std::make_shared<versioning::PayloadArtifacts>(owner);
    std::shared_ptr<NativeFileStore> native_store;
    if (!settings.native_file_root.empty()) native_store = std::make_shared<NativeFileStore>(settings.native_file_root);
    std::uint64_t closure_bytes = 0;
    std::vector<StructuredBlobVersionKey> committed_files;
    const auto exact_records=owner->scan(view,versioning::Table::Exact);
    for (const auto& item : exact_records) {
      const auto record = versioning::decode_namespace_record(item.second);
      if (item.first != versioning::exact_key(record.inode.path, record.inode.row_time))
        throw std::runtime_error("namespace exact binding differs from record");
      if (!record.inode.is_dir) {
        const auto verified = artifacts->verify({record.descriptor_id, record.seal_id},
          record.manifest.key, record.manifest.format_id, record.manifest.payload_size,
          record.manifest.layout_size, true, &closure_bytes);
        payload_artifacts->verify(verified->descriptor.payload_root,
          record.manifest.payload_size, true, &closure_bytes, payload_source_identity(record.manifest.key));
        committed_files.push_back(record.manifest.key);
      }
    }
    // Latest queries can use Current only after its plain/per-format maxima
    // have been checked against the same authenticated Exact closure. Later
    // owner commits maintain these indexes atomically; queries still verify
    // selected Current bindings against Exact in their captured view.
    versioning::validate_current_index(*owner,view,exact_records);
    // Native-file records extend Current, without changing namespace-record/3.
    // Deleted versions may retain orphan locators, but live versions must bind
    // exactly the immutable scientific descriptor authenticated above.
    for (const auto& item : owner->scan(view, versioning::Table::Current, std::string(1, '\x01') + "native-file/")) {
      const auto location = decode_native_file_location(item.second);
      if (item.first != native_file_location_key(location.key) ||
          !(location.namespace_identity == view.head.identity))
        throw std::runtime_error("native file locator namespace or key differs");
      const auto exact = owner->get(view, versioning::Table::Exact,
        versioning::exact_key(location.key.path, location.key.version));
      if (exact) {
        const auto record = versioning::decode_namespace_record(*exact);
        if (record.inode.is_dir) throw std::runtime_error("native file locator refers to a directory");
        const auto verified = artifacts->verify({record.descriptor_id, record.seal_id},
          record.manifest.key, record.manifest.format_id, record.manifest.payload_size,
          record.manifest.layout_size);
        validate_native_file_binding(location, *verified);
      }
    }
    content_artifacts_ = std::move(artifacts);
    payload_artifacts_ = std::move(payload_artifacts);
    native_file_store_ = std::move(native_store);
    table_prefix_ = settings.table_prefix;
    version_owner_lock_ = std::move(exclusion);
    owner_ = std::move(owner);
    try {hot_cache_=std::make_shared<HotQueryCache>();}catch(const std::bad_alloc&){}

    metadata_generation_.store(owner_->view().head.sequence, std::memory_order_release);
    // Recovery has authenticated every committed file before any staging row
    // is removed. Retry cleanup interrupted after a previously confirmed head.
    for (const auto& key : committed_files) cleanup_staging_artifacts_unlocked(key);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR(std::string("version directory initialization rejected: ") + error.what());
    return false;
  }
}

versioning::ContentArtifactStats IoTDBStructuredBlobBackend::content_artifact_stats() const {
  return content_artifacts_ ? content_artifacts_->stats() : versioning::ContentArtifactStats{};
}

NamespaceMutation::NamespaceMutation(IoTDBStructuredBlobBackend& backend)
  : backend_(backend), lock_(backend.mutation_mu_) {
  if (!backend_.owner_ || backend_.pending_tx_) throw std::runtime_error("namespace transaction unavailable");
  backend_.pending_tx_ = backend_.owner_->begin("namespace-mutation");
}
NamespaceMutation::~NamespaceMutation() { if (!finished_) backend_.abort_namespace_unlocked(); }
bool NamespaceMutation::commit() {
  if (finished_) return false;
  finished_ = true;
  return backend_.commit_namespace_unlocked();
}
std::unique_ptr<NamespaceMutation> IoTDBStructuredBlobBackend::begin_namespace_mutation() {
  return std::unique_ptr<NamespaceMutation>(new NamespaceMutation(*this));
}
void IoTDBStructuredBlobBackend::abort_namespace_unlocked() {
  pending_tx_.reset(); pending_records_.clear(); pending_artifact_cleanup_.clear();
}
bool IoTDBStructuredBlobBackend::commit_namespace_unlocked() {
  try {
    if (!pending_tx_) return false;
    pending_tx_->commit();
    auto cleanup = std::move(pending_artifact_cleanup_);
    abort_namespace_unlocked();
    metadata_generation_.store(owner_->view().head.sequence, std::memory_order_release);
    for (const auto& key : cleanup) cleanup_staging_artifacts_unlocked(key);
    return true;
  } catch (const std::exception& error) {
    abort_namespace_unlocked();
    LOG_ERROR(std::string("namespace commit failed: ") + error.what());
    return false;
  }
}
std::optional<versioning::Bytes> IoTDBStructuredBlobBackend::namespace_record_unlocked(
    const StructuredBlobVersionKey& key, bool pending) const {
  const auto exact = versioning::exact_key(path_utils::norm(key.path), key.version);
  if (pending && pending_tx_) return pending_tx_->get(versioning::Table::Exact, exact);
  return owner_->get(owner_->view(), versioning::Table::Exact, exact);
}
std::vector<versioning::Bytes> IoTDBStructuredBlobBackend::namespace_records_unlocked(bool pending) const {
  std::map<std::string, versioning::Bytes> records;
  for (auto& item : owner_->scan(owner_->view(), versioning::Table::Exact)) records.emplace(item.first, std::move(item.second));
  if (pending) for (const auto& item : pending_records_) {
    if (item.second) records[item.first] = *item.second; else records.erase(item.first);
  }
  std::vector<versioning::Bytes> result;
  for (auto& item : records) result.push_back(std::move(item.second));
  return result;
}
void IoTDBStructuredBlobBackend::put_namespace_record_unlocked(const Inode& inode,
    const StructuredBlobManifest& manifest) {
  if (!pending_tx_) throw std::runtime_error("namespace write requires transaction");
  const auto exact = versioning::exact_key(inode.path, inode.row_time);
  auto retired = pending_tx_->get(versioning::Table::Liveness, exact);
  if (retired && *retired == versioning::Bytes{0}) throw std::runtime_error("exact namespace identity is retired");
  versioning::NamespaceRecord record{inode, manifest};
  if (!inode.is_dir) {
    auto previous = namespace_record_unlocked(manifest.key, true);
    if (previous) {
      const auto prior = decode_namespace_record_unlocked(*previous);
      record.descriptor_id = prior.descriptor_id;
      record.seal_id = prior.seal_id;
    } else {
      const auto layout = read_staging_layout_unlocked(manifest.key, manifest.layout_size);
      std::optional<DatasetCatalogArtifact> catalog;
      if (manifest.metadata.count("__iotdbfs.dataset_catalog.protocol")) {
        auto stored = read_staging_catalog_unlocked(manifest.key);
        if (!stored.ok()) throw std::runtime_error(stored.status().message());
        catalog = std::move(stored.value());
      }
      const auto payload_root = payload_artifacts_->build(*pending_tx_, manifest.payload_size,
        [this, key = manifest.key](std::uint64_t offset, std::uint64_t length) {
          return read_staging_payload_unlocked(key, offset, length);
        });
      const auto binding = content_artifacts_->build(*pending_tx_, manifest, layout, catalog, payload_root);
      record.descriptor_id = binding.descriptor_id;
      record.seal_id = binding.seal_id;
      pending_artifact_cleanup_.insert(manifest.key);
    }
    // Content refs are preserved for POSIX mutations; validate outer identity
    // without rebuilding or reparsing the immutable layout/catalog.
    const auto verified = verified_content_unlocked(record);
    const auto native_key = native_file_location_key(manifest.key);
    const auto native_record = pending_tx_->get(versioning::Table::Current, native_key);
    if (native_record) {
      validate_native_file_binding(decode_native_file_location(*native_record), *verified);
    } else if (!previous && native_file_store_ && manifest.format_id == std::optional<std::string>("hdf5")) {
      auto installed = native_file_store_->install(native_file_identity(*verified),
        [this, key = manifest.key](std::uint64_t offset, std::uint64_t length) {
          return StructuredBlobResult<StructuredBlobBytes>::success(read_staging_payload_unlocked(key, offset, length));
        });
      if (!installed.ok()) throw std::runtime_error(installed.status().message());
      validate_native_file_binding(installed.value(), *verified);
      pending_tx_->put(versioning::Table::Current, native_key, encode_native_file_location(installed.value()));
    }
  }
  const auto bytes = versioning::encode_namespace_record(record);
  pending_tx_->put(versioning::Table::Exact, exact, bytes);
  pending_tx_->put(versioning::Table::Permission, exact, bytes);
  pending_tx_->put(versioning::Table::Liveness, exact, versioning::Bytes{1});
  for (const auto& current : {inode.path, current_format_key(inode)}) {
    auto old = pending_tx_->get(versioning::Table::Current, current);
    if (!old || namespace_identity(*old).inode.row_time <= inode.row_time)
      pending_tx_->put(versioning::Table::Current, current, bytes);
  }
  pending_records_[exact] = bytes;
}
std::shared_ptr<const versioning::VerifiedFileContent>
IoTDBStructuredBlobBackend::verified_content_unlocked(const versioning::NamespaceRecord& record) const {
  if (!content_artifacts_ || record.inode.is_dir)
    throw std::runtime_error("file content identity unavailable");
  auto verified = content_artifacts_->verify({record.descriptor_id, record.seal_id}, record.manifest.key,
    record.manifest.format_id, record.manifest.payload_size, record.manifest.layout_size);
  if (!payload_artifacts_) throw std::runtime_error("payload content service unavailable");
  payload_artifacts_->verify(verified->descriptor.payload_root, record.manifest.payload_size,
    false, nullptr, payload_source_identity(record.manifest.key));
  return verified;
}
versioning::NamespaceRecord IoTDBStructuredBlobBackend::decode_namespace_record_unlocked(
    const versioning::Bytes& bytes) const {
  auto record = versioning::decode_namespace_record(bytes);
  if (!record.inode.is_dir) {
    const auto verified = verified_content_unlocked(record);
    record.manifest.metadata = verified->metadata;
    record.manifest.metadata_materialized = true;
    record.manifest.payload_reader = make_payload_reader_unlocked(record.manifest, verified);
    const auto metadata = serialize_metadata(verified->metadata, 65536);
    if (!metadata.ok()) throw std::runtime_error(metadata.error());
    record.inode.metadata = metadata.value();
  }
  return record;
}
std::shared_ptr<const StructuredBlobPayloadReader>
IoTDBStructuredBlobBackend::make_payload_reader_unlocked(const StructuredBlobManifest& manifest,
    std::shared_ptr<const versioning::VerifiedFileContent> verified) const {
  std::optional<NativeFileLocation> location;
  if (manifest.format_id == std::optional<std::string>("hdf5") && native_file_store_) {
    const auto bytes = owner_->get(owner_->view(), versioning::Table::Current, native_file_location_key(manifest.key));
    if (bytes) {
      location = decode_native_file_location(*bytes);
      validate_native_file_binding(*location, *verified);
    }
  }
  return std::make_shared<const OwnerPublishedPayloadReader>(storage_namespace(), manifest,
    owner_, content_artifacts_, payload_artifacts_, std::move(verified), native_file_store_, std::move(location));
}

StructuredBlobResult<std::string> IoTDBStructuredBlobBackend::native_binding_source_identity(
    const StructuredBlobVersionKey& key) const {
  using Result = StructuredBlobResult<std::string>;
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  if (!owner_) return Result::failure(StructuredBlobError::InvalidState, "native binding audit requires owner mode");
  try {
    const auto exact = versioning::exact_key(key.path, key.version);
    const auto bytes = owner_->get(owner_->view(), versioning::Table::Exact, exact);
    if (!bytes) return Result::failure(StructuredBlobError::NotFound, "native binding audit exact version is not published");
    const auto record = versioning::decode_namespace_record(*bytes);
    if (!(record.manifest.key == key) || record.inode.is_dir ||
        record.manifest.format_id != std::optional<std::string>("hdf5"))
      return Result::failure(StructuredBlobError::InvalidState, "native binding audit requires an exact published HDF5 file");
    (void)verified_content_unlocked(record);
    const auto canonical = versioning::encode_namespace_record(record);
    if (canonical != *bytes) return Result::failure(StructuredBlobError::InvalidState, "native binding audit record is not canonical");
    return Result::success(versioning::content_id("native-binding-source", "iotdbfs-native-binding-source/1", canonical));
  } catch (const std::exception& error) { return backend_failure_result<std::string>(error.what()); }
}

StructuredBlobStatus IoTDBStructuredBlobBackend::bind_native_file(const StructuredBlobVersionKey& key) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  if (!owner_ || !native_file_store_ || pending_tx_)
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "native binding requires a configured owner/store and no active namespace mutation");
  try {
    auto transaction = owner_->begin("bind-native-file");
    const auto bytes = transaction->get(versioning::Table::Exact, versioning::exact_key(key.path, key.version));
    if (!bytes) return StructuredBlobStatus::failure(StructuredBlobError::NotFound, "exact version is not published");
    const auto record = versioning::decode_namespace_record(*bytes);
    if (record.inode.is_dir || !(record.manifest.key == key) || record.manifest.format_id != std::optional<std::string>("hdf5"))
      return StructuredBlobStatus::failure(StructuredBlobError::InvalidState, "native binding requires an exact published HDF5 file");
    const auto verified = verified_content_unlocked(record);
    const auto locator_key = native_file_location_key(key);
    if (const auto existing = transaction->get(versioning::Table::Current, locator_key)) {
      const auto location = decode_native_file_location(*existing);
      validate_native_file_binding(location, *verified);
      // Idempotence never silently repairs or replaces a missing/changed file.
      const auto opened = native_file_store_->open(location);
      if (!opened.ok()) return opened.status();
      return StructuredBlobStatus::success();
    }
    auto installed = native_file_store_->install(native_file_identity(*verified),
      [this, verified, key](std::uint64_t offset, std::uint64_t length) {
        try {
          auto batch = payload_artifacts_->read_ranges(verified->descriptor.payload_root,
            verified->descriptor.payload_size, {{offset, length}}, payload_source_identity(key));
          return StructuredBlobResult<StructuredBlobBytes>::success(std::move(batch.front()));
        } catch (const std::exception& error) { return backend_failure_result<StructuredBlobBytes>(error.what()); }
      });
    if (!installed.ok()) return installed.status();
    validate_native_file_binding(installed.value(), *verified);
    transaction->put(versioning::Table::Current, locator_key, encode_native_file_location(installed.value()));
    transaction->commit();
    metadata_generation_.store(owner_->view().head.sequence, std::memory_order_release);
    return StructuredBlobStatus::success();
  } catch (const std::exception& error) { return backend_failure(error.what()); }
}
StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
IoTDBStructuredBlobBackend::bind_published_payload(const StructuredBlobManifest& manifest) const {
  using Result = StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>;
  if (!owner_) return Result::success(nullptr);
  try {
    (void)owner_->view();
    const auto reader = std::dynamic_pointer_cast<const OwnerPublishedPayloadReader>(manifest.payload_reader);
    if (!reader || !reader->matches(storage_namespace(), manifest))
      return Result::failure(StructuredBlobError::InvalidState,
        "owner published manifest lacks its verified payload capability");
    return Result::success(std::move(reader));
  } catch (const std::exception& error) {
    return backend_failure_result<std::shared_ptr<const StructuredBlobPayloadReader>>(error.what());
  }
}
StructuredBlobBytes IoTDBStructuredBlobBackend::read_staging_payload_unlocked(
    const StructuredBlobVersionKey& key, std::uint64_t offset, std::uint64_t length) const {
  if (!length) return {};
  if (length > versioning::kPayloadBlockBytes || offset > UINT64_MAX - length)
    throw std::runtime_error("staging payload range exceeds block bound");
  IoTDBReadObservation observation(true, 1, length);
  auto rows = execute_query("SELECT READ_OBJECT(data," + std::to_string(offset) + ',' +
    std::to_string(length) + ") FROM blob_payload WHERE " + key_predicate(key) + " LIMIT 2", false);
  if (!rows || !publication_has_next(rows)) throw std::runtime_error("staging payload missing");
  const auto fields = publication_next(rows)->fields;
  if (fields.size() != 1 || !fields[0].stringV || fields[0].stringV.value().size() != length ||
      publication_has_next(rows)) throw std::runtime_error("staging payload is malformed or short");
  const auto& value = fields[0].stringV.value();
  observation.success(value.size());
  return {value.begin(), value.end()};
}
StructuredBlobBytes IoTDBStructuredBlobBackend::read_staging_layout_unlocked(
    const StructuredBlobVersionKey& key, std::uint64_t size) const {
  if (size > versioning::kMetadataBytes) throw std::runtime_error("staging layout exceeds metadata bound");
  if (!size) return {};
  auto rows = execute_query("SELECT READ_OBJECT(data,0," + std::to_string(size) +
    ") FROM blob_layout WHERE " + key_predicate(key) + " LIMIT 2", false);
  if (!rows || !publication_has_next(rows)) throw std::runtime_error("staging layout missing");
  const auto fields = publication_next(rows)->fields;
  if (fields.size() != 1 || !fields[0].stringV || fields[0].stringV.value().size() != size ||
      publication_has_next(rows)) throw std::runtime_error("staging layout is malformed or short");
  const auto& value = fields[0].stringV.value();
  return {value.begin(), value.end()};
}
void IoTDBStructuredBlobBackend::cleanup_staging_artifacts_unlocked(const StructuredBlobVersionKey& key) const noexcept {
  // The head has committed. Even SQL construction/diagnostic allocation must
  // not escape and turn a cleanup orphan into a reported failed publication.
  auto failed = [&]() noexcept {
    staging_cleanup_failures_.fetch_add(1, std::memory_order_relaxed);
    try {
      query_profile_count("content_artifacts.staging_cleanup_failures");
      LOG_ERROR("V04 committed staging artifact cleanup failed path=" + key.path);
    } catch (...) {}
  };
  try {
    auto cleanup = [&](const std::string& sql) {
      try { if (execute_nonquery(sql)) return; } catch (...) {}
      failed();
    };
    for (const auto* table : {"blob_payload", "blob_layout", "blob_dataset_catalog", "blob_metadata_set"})
      cleanup(std::string("DELETE FROM ") + table + " WHERE " + key_predicate(key));
    // Owner readers hydrate metadata from the committed descriptor; SQL inode
    // metadata and its set index serve only the unpublished lifecycle here.
    cleanup("INSERT INTO inode(time,path,metadata) VALUES(" + std::to_string(key.version) +
      ",'" + escape_sql(path_utils::norm(key.path)) + "','{}')");
  } catch (...) { failed(); }
}

StructuredBlobResult<std::optional<std::string>> IoTDBStructuredBlobBackend::verified_layout_identity(
    const StructuredBlobVersionKey& key) const {
  using Result = StructuredBlobResult<std::optional<std::string>>;
  if (!owner_) return Result::success(std::nullopt);
  try {
    std::unique_ptr<ScopedQueryMetadataBudget> scope;
    if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
      scope = std::make_unique<ScopedQueryMetadataBudget>();
    const auto encoded = namespace_record_unlocked(key, false);
    if (!encoded) return Result::failure(StructuredBlobError::NotFound, "version is not visible");
    auto verified = verified_content_unlocked(versioning::decode_namespace_record(*encoded));
    require_query_metadata("layout-demand:"+verified->seal.identity.namespace_id+
      verified->seal.identity.incarnation+":"+std::to_string(key.path.size())+":"+key.path+":"+
      std::to_string(key.version)+":"+verified->descriptor.layout_id,verified->descriptor.layout_size);
    return Result::success(content_artifacts_->layout_identity(*verified));
  } catch (const std::exception& error) { return Result::failure(StructuredBlobError::BackendFailure, error.what()); }
}

std::optional<Inode> IoTDBStructuredBlobBackend::load_staging_inode_unlocked(const StructuredBlobVersionKey& key) const {
  auto rows = execute_query("SELECT " + std::string(kInodeProjection) + " FROM inode WHERE " + key_predicate(key) + " LIMIT 1");
  if (!rows || !publication_has_next(rows)) return std::nullopt;
  Inode inode;
  if (!parse_inode_fields(publication_next(rows)->fields, inode)) return std::nullopt;
  inode.path = path_utils::norm(inode.path); inode.parent = path_utils::norm(inode.parent);
  return inode;
}
StructuredBlobResult<std::vector<Inode>> IoTDBStructuredBlobBackend::enumerate_namespace_subtree(const std::string& path) const {
  try {
    if (!owner_) return backend_failure_result<std::vector<Inode>>("namespace owner required");
    const auto root = path_utils::norm(path);
    std::vector<Inode> result;
    for (const auto& bytes : namespace_records_unlocked()) {
      auto inode = namespace_identity(bytes).inode;
      if (root == "/" || inode.path == root || inode.path.compare(0, root.size()+1, root+"/")==0)
        result.push_back(std::move(inode));
    }
    return StructuredBlobResult<std::vector<Inode>>::success(std::move(result));
  } catch (const std::exception& error) { return backend_failure_result<std::vector<Inode>>(error.what()); }
}

bool IoTDBStructuredBlobBackend::ensure_schema() {
  const std::string inode_sql =
    std::string("CREATE TABLE IF NOT EXISTS inode(") +
    " time TIMESTAMP TIME,"
    " path STRING TAG,"
    " parent STRING FIELD,"
    " name STRING FIELD,"
    " is_dir BOOLEAN FIELD,"
    " size INT64 FIELD,"
    " mode INT32 FIELD,"
    " uid INT32 FIELD,"
    " gid INT32 FIELD,"
    " nlink INT32 FIELD,"
    " ctime TIMESTAMP FIELD,"
    " mtime TIMESTAMP FIELD,"
    " atime TIMESTAMP FIELD,"
    " format_id STRING FIELD,"
    " metadata STRING FIELD,"
    " layout_size INT64 FIELD,"
    " blob_phase INT32 FIELD,"
    " published BOOLEAN FIELD,"
    " blob_revision INT64 FIELD)";
  const std::string payload_sql =
    "CREATE TABLE IF NOT EXISTS blob_payload("
    " time TIMESTAMP TIME,"
    " path STRING TAG,"
    " data OBJECT FIELD,"
    " size INT64 FIELD,"
    " sealed BOOLEAN FIELD)";
  const std::string layout_sql =
    "CREATE TABLE IF NOT EXISTS blob_layout("
    " time TIMESTAMP TIME,"
    " path STRING TAG,"
    " data OBJECT FIELD,"
    " size INT64 FIELD,"
    " sealed BOOLEAN FIELD)";
  const std::string metadata_set_sql =
    "CREATE TABLE IF NOT EXISTS blob_metadata_set("
    " time TIMESTAMP TIME,"
    " path STRING TAG,"
    " field_id STRING TAG,"
    " member_value STRING TAG,"
    " member_type STRING TAG,"
    " present BOOLEAN FIELD)";
  const std::string version_identity_sql =
    "CREATE TABLE IF NOT EXISTS blob_version_identity("
    " time TIMESTAMP TIME,"
    " path STRING TAG,"
    " retired BOOLEAN FIELD)";
  const std::string catalog_sql = "CREATE TABLE IF NOT EXISTS blob_dataset_catalog("
    "time TIMESTAMP TIME,path STRING TAG,data OBJECT FIELD,size INT64 FIELD,"
    "protocol STRING FIELD,layout_digest STRING FIELD,layout_protocol STRING FIELD,"
    "directory_complete BOOLEAN FIELD,record_count INT64 FIELD,attribute_count INT64 FIELD)";
  const std::string dataset_sql = "CREATE TABLE IF NOT EXISTS blob_dataset_entry("
    "time TIMESTAMP TIME,path STRING TAG,object_id STRING TAG,"
    "dataset_path STRING FIELD,attributes_complete BOOLEAN FIELD)";
  const std::string attribute_sql = "CREATE TABLE IF NOT EXISTS blob_dataset_attribute("
    "time TIMESTAMP TIME,path STRING TAG,object_id STRING TAG,attribute_name STRING TAG,"
    "state INT32 FIELD,value_type STRING FIELD,svalue STRING FIELD,"
    "ivalue INT64 FIELD,fvalue DOUBLE FIELD,bvalue BOOLEAN FIELD)";
  return execute_nonquery(inode_sql) && execute_nonquery(payload_sql) &&
    execute_nonquery(layout_sql) && execute_nonquery(metadata_set_sql) &&
    execute_nonquery(version_identity_sql) && execute_nonquery(catalog_sql) &&
    execute_nonquery(dataset_sql) && execute_nonquery(attribute_sql);
}

std::optional<Inode> IoTDBStructuredBlobBackend::load_inode(
  const std::string& path, std::optional<long long> version) const {
  std::unique_lock<std::recursive_mutex> lock(mutation_mu_, std::defer_lock);
  if (!owner_) lock.lock();
  const auto normalized = path_utils::norm(path);
  if (owner_) {
    const auto view = owner_->view();
    auto record = version ? owner_->get(view, versioning::Table::Exact,
      versioning::exact_key(normalized, *version)) :
      owner_->get(view, versioning::Table::Current, normalized);
    if (!record) return std::nullopt;
    return namespace_identity(*record).inode;
  }
  std::ostringstream sql;
  sql << "SELECT " << kInodeProjection
      << " FROM inode WHERE published=true AND path='"
      << escape_sql(normalized) << "'";
  if (version) sql << " AND time=" << *version;
  sql << " ORDER BY time DESC,mtime DESC LIMIT 1";
  auto rows = execute_query(sql.str());
  if (!rows || !publication_has_next(rows)) return std::nullopt;
  auto row = publication_next(rows);
  Inode inode;
  if (!parse_inode_fields(row->fields, inode)) return std::nullopt;
  inode.path = path_utils::norm(inode.path);
  inode.parent = path_utils::norm(inode.parent);
  if (inode.name.empty()) inode.name = path_utils::base(inode.path);
  return inode;
}

std::vector<Inode> IoTDBStructuredBlobBackend::load_children(
  const std::string& directory, bool latest_only) const {
  std::unique_lock<std::recursive_mutex> lock(mutation_mu_, std::defer_lock);
  if (!owner_) lock.lock();
  const auto normalized = path_utils::norm(directory);
  if (owner_) {
    std::map<std::string, Inode> latest;
    std::vector<Inode> result;
    for (const auto& bytes : namespace_records_unlocked()) {
      auto inode = namespace_identity(bytes).inode;
      if (inode.path == normalized || inode.parent != normalized) continue;
      if (!latest_only) result.push_back(inode);
      else if (!latest.count(inode.path) || latest.at(inode.path).row_time < inode.row_time)
        latest[inode.path] = std::move(inode);
    }
    if (latest_only) for (auto& item : latest) result.push_back(std::move(item.second));
    // Version-directory listing skips the first row of each path as its
    // current version. Exact keys encode ascending versions, so restore the
    // established latest-first ordering before returning all versions.
    if (!latest_only) std::sort(result.begin(), result.end(), [](const Inode& a, const Inode& b) {
      return a.path != b.path ? a.path < b.path : a.row_time > b.row_time;
    });
    return result;
  }
  const std::string sql = "SELECT " + std::string(kInodeProjection) +
    " FROM inode WHERE published=true AND path!='" +
    escape_sql(normalized) + "' AND parent='" + escape_sql(normalized) +
    "' ORDER BY time DESC";
  std::vector<Inode> result;
  std::unordered_set<std::string> seen;
  auto rows = execute_query(sql);
  while (rows && publication_has_next(rows)) {
    auto row = publication_next(rows);
    Inode inode;
    if (!parse_inode_fields(row->fields, inode)) continue;
    inode.path = path_utils::norm(inode.path);
    inode.parent = path_utils::norm(inode.parent);
    if (inode.name.empty()) inode.name = path_utils::base(inode.path);
    if (latest_only && !seen.insert(inode.path).second) continue;
    result.push_back(std::move(inode));
  }
  return result;
}

bool IoTDBStructuredBlobBackend::upsert_inode(
  const Inode& inode, bool publish_namespace_entry) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  // Only directory namespace entries may bypass the blob publication gate.
  if (publish_namespace_entry && !inode.is_dir) return false;
  auto metadata = deserialize_metadata(inode.metadata);
  if (!metadata.ok()) return false;
  const StructuredBlobVersionKey key{
    path_utils::norm(inode.path), inode.row_time};
  auto existing = load_manifest_unlocked(key);
  if (!existing.ok() && existing.status().code() != StructuredBlobError::NotFound)
    return false;
  const auto encoded_metadata = serialize_metadata(metadata.value(), 65536);
  if (!encoded_metadata.ok()) return false;
  if (existing.ok() && (existing.value().published ||
      existing.value().phase == StructuredBlobPhase::Published)) {
    std::optional<Inode> original_inode;
    if (owner_) {
      const auto bytes = namespace_record_unlocked(key, true);
      if (bytes) original_inode = decode_namespace_record_unlocked(*bytes).inode;
    } else original_inode = load_inode(key.path, key.version);
    if (!original_inode || original_inode->is_dir != inode.is_dir) return false;
  }
  if (existing.ok() && !inode.is_dir &&
      existing.value().phase == StructuredBlobPhase::Described) {
    const auto original = serialize_metadata(existing.value().metadata, 65536);
    if (!original.ok() || original.value() != encoded_metadata.value() ||
        inode.size < 0 || static_cast<std::uint64_t>(inode.size) != existing.value().payload_size ||
        inode.format_id != existing.value().format_id.value_or("")) return false;
  }
  if (existing.ok() && (existing.value().published ||
      existing.value().phase == StructuredBlobPhase::Published) && !inode.is_dir) {
    const auto original = serialize_metadata(existing.value().metadata, 65536);
    if (publish_namespace_entry || inode.size < 0 ||
        static_cast<std::uint64_t>(inode.size) != existing.value().payload_size ||
        inode.format_id != existing.value().format_id.value_or("") ||
        !original.ok() || original.value() != encoded_metadata.value()) return false;
  }
  // An unpublished file version cannot be overwritten by a directory entry.
  if (existing.ok() && inode.is_dir) {
    if (!existing.value().published || existing.value().phase != StructuredBlobPhase::Published)
      return false;
  }
  if (owner_ && (inode.is_dir || (existing.ok() && existing.value().published))) {
    const bool standalone = !pending_tx_;
    try {
      if (standalone) pending_tx_ = owner_->begin("inode-permission-or-directory");
      Inode normalized = inode;
      normalized.path = key.path; normalized.parent = path_utils::parent_of(key.path);
      normalized.metadata = encoded_metadata.value();
      StructuredBlobManifest manifest;
      if (existing.ok()) manifest = existing.value();
      else {
        manifest.key = key; manifest.published = true;
        manifest.phase = StructuredBlobPhase::Published;
        manifest.payload_size = inode.size; manifest.metadata = metadata.value();
        if (!inode.format_id.empty()) manifest.format_id = inode.format_id;
      }
      put_namespace_record_unlocked(normalized, manifest);
      return !standalone || commit_namespace_unlocked();
    } catch (const std::exception& error) {
      if (standalone) abort_namespace_unlocked();
      LOG_ERROR(std::string("namespace inode mutation failed: ") + error.what());
      return false;
    }
  }
  // Invalidate before mutable SQL: an ambiguous/partial failure must not leave
  // older permission evidence usable. A new scan waits for this mutation.
  if (!owner_) metadata_generation_.fetch_add(1, std::memory_order_acq_rel);
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_path(key.path, key.version));
  if (!replace_metadata_set_members(key, metadata.value()).ok()) return false;
  std::ostringstream sql;
  sql << "INSERT INTO inode(time,path,parent,name,is_dir,size,mode,uid,gid,"
         "nlink,ctime,mtime,atime,format_id,metadata";
  if (publish_namespace_entry)
    sql << ",layout_size,blob_phase,published,blob_revision";
  sql << ") VALUES(" << inode.row_time << ",'"
      << escape_sql(path_utils::norm(inode.path)) << "','"
      << escape_sql(path_utils::norm(inode.parent)) << "','"
      << escape_sql(inode.name) << "',"
      << (inode.is_dir ? "true" : "false") << ','
      << inode.size << ',' << inode.mode << ',' << inode.uid << ','
      << inode.gid << ',' << inode.nlink << ',' << inode.ctime << ','
      << inode.mtime << ',' << inode.atime << ",'"
      << escape_sql(inode.format_id) << "','"
      << escape_sql(encoded_metadata.value()) << "'";
  if (publish_namespace_entry) {
    sql << ",0," << phase_value(StructuredBlobPhase::Published)
        << ",true,0";
  }
  sql << ')';
  return execute_nonquery(sql.str());
}

StructuredBlobResult<bool>
IoTDBStructuredBlobBackend::version_key_retired_unlocked(
    const StructuredBlobVersionKey& key) const {
  if (owner_) {
    try {
      const auto exact = versioning::exact_key(key.path, key.version);
      auto value = pending_tx_ ? pending_tx_->get(versioning::Table::Liveness, exact) :
        owner_->get(owner_->view(), versioning::Table::Liveness, exact);
      return StructuredBlobResult<bool>::success(value && *value == versioning::Bytes{0});
    } catch (const std::exception& error) { return backend_failure_result<bool>(error.what()); }
  }
  const std::string sql = "SELECT retired FROM blob_version_identity WHERE " +
    key_predicate(key) + " LIMIT 1";
  auto rows = execute_query(sql);
  if (!rows)
    return backend_failure_result<bool>(
      "failed to check structured blob version identity");
  if (!publication_has_next(rows)) return StructuredBlobResult<bool>::success(false);
  auto row = publication_next(rows);
  if (row->fields.size() != 1 || !row->fields[0].boolV)
    return backend_failure_result<bool>(
      "structured blob version identity row is malformed");
  return StructuredBlobResult<bool>::success(row->fields[0].boolV.value());
}

bool IoTDBStructuredBlobBackend::retire_published_versions_unlocked(
    const std::string& predicate) {
  const std::string sql =
    "SELECT time,path FROM inode WHERE published=true AND is_dir=false AND (" +
    predicate + ")";
  auto rows = execute_query(sql);
  if (!rows) return false;

  std::vector<StructuredBlobVersionKey> versions;
  while (publication_has_next(rows)) {
    auto row = publication_next(rows);
    if (row->fields.size() != 2 || !row->fields[0].longV ||
        !row->fields[1].stringV) {
      LOG_ERROR("published File Version identity row is malformed");
      return false;
    }
    versions.push_back({path_utils::norm(row->fields[1].stringV.value()),
      row->fields[0].longV.value()});
  }
  rows.reset(); // Release the read session before writing retirement rows.
  if (versions.empty()) return true;

  static const std::vector<std::pair<std::string, TSDataType::TSDataType>>
    schema = {{"path", TSDataType::STRING},
      {"retired", TSDataType::BOOLEAN}};
  static const std::vector<ColumnCategory> categories = {
    ColumnCategory::TAG, ColumnCategory::FIELD};
  Tablet tablet("blob_version_identity", schema, categories,
    versions.size());
  for (const auto& version : versions) {
    const int row = tablet.rowSize++;
    tablet.addTimestamp(row, version.version);
    tablet.addValue(0, row, version.path);
    tablet.addValue(1, row, true);
  }
  return insert_tablet(tablet);
}

bool IoTDBStructuredBlobBackend::remove_path(
  const std::string& path, std::optional<long long> version) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  const std::string normalized = path_utils::norm(path);
  if (owner_) {
    const bool standalone = !pending_tx_;
    try {
      if (standalone) pending_tx_ = owner_->begin("remove-path");
      std::set<std::string> affected;
      for (const auto& bytes : namespace_records_unlocked(true)) {
        const auto record = namespace_identity(bytes);
        if (record.inode.path != normalized || (version && record.inode.row_time != *version)) continue;
        const auto exact = versioning::exact_key(normalized, record.inode.row_time);
        pending_tx_->erase(versioning::Table::Exact, exact);
        pending_tx_->erase(versioning::Table::Permission, exact);
        pending_tx_->put(versioning::Table::Liveness, exact, versioning::Bytes{0});
        pending_records_[exact] = std::nullopt;
        affected.insert(record.inode.path); affected.insert(current_format_key(record.inode));
      }
      for (const auto& current : affected) {
        pending_tx_->erase(versioning::Table::Current, current);
        std::optional<versioning::NamespaceRecord> latest;
        for (const auto& bytes : namespace_records_unlocked(true)) {
          auto record = namespace_identity(bytes);
          if ((record.inode.path == current || current_format_key(record.inode) == current) &&
              (!latest || latest->inode.row_time < record.inode.row_time)) latest = std::move(record);
        }
        if (latest) pending_tx_->put(versioning::Table::Current, current,
          versioning::encode_namespace_record(*latest));
      }
      return !standalone || commit_namespace_unlocked();
    } catch (const std::exception& error) {
      if (standalone) abort_namespace_unlocked();
      LOG_ERROR(std::string("namespace remove failed: ") + error.what()); return false;
    }
  }
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_path(normalized, version));
  std::string predicate = "path='" + escape_sql(normalized) + "'";
  if (version) predicate += " AND time=" + std::to_string(*version);
  if (!retire_published_versions_unlocked(predicate)) {
    LOG_ERROR("failed to retire published File Version identities for " +
      normalized);
    return false;
  }
  // Retirement is durable before visibility is removed. Visibility is then
  // removed before artifacts, so failed cleanup can only leave unreachable
  // orphans, never a visible inode pointing at missing bytes.
  if (!owner_) metadata_generation_.fetch_add(1, std::memory_order_acq_rel);
  const bool hidden = execute_nonquery("DELETE FROM inode WHERE " + predicate);
  const bool payload = execute_nonquery(
    "DELETE FROM blob_payload WHERE " + predicate);
  const bool layout = execute_nonquery(
    "DELETE FROM blob_layout WHERE " + predicate);
  const bool metadata_set = execute_nonquery(
    "DELETE FROM blob_metadata_set WHERE " + predicate);
  const bool catalog = execute_nonquery("DELETE FROM blob_dataset_catalog WHERE " + predicate);
  const bool datasets = execute_nonquery("DELETE FROM blob_dataset_entry WHERE " + predicate);
  const bool attributes = execute_nonquery("DELETE FROM blob_dataset_attribute WHERE " + predicate);
  if (hidden && (!payload || !layout || !metadata_set || !catalog || !datasets || !attributes))
    LOG_WARN("structured blob delete left unreachable artifact rows for " +
      normalized);
  return hidden;
}

bool IoTDBStructuredBlobBackend::remove_tree(const std::string& directory) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  const auto normalized = path_utils::norm(directory);
  if (owner_) {
    const bool standalone = !pending_tx_;
    try {
      if (standalone) pending_tx_ = owner_->begin("remove-tree");
      std::set<std::string> paths;
      for (const auto& bytes : namespace_records_unlocked(true)) {
        const auto record = namespace_identity(bytes);
        if (normalized == "/" || record.inode.path == normalized ||
            record.inode.path.compare(0, normalized.size()+1, normalized+"/") == 0)
          paths.insert(record.inode.path);
      }
      for (const auto& path : paths) if (!remove_path(path)) throw std::runtime_error("subtree mutation failed");
      return !standalone || commit_namespace_unlocked();
    } catch (const std::exception& error) {
      if (standalone) abort_namespace_unlocked();
      LOG_ERROR(std::string("namespace subtree mutation failed: ") + error.what()); return false;
    }
  }
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_tree(normalized));
  const std::string predicate = "path='" + escape_sql(normalized) +
    "' OR " + query_utils::like_subtree_predicate(normalized);
  // IoTDB accepts LIKE for SELECT but only exact tag predicates for DELETE.
  // Enumerate immutable keys before hiding anything; never issue a broad
  // unsupported DELETE and leave newly added catalog artifacts visible.
  auto rows = execute_query("SELECT path FROM inode WHERE " + predicate);
  if (!rows) return false;
  std::set<std::string> paths;
  while (publication_has_next(rows)) {
    auto fields = publication_next(rows)->fields;
    if (fields.size() != 1 || !fields[0].stringV) return false;
    const auto path = path_utils::norm(fields[0].stringV.value());
    if (normalized != "/" && path != normalized &&
        path.compare(0, normalized.size() + 1, normalized + "/") != 0)
      return false;
    paths.insert(path);
  }
  rows.reset(); // Do not pin a session across the nested retirement query.
  if (!retire_published_versions_unlocked(predicate)) return false;
  // Hide all source versions before reclaiming any of the artifact tables.
  // Invalidate before the first hide, including failure after partial removal.
  if (!paths.empty()) metadata_generation_.fetch_add(1, std::memory_order_acq_rel);
  for (const auto& path : paths)
    if (!execute_nonquery("DELETE FROM inode WHERE path='" + escape_sql(path) + "'"))
      return false;
  for (const auto& path : paths) {
    const auto exact = "path='" + escape_sql(path) + "'";
    for (const char* table : {"blob_payload", "blob_layout", "blob_metadata_set",
         "blob_dataset_catalog", "blob_dataset_entry", "blob_dataset_attribute"})
      if (!execute_nonquery(std::string("DELETE FROM ") + table + " WHERE " + exact))
        LOG_WARN("structured blob subtree cleanup left unreachable artifacts for " + path);
  }
  return true;
}

StructuredBlobStatus IoTDBStructuredBlobBackend::create_manifest(
  const StructuredBlobManifest& manifest) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto retired = version_key_retired_unlocked(manifest.key);
  if (!retired.ok()) return retired.status();
  if (retired.value()) {
    return StructuredBlobStatus::failure(StructuredBlobError::AlreadyExists,
      "published structured blob version identity cannot be reused");
  }
  auto existing = load_manifest_unlocked(manifest.key);
  if (existing.ok()) {
    return StructuredBlobStatus::failure(StructuredBlobError::AlreadyExists,
      "structured blob version already exists");
  }
  if (existing.status().code() != StructuredBlobError::NotFound)
    return existing.status();
  const std::string sql = "INSERT INTO inode(time,path,is_dir,size,layout_size,"
    "blob_phase,published,blob_revision,format_id,metadata) VALUES(" +
    std::to_string(manifest.key.version) + ",'" +
    escape_sql(path_utils::norm(manifest.key.path)) + "',false,0,0," +
    std::to_string(phase_value(manifest.phase)) + ",false,0,'','')";
  return execute_nonquery(sql) ? StructuredBlobStatus::success() :
    backend_failure("failed to create structured blob manifest");
}

StructuredBlobResult<StructuredBlobManifest>
IoTDBStructuredBlobBackend::load_manifest_unlocked(
  const StructuredBlobVersionKey& key) const {
  if (owner_) {
    try {
      auto bytes = namespace_record_unlocked(key, true);
      if (bytes) return StructuredBlobResult<StructuredBlobManifest>::success(
        decode_namespace_record_unlocked(*bytes).manifest);
      auto retired = version_key_retired_unlocked(key);
      if (!retired.ok()) return backend_failure_result<StructuredBlobManifest>(retired.status().message());
      if (retired.value()) return not_found_result("version identity is retired");
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobManifest>(error.what()); }
  }
  const std::string sql =
    "SELECT format_id,metadata,size,layout_size,blob_phase,published,"
    "blob_revision FROM inode WHERE " + key_predicate(key) + " LIMIT 1";
  auto rows = execute_query(sql);
  if (!rows) return backend_failure_result<StructuredBlobManifest>(
    "failed to load structured blob manifest");
  if (!publication_has_next(rows)) return not_found_result(
    "structured blob manifest was not found");
  auto row = publication_next(rows);
  const auto& fields = row->fields;
  if (fields.size() != 7 || !fields[2].longV || !fields[3].longV ||
      !fields[4].intV || !fields[5].boolV || !fields[6].longV ||
      fields[2].longV.value() < 0 || fields[3].longV.value() < 0 ||
      fields[6].longV.value() < 0) {
    return backend_failure_result<StructuredBlobManifest>(
      "structured blob manifest row is malformed");
  }
  auto phase = parse_phase(fields[4].intV.value());
  if (!phase) return backend_failure_result<StructuredBlobManifest>(
    "structured blob manifest phase is invalid");
  StructuredBlobManifest manifest;
  manifest.key = key;
  const std::string format =
    fields[0].stringV ? fields[0].stringV.value() : "";
  if (!format.empty()) manifest.format_id = format;
  manifest.payload_size =
    static_cast<std::uint64_t>(fields[2].longV.value());
  manifest.layout_size =
    static_cast<std::uint64_t>(fields[3].longV.value());
  manifest.phase = *phase;
  if (owner_ && *phase == StructuredBlobPhase::Published)
    return not_found_result("uncommitted publication is not visible");
  manifest.published = fields[5].boolV.value();
  manifest.revision =
    static_cast<std::uint64_t>(fields[6].longV.value());
  // Metadata may have been written ahead of its manifest CAS. It becomes
  // durable lifecycle state only once the Described frontier is visible.
  if (phase_value(manifest.phase) >=
      phase_value(StructuredBlobPhase::Described)) {
    const std::string metadata_json =
      fields[1].stringV ? fields[1].stringV.value() : "";
    auto metadata = deserialize_metadata(metadata_json);
    if (!metadata.ok()) return backend_failure_result<StructuredBlobManifest>(
      "structured blob metadata is invalid: " + metadata.error());
    manifest.metadata = metadata.value();
  }
  return StructuredBlobResult<StructuredBlobManifest>::success(
    std::move(manifest));
}

StructuredBlobResult<StructuredBlobManifest>
IoTDBStructuredBlobBackend::load_manifest(
  const StructuredBlobVersionKey& key) const {
  if (owner_) {
    try {
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
        scope = std::make_unique<ScopedQueryMetadataBudget>();
      // Committed content is immutable and does not require the staging lock.
      auto bytes = namespace_record_unlocked(key, false);
      if (bytes) return StructuredBlobResult<StructuredBlobManifest>::success(
        decode_namespace_record_unlocked(*bytes).manifest);
      std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
      if (pending_records_.count(versioning::exact_key(key.path, key.version)))
        return not_found_result("publication is not yet committed");
      return load_manifest_unlocked(key);
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobManifest>(error.what()); }
  }
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  return load_manifest_unlocked(key);
}

StructuredBlobResult<std::vector<StructuredBlobManifest>>
IoTDBStructuredBlobBackend::load_published_manifests() const {
  return load_published_manifests(SourceSelector{});
}

StructuredBlobResult<std::vector<StructuredBlobManifest>>
IoTDBStructuredBlobBackend::load_published_manifests(
    const SourceSelector& selector) const {
  auto snapshot = load_published_snapshot(selector, false);
  if (!snapshot.ok())
    return StructuredBlobResult<std::vector<StructuredBlobManifest>>::failure(
      snapshot.status().code(), snapshot.status().message());
  return StructuredBlobResult<std::vector<StructuredBlobManifest>>::success(
    std::move(snapshot.value().manifests));
}

StructuredBlobResult<PublishedVersionSnapshot>
IoTDBStructuredBlobBackend::load_published_snapshot(
    const SourceSelector& selector, bool include_ancestors) const {
  if (owner_) {
    try {
      auto valid = validate_source_selector(selector);
      if (!valid.ok()) return StructuredBlobResult<PublishedVersionSnapshot>::failure(
        StructuredBlobError::InvalidArgument, valid.message());
      const auto snapshot = version_snapshot(selector);
      const auto self = weak_from_this();
      if (self.expired()) throw std::runtime_error("owner catalog requires shared backend lifetime");
      auto permissions = std::make_shared<SourcePermissionSnapshot>();
      permissions->storage_namespace = storage_namespace();
      permissions->metadata_generation = snapshot->view.head.sequence;
      permissions->observed_at = std::chrono::steady_clock::now();
      std::vector<StructuredBlobManifest> manifests;
      manifests.reserve(snapshot->sources.size());
      for (const auto& key : snapshot->sources) {
        const auto& record=snapshot->records.at(key);
        const auto& m=record.manifest;const auto& inode=record.inode;
        permissions->sources.emplace(m.key,CatalogSourcePermission{m.format_id.value_or(""),
          m.revision,m.payload_size,m.layout_size,inode.mode,inode.uid,inode.gid,false});
        if (include_ancestors) for (auto parent=path_utils::parent_of(inode.path);;parent=path_utils::parent_of(parent)) {
          if (!permissions->parents.count(parent)) {
            const auto found=snapshot->latest.find(parent);
            if (found==snapshot->latest.end()) permissions->parents[parent]=std::nullopt;
            else {const auto& p=snapshot->records.at(found->second).inode;
              permissions->parents[parent]=CatalogParentPermission{p.row_time,p.mode,p.uid,p.gid,p.is_dir};}
          }
          if (parent=="/") break;
        }
        manifests.push_back(m);
      }
      return StructuredBlobResult<PublishedVersionSnapshot>::success(PublishedVersionSnapshot{
        std::move(manifests),std::move(permissions),
        std::make_shared<VersionDirectoryReadContext>(self,snapshot)});
    } catch(const std::exception& error) {return backend_failure_result<PublishedVersionSnapshot>(error.what());}
  }
  auto compiled = compile_published_catalog_sql(selector);
  if (!compiled.ok())
    return StructuredBlobResult<PublishedVersionSnapshot>::failure(
      StructuredBlobError::InvalidArgument, compiled.status().message());
  std::set<std::string> source_paths, ancestors;
  if (include_ancestors && kCombineExactCatalogAncestors &&
      !selector.paths.empty() && selector.paths.size() <= kMaxInodePermissionPathsPerQuery &&
      std::all_of(selector.paths.begin(), selector.paths.end(),
        [](const auto& scope) { return scope.kind == PathScopeKind::Exact; })) {
    bool bounded = true;
    for (const auto& scope : selector.paths.normalized_paths()) {
      const auto& path = scope.path;
      if (path.size() > 4096) { bounded = false; break; }
      source_paths.insert(path);
      for (auto parent = path_utils::parent_of(path);;
           parent = path_utils::parent_of(parent)) {
        ancestors.insert(parent);
        if (ancestors.size() > kMaxInodePermissionPathsPerQuery) {
          bounded = false; break;
        }
        if (parent == "/") break;
      }
      if (!bounded) break;
    }
    // A requested source can itself be another source's parent. Leave that
    // case to the bulk authorization reader rather than mixing row identities.
    const bool overlap = std::any_of(source_paths.begin(), source_paths.end(),
      [&](const auto& path) { return ancestors.count(path) != 0; });
    auto combined = (overlap || !bounded) ? QueryResult<std::string>::failure(
      QueryStatus::failure(QueryError::UnsupportedCapability, QueryPhase::Catalog,
        "catalog sources and ancestors cannot be combined")) :
      compile_published_catalog_sql(selector,
        std::vector<std::string>(ancestors.begin(), ancestors.end()));
    if (combined.ok()) compiled = std::move(combined);
    else { source_paths.clear(); ancestors.clear(); }
  }
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  QueryProfileTimer timer("published_catalog_scan");
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = storage_namespace();
  permissions->metadata_generation = metadata_generation();
  permissions->observed_at = std::chrono::steady_clock::now();
  for (const auto& path : ancestors) permissions->parents.emplace(path, std::nullopt);
  if (!ancestors.empty()) query_profile_count("published_catalog_combined_ancestor_queries");
  auto rows = execute_query(compiled.value());
  if (!rows)
    return backend_failure_result<PublishedVersionSnapshot>(
      "failed to load published File Version catalog snapshot");

  std::vector<StructuredBlobManifest> manifests;
  std::set<std::string> observed_parents;
  while (publication_has_next(rows)) {
    auto row = publication_next(rows);
    const auto& fields = row->fields;
    if (fields.size() != 13 || !fields[0].longV || !fields[1].stringV)
      return backend_failure_result<PublishedVersionSnapshot>(
        "published File Version catalog row identity is malformed");
    const auto path = path_utils::norm(fields[1].stringV.value());
    if (ancestors.count(path)) {
      query_profile_count("published_catalog_ancestor_rows");
      if (!observe_latest_parent_permission(*permissions, observed_parents, path,
          fields[0].longV.value(),
          fields[9].intV ? std::optional<int>(fields[9].intV.value()) : std::nullopt,
          fields[10].intV ? std::optional<int>(fields[10].intV.value()) : std::nullopt,
          fields[11].intV ? std::optional<int>(fields[11].intV.value()) : std::nullopt,
          fields[12].boolV ? std::optional<bool>(fields[12].boolV.value()) : std::nullopt))
        return backend_failure_result<PublishedVersionSnapshot>(
          "catalog ancestor row identity is malformed");
      continue; // Directory/blob metadata must not enter source validation.
    }
    if (!ancestors.empty() && !source_paths.count(path))
      return backend_failure_result<PublishedVersionSnapshot>(
        "combined catalog returned an unexpected source path");
    if (fields.size() != 13 || !fields[0].longV || !fields[1].stringV ||
        !fields[4].longV || !fields[5].longV || !fields[6].intV ||
        !fields[7].boolV || !fields[8].longV ||
        fields[0].longV.value() < 0 || fields[4].longV.value() < 0 ||
        fields[5].longV.value() < 0 || fields[8].longV.value() < 0) {
      return backend_failure_result<PublishedVersionSnapshot>(
        "published File Version catalog row is malformed");
    }
    auto phase = parse_phase(fields[6].intV.value());
    if (!phase || *phase != StructuredBlobPhase::Published ||
        !fields[7].boolV.value()) {
      return backend_failure_result<PublishedVersionSnapshot>(
        "published File Version catalog row violates lifecycle state");
    }

    StructuredBlobManifest manifest;
    manifest.key = {path_utils::norm(fields[1].stringV.value()),
      fields[0].longV.value()};
    const std::string format = fields[2].stringV
      ? fields[2].stringV.value() : "";
    if (!format.empty()) manifest.format_id = format;
    const std::string metadata_json = fields[3].stringV
      ? fields[3].stringV.value() : "";
    auto metadata = deserialize_metadata(metadata_json);
    if (!metadata.ok())
      return backend_failure_result<PublishedVersionSnapshot>(
        "published File Version metadata is invalid: " + metadata.error());
    manifest.metadata = std::move(metadata.value());
    manifest.payload_size = static_cast<std::uint64_t>(
      fields[4].longV.value());
    manifest.layout_size = static_cast<std::uint64_t>(
      fields[5].longV.value());
    manifest.phase = *phase;
    manifest.published = true;
    manifest.revision = static_cast<std::uint64_t>(
      fields[8].longV.value());
    if (!manifest.format_id &&
        (!manifest.metadata.empty() || manifest.layout_size != 0)) {
      return backend_failure_result<PublishedVersionSnapshot>(
        "published byte pass-through catalog row has format-derived state");
    }
    // Permissions are mutable namespace data. Missing/unusable fields do not
    // invalidate the content inventory, but cannot authorize a query.
    if (fields[9].intV && fields[10].intV && fields[11].intV &&
        fields[12].boolV && fields[9].intV.value() >= 0 &&
        fields[10].intV.value() >= 0 && fields[11].intV.value() >= 0 &&
        !fields[12].boolV.value()) {
      permissions->sources.emplace(manifest.key, CatalogSourcePermission{
        manifest.format_id.value_or(""), manifest.revision,
        manifest.payload_size, manifest.layout_size, fields[9].intV.value(),
        fields[10].intV.value(), fields[11].intV.value(), false});
      query_profile_count("published_catalog_permission_rows");
    }
    manifests.push_back(std::move(manifest));
  }
  query_profile_count("published_catalog_manifest_rows", manifests.size());
  return StructuredBlobResult<PublishedVersionSnapshot>::success(
    PublishedVersionSnapshot{std::move(manifests), std::move(permissions)});
}

StructuredBlobResult<std::shared_ptr<const SourcePermissionSnapshot>>
IoTDBStructuredBlobBackend::load_latest_inode_permissions(
    const std::vector<std::string>& paths) const {
  using Result = StructuredBlobResult<std::shared_ptr<const SourcePermissionSnapshot>>;
  auto normalized = normalize_inode_permission_paths(paths);
  auto statements = compile_inode_permission_sql_batches(paths);
  if (!normalized.ok() || !statements.ok())
    return Result::failure(StructuredBlobError::InvalidArgument,
      normalized.ok() ? statements.status().message() : normalized.status().message());
  std::unique_lock<std::recursive_mutex> lock(mutation_mu_, std::defer_lock);
  if (!owner_) lock.lock();
  QueryProfileTimer timer("inode_permission_bulk_read");
  auto snapshot = std::make_shared<SourcePermissionSnapshot>();
  snapshot->storage_namespace = storage_namespace();
  snapshot->metadata_generation = metadata_generation();
  snapshot->observed_at = std::chrono::steady_clock::now();
  for (const auto& path : normalized.value()) snapshot->parents.emplace(path, std::nullopt);
  if (owner_) {
    try {
      const auto view = owner_->view();
      snapshot->metadata_generation = view.head.sequence;
      for (const auto& path : normalized.value()) {
        const auto bytes=owner_->get(view,versioning::Table::Current,path);
        if (!bytes) continue;
        observe_namespace_evidence(namespace_evidence(view.head.identity,versioning::Table::Current,path,*bytes));
        auto inode=namespace_identity(*bytes).inode;
        if (inode.path!=path) throw versioning::Error(versioning::ErrorCode::Corrupt,"permission key differs from inode");
        snapshot->parents[path] = CatalogParentPermission{inode.row_time,inode.mode,inode.uid,inode.gid,inode.is_dir};
      }
      return Result::success(std::move(snapshot));
    } catch (const std::exception& error) { return Result::failure(StructuredBlobError::BackendFailure,error.what()); }
  }
  std::set<std::string> seen;
  for (const auto& sql : statements.value()) {
    query_profile_count("inode_permission_bulk_sql_calls");
    query_profile_count("authorization_parent_metadata_calls");
    auto rows = execute_query(sql);
    if (!rows) return Result::failure(StructuredBlobError::BackendFailure,
      "failed to load bulk inode permissions");
    while (publication_has_next(rows)) {
      query_profile_count("inode_permission_bulk_rows");
      const auto fields = publication_next(rows)->fields;
      if (fields.size() != 6 || !fields[0].longV || !fields[1].stringV)
        return Result::failure(StructuredBlobError::BackendFailure,
          "bulk inode permission identity is malformed");
      if (!observe_latest_parent_permission(*snapshot, seen,
          path_utils::norm(fields[1].stringV.value()), fields[0].longV.value(),
          fields[2].intV ? std::optional<int>(fields[2].intV.value()) : std::nullopt,
          fields[3].intV ? std::optional<int>(fields[3].intV.value()) : std::nullopt,
          fields[4].intV ? std::optional<int>(fields[4].intV.value()) : std::nullopt,
          fields[5].boolV ? std::optional<bool>(fields[5].boolV.value()) : std::nullopt))
        return Result::failure(StructuredBlobError::BackendFailure,
          "bulk inode permission path or version is invalid");
    }
  }
  return Result::success(std::move(snapshot));
}

StructuredBlobStatus IoTDBStructuredBlobBackend::append_payload_eof(
  const StructuredBlobVersionKey& key, std::uint64_t size) {
  if (size > static_cast<std::uint64_t>(std::numeric_limits<long>::max()))
    return backend_failure("payload size exceeds IoTDB INT64 range");
  static const std::vector<std::pair<std::string, TSDataType::TSDataType>> schema = {
    {"path", TSDataType::STRING}, {"data", TSDataType::OBJECT},
    {"size", TSDataType::INT64}, {"sealed", TSDataType::BOOLEAN}};
  static const std::vector<ColumnCategory> categories = {
    ColumnCategory::TAG, ColumnCategory::FIELD,
    ColumnCategory::FIELD, ColumnCategory::FIELD};
  PublicationOperationTimer build_timer(PublicationOperation::TabletBuild);
  Tablet tablet("blob_payload", schema, categories, 1);
  const int row = tablet.rowSize++;
  tablet.addTimestamp(row, key.version);
  tablet.addValue(0, row, path_utils::norm(key.path));
  build_timer.finish(true);
  std::vector<std::uint8_t> empty;
  {
    PublicationOperationTimer encode_timer(PublicationOperation::ObjectEncode);
    tablet.addValue(1, row, true, static_cast<std::size_t>(size), empty);
    encode_timer.finish(true);
  }
  tablet.addValue(2, row, static_cast<long>(size));
  tablet.addValue(3, row, true);
  return insert_tablet(tablet) ?
    StructuredBlobStatus::success() :
    backend_failure("failed to seal payload object");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::replace_manifest(
  const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
  const StructuredBlobManifest& next) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto current = load_manifest_unlocked(key);
  if (!current.ok()) return current.status();
  if (current.value().revision != expected_revision) {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "structured blob manifest revision changed");
  }
  return persist_manifest_unlocked(key, current.value(), next);
}

StructuredBlobStatus IoTDBStructuredBlobBackend::persist_manifest_unlocked(
    const StructuredBlobVersionKey& key, const StructuredBlobManifest& current,
    const StructuredBlobManifest& next) {
  if (!(current.key == key) || !(next.key == key) || current.published ||
      current.phase == StructuredBlobPhase::Published)
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "published structured blob manifests are immutable");
  if (next.published || next.phase == StructuredBlobPhase::Published) {
    const auto current_metadata = serialize_metadata(current.metadata, 65536);
    const auto next_metadata = serialize_metadata(next.metadata, 65536);
    if (!next.published || next.phase != StructuredBlobPhase::Published ||
        current.phase != StructuredBlobPhase::Described ||
        current.format_id != next.format_id || current.payload_size != next.payload_size ||
        current.layout_size != next.layout_size || !current_metadata.ok() ||
        !next_metadata.ok() || current_metadata.value() != next_metadata.value())
      return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
        "invalid structured blob publication transition");
    auto verified = validate_dataset_publication_unlocked(next);
    if (!verified.ok()) return verified;
  }
  auto bounded = validate_manifest_write_bounds(next);
  if (!bounded.ok()) return bounded;
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_path(key.path, key.version));
  if (current.phase == StructuredBlobPhase::PayloadOpen &&
      next.phase == StructuredBlobPhase::PayloadSealed) {
    auto sealed = append_payload_eof(key, next.payload_size);
    if (!sealed.ok()) return sealed;
  }
  auto metadata = serialize_metadata(next.metadata, 65536);
  if (!metadata.ok()) return backend_failure(metadata.error());
  const std::string format = next.format_id.value_or("");
  const std::string sql =
    "INSERT INTO inode(time,path,size,layout_size,blob_phase,published,"
    "blob_revision,format_id,metadata) VALUES(" +
    std::to_string(key.version) + ",'" +
    escape_sql(path_utils::norm(key.path)) + "'," +
    std::to_string(next.payload_size) + ',' +
    std::to_string(next.layout_size) + ',' +
    std::to_string(phase_value(next.phase)) + ',' +
    (next.published && !owner_ ? "true" : "false") + ',' +
    std::to_string(next.revision) + ",'" + escape_sql(format) + "','" +
    escape_sql(metadata.value()) + "')";
  if (!execute_nonquery(sql)) return backend_failure("failed to replace structured blob manifest");
  if (owner_ && next.published) {
    const bool standalone = !pending_tx_;
    try {
      auto inode = load_staging_inode_unlocked(key);
      if (!inode) return backend_failure("publication inode is missing or malformed");
      inode->size = static_cast<long long>(next.payload_size);
      inode->format_id = next.format_id.value_or(""); inode->metadata = metadata.value();
      if (standalone) pending_tx_ = owner_->begin("publish-version");
      put_namespace_record_unlocked(*inode, next);
      if (standalone && !commit_namespace_unlocked()) return backend_failure("namespace publication failed");
    } catch (const std::exception& error) {
      if (standalone) abort_namespace_unlocked();
      return backend_failure(error.what());
    }
  }
  return StructuredBlobStatus::success();
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_payload_and_advance(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobPayloadWriteChunks& chunks) {
  // One lifecycle writer owns this key. Holding the same lock as every manifest
  // mutation makes this fresh read its CAS validation, without a second SELECT.
  PublicationDetailTimer lock_timer("mutation_lock_wait");
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  lock_timer.finish(true);
  auto current = load_manifest_unlocked(key);
  if (!current.ok()) return current.status();
  auto planned = plan_payload_write(current.value(), key, offset, chunks);
  if (!planned.ok()) return planned.status();
  if (!planned.value()) return StructuredBlobStatus::success();
  auto bounded = validate_manifest_write_bounds(*planned.value());
  if (!bounded.ok()) return bounded;
  // Reject every unsupported offset/end before issuing even the first Tablet.
  auto end = offset;
  const auto process_max = static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max());
  for (const auto& chunk : chunks) {
    if (end > process_max || chunk.get().size() > process_max - end)
      return backend_failure("payload offset exceeds process range");
    end += chunk.get().size();
  }
  auto cursor = offset;
  for (const auto& chunk : chunks) {
    if (chunk.get().empty()) continue;
    auto written = write_payload_range_unlocked(key, cursor, chunk.get());
    if (!written.ok()) return written;
    cursor += chunk.get().size();
  }
  // All artifact writes precede the single durable frontier update. Failure
  // leaves unpublished bytes for fresh-check retry/abort. A lost manifest
  // acknowledgement may have advanced the frontier, so never reuse this read.
  return persist_manifest_unlocked(key, current.value(), *planned.value());
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_payload_range(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  const StructuredBlobBytes& bytes) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto writable = require_write_phase_unlocked(key, StructuredBlobPhase::PayloadOpen,
    "payload write");
  if (!writable.ok()) return writable;
  return write_payload_range_unlocked(key, offset, bytes);
}

StructuredBlobStatus IoTDBStructuredBlobBackend::require_write_phase_unlocked(
    const StructuredBlobVersionKey& key, StructuredBlobPhase phase,
    const char* operation) const {
  auto current = load_manifest_unlocked(key);
  if (!current.ok()) return current.status();
  if (current.value().published || current.value().phase != phase)
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      std::string(operation) + " is not valid in the current lifecycle phase");
  return StructuredBlobStatus::success();
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_payload_range_unlocked(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  const StructuredBlobBytes& bytes) {
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()))
    return backend_failure("payload offset exceeds process range");
  static const std::vector<std::pair<std::string, TSDataType::TSDataType>> schema = {
    {"path", TSDataType::STRING}, {"data", TSDataType::OBJECT}};
  static const std::vector<ColumnCategory> categories = {
    ColumnCategory::TAG, ColumnCategory::FIELD};
  PublicationOperationTimer build_timer(PublicationOperation::TabletBuild);
  Tablet tablet("blob_payload", schema, categories, 1);
  const int row = tablet.rowSize++;
  tablet.addTimestamp(row, key.version);
  tablet.addValue(0, row, path_utils::norm(key.path));
  build_timer.finish(true);
  {
    PublicationOperationTimer encode_timer(PublicationOperation::ObjectEncode);
    tablet.addValue(1, row, false, static_cast<std::size_t>(offset), bytes);
    encode_timer.finish(true);
  }
  return insert_tablet(tablet) ?
    StructuredBlobStatus::success() :
    backend_failure("failed to write payload range");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_layout(
  const StructuredBlobVersionKey& key,
  const StructuredBlobBytes& layout) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto writable = require_write_phase_unlocked(key, StructuredBlobPhase::PayloadSealed,
    "layout write");
  if (!writable.ok()) return writable;
  if (layout.size() > static_cast<std::size_t>(std::numeric_limits<long>::max()))
    return backend_failure("layout size exceeds IoTDB INT64 range");
  static const std::vector<std::pair<std::string, TSDataType::TSDataType>> schema = {
    {"path", TSDataType::STRING}, {"data", TSDataType::OBJECT},
    {"size", TSDataType::INT64}, {"sealed", TSDataType::BOOLEAN}};
  static const std::vector<ColumnCategory> categories = {
    ColumnCategory::TAG, ColumnCategory::FIELD,
    ColumnCategory::FIELD, ColumnCategory::FIELD};
  PublicationOperationTimer build_timer(PublicationOperation::TabletBuild);
  Tablet tablet("blob_layout", schema, categories, 1);
  const int row = tablet.rowSize++;
  tablet.addTimestamp(row, key.version);
  tablet.addValue(0, row, path_utils::norm(key.path));
  build_timer.finish(true);
  {
    PublicationOperationTimer encode_timer(PublicationOperation::ObjectEncode);
    tablet.addValue(1, row, true, 0, layout);
    encode_timer.finish(true);
  }
  tablet.addValue(2, row, static_cast<long>(layout.size()));
  tablet.addValue(3, row, true);
  return insert_tablet(tablet) ?
    StructuredBlobStatus::success() :
    backend_failure("failed to write layout artifact");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_metadata(
  const StructuredBlobVersionKey& key, const MetadataMap& metadata) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto writable = require_write_phase_unlocked(key, StructuredBlobPhase::LayoutStored,
    "metadata write");
  if (!writable.ok()) return writable;
  auto serialized = serialize_metadata(metadata, 65536);
  if (!serialized.ok()) return backend_failure(serialized.error());
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_path(key.path, key.version));
  auto indexed = replace_metadata_set_members(key, metadata);
  if (!indexed.ok()) return indexed;
  const std::string sql = "INSERT INTO inode(time,path,metadata) VALUES(" +
    std::to_string(key.version) + ",'" +
    escape_sql(path_utils::norm(key.path)) + "','" +
    escape_sql(serialized.value()) + "')";
  return execute_nonquery(sql) ? StructuredBlobStatus::success() :
    backend_failure("failed to write typed metadata document");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::replace_metadata_set_members(
  const StructuredBlobVersionKey& key, const MetadataMap& metadata) {
  const std::string predicate = key_predicate(key);
  if (!execute_nonquery("DELETE FROM blob_metadata_set WHERE " + predicate))
    return backend_failure("failed to clear typed metadata set index");
  const auto members = metadata_set_members(metadata);
  if (members.empty()) return StructuredBlobStatus::success();

  static const std::vector<std::pair<std::string, TSDataType::TSDataType>> schema = {
    {"path", TSDataType::STRING}, {"field_id", TSDataType::STRING},
    {"member_value", TSDataType::STRING}, {"member_type", TSDataType::STRING},
    {"present", TSDataType::BOOLEAN}};
  static const std::vector<ColumnCategory> categories = {
    ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::TAG,
    ColumnCategory::TAG, ColumnCategory::FIELD};
  Tablet tablet("blob_metadata_set", schema, categories, members.size());
  for (const auto& member : members) {
    const int row = tablet.rowSize++;
    tablet.addTimestamp(row, key.version);
    tablet.addValue(0, row, path_utils::norm(key.path));
    tablet.addValue(1, row, member.field_id);
    tablet.addValue(2, row, member.member_value);
    tablet.addValue(3, row, member.member_type);
    tablet.addValue(4, row, true);
  }
  return insert_tablet(tablet) ?
    StructuredBlobStatus::success() :
    backend_failure("failed to write typed metadata set index");
}

StructuredBlobResult<StructuredBlobBytes>
IoTDBStructuredBlobBackend::read_payload_range(
  const StructuredBlobVersionKey& key, std::uint64_t offset,
  std::uint64_t length) const {
  if (owner_) {
    try {
      versioning::detail::PayloadBlockFlights::ReuseScope payload_reuse;
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
        scope = std::make_unique<ScopedQueryMetadataBudget>();
      const auto encoded = namespace_record_unlocked(key, false);
      if (!encoded) return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "version is not visible in the current namespace root");
      const auto record = versioning::decode_namespace_record(*encoded);
      const auto verified = verified_content_unlocked(record);
      if (offset > record.manifest.payload_size || length > record.manifest.payload_size - offset ||
          length > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
          length > kMaxSharedPayloadReadBytes)
        return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::RangeError,
          "payload read range is outside the published payload");
      StructuredBlobBytes bytes; bytes.reserve(static_cast<std::size_t>(length));
      for (std::uint64_t done = 0; done < length;) {
        const auto count = std::min(length - done, kMaxPayloadBytesPerBatch);
        auto batch = payload_artifacts_->read_ranges(verified->descriptor.payload_root,
          record.manifest.payload_size, {{offset + done, count}}, payload_source_identity(key));
        if (batch.size() != 1 || batch[0].size() != count)
          throw std::runtime_error("payload content returned an unexpected byte count");
        // Preserve the eager allocation and validation order; a single batch
        // already owns the final buffer, so it need not be copied again.
        if (count == length)
          bytes = std::move(batch[0]);
        else
          bytes.insert(bytes.end(), batch[0].begin(), batch[0].end());
        done += count;
      }
      return StructuredBlobResult<StructuredBlobBytes>::success(std::move(bytes));
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobBytes>(error.what()); }
  }

  IoTDBReadObservation observation(true, 1, length);
  const std::string sql = "SELECT READ_OBJECT(data," +
    std::to_string(offset) + ',' + std::to_string(length) +
    ") FROM blob_payload WHERE " + key_predicate(key) + " LIMIT 1";
  auto rows = execute_query(sql, false);
  if (!rows) return backend_failure_result<StructuredBlobBytes>(
    "failed to read payload range");
  if (!publication_has_next(rows)) return StructuredBlobResult<StructuredBlobBytes>::failure(
    StructuredBlobError::NotFound, "payload object was not found");
  auto row = publication_next(rows);
  if (row->fields.empty() || !row->fields[0].stringV)
    return backend_failure_result<StructuredBlobBytes>(
      "payload range result is malformed");
  const auto& binary = row->fields[0].stringV.value();
  StructuredBlobBytes bytes(binary.begin(), binary.end());
  observation.success(bytes.size());
  return StructuredBlobResult<StructuredBlobBytes>::success(std::move(bytes));
}

StructuredBlobResult<StructuredBlobPayloadBatch>
IoTDBStructuredBlobBackend::read_payload_ranges(
    const StructuredBlobVersionKey& key,
    const StructuredBlobPayloadRanges& ranges) const {
  if (owner_) {
    try {
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
        scope = std::make_unique<ScopedQueryMetadataBudget>();
      const auto encoded = namespace_record_unlocked(key, false);
      if (!encoded) return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
        StructuredBlobError::NotFound, "version is not visible in the current namespace root");
      const auto record = versioning::decode_namespace_record(*encoded);
      const auto verified = verified_content_unlocked(record);
      const auto valid = validate_shared_payload_ranges(record.manifest.payload_size, ranges);
      if (!valid.ok()) return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(valid.code(), valid.message());
      return StructuredBlobResult<StructuredBlobPayloadBatch>::success(
        payload_artifacts_->read_ranges(verified->descriptor.payload_root, record.manifest.payload_size,
          ranges, payload_source_identity(key)));
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobPayloadBatch>(error.what()); }
  }

  if (ranges.empty() || ranges.size() > kMaxPayloadRangesPerBatch)
    return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
      StructuredBlobError::RangeError,
      "payload batch range count is outside the supported bounds");
  std::uint64_t total = 0;
  std::string sql = "SELECT ";
  for (std::size_t i = 0; i < ranges.size(); ++i) {
    const auto& range = ranges[i];
    if (range.length == 0 || range.length > kMaxPayloadBytesPerBatch - total ||
        range.offset > std::numeric_limits<std::uint64_t>::max() - range.length)
      return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
        StructuredBlobError::RangeError,
        "payload batch contains an invalid range or exceeds the byte budget");
    total += range.length;
    if (i != 0) sql += ',';
    sql += "READ_OBJECT(data," + std::to_string(range.offset) + ',' +
      std::to_string(range.length) + ") AS range_" + std::to_string(i);
  }
  sql += " FROM blob_payload WHERE " + key_predicate(key) + " LIMIT 1";
  IoTDBReadObservation observation(true, ranges.size(), total);
  auto rows = execute_query(sql, false);
  if (!rows)
    return backend_failure_result<StructuredBlobPayloadBatch>(
      "failed to read payload range batch");
  if (!publication_has_next(rows))
    return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
      StructuredBlobError::NotFound, "payload object was not found");
  auto row = publication_next(rows);
  if (row->fields.size() != ranges.size())
    return backend_failure_result<StructuredBlobPayloadBatch>(
      "payload range batch returned an unexpected column count");
  StructuredBlobPayloadBatch batch;
  batch.reserve(ranges.size());
  for (std::size_t i = 0; i < ranges.size(); ++i) {
    if (!row->fields[i].stringV ||
        row->fields[i].stringV.value().size() != ranges[i].length)
      return backend_failure_result<StructuredBlobPayloadBatch>(
        "payload range batch returned malformed or short data");
    const auto& binary = row->fields[i].stringV.value();
    batch.emplace_back(binary.begin(), binary.end());
  }
  std::uint64_t returned_bytes = 0;
  for (const auto& bytes : batch) returned_bytes += bytes.size();
  observation.success(returned_bytes);
  return StructuredBlobResult<StructuredBlobPayloadBatch>::success(
    std::move(batch));
}

StructuredBlobResult<StructuredBlobBytes>
IoTDBStructuredBlobBackend::read_layout_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const {
  if (owner_) {
    try {
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
        scope = std::make_unique<ScopedQueryMetadataBudget>();
      const auto encoded = namespace_record_unlocked(key, false);
      if (!encoded) return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "version is not visible in the current namespace root");
      auto verified = verified_content_unlocked(versioning::decode_namespace_record(*encoded));
      return StructuredBlobResult<StructuredBlobBytes>::success(content_artifacts_->read_layout(*verified, offset, length));
    } catch (const std::exception& error) { return backend_failure_result<StructuredBlobBytes>(error.what()); }
  }

  if (length == 0)
    return StructuredBlobResult<StructuredBlobBytes>::success({});
  IoTDBReadObservation observation(false, 1, length);
  const std::string sql = "SELECT READ_OBJECT(data," +
    std::to_string(offset) + ',' + std::to_string(length) +
    ") FROM blob_layout WHERE " + key_predicate(key) + " LIMIT 1";
  auto rows = execute_query(sql, false);
  if (!rows) return backend_failure_result<StructuredBlobBytes>(
    "failed to read layout artifact");
  if (!publication_has_next(rows)) return StructuredBlobResult<StructuredBlobBytes>::failure(
    StructuredBlobError::NotFound, "layout artifact was not found");
  auto row = publication_next(rows);
  if (row->fields.empty() || !row->fields[0].stringV)
    return backend_failure_result<StructuredBlobBytes>(
      "layout artifact result is malformed");
  const auto& binary = row->fields[0].stringV.value();
  StructuredBlobBytes bytes(binary.begin(), binary.end());
  observation.success(bytes.size());
  return StructuredBlobResult<StructuredBlobBytes>::success(std::move(bytes));
}

StructuredBlobStatus IoTDBStructuredBlobBackend::erase(
  const StructuredBlobVersionKey& key, std::uint64_t expected_revision) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto current = load_manifest_unlocked(key);
  if (!current.ok()) return current.status();
  if (current.value().revision != expected_revision) {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "structured blob manifest revision changed");
  }
  return remove_path(key.path, key.version) ?
    StructuredBlobStatus::success() :
    backend_failure("failed to remove structured blob version");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::write_dataset_catalog(
    const StructuredBlobVersionKey &key,
    const DatasetCatalogArtifact &artifact) {
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  auto writable = require_write_phase_unlocked(key, StructuredBlobPhase::LayoutStored,
    "dataset catalog write");
  if (!writable.ok()) return writable;
  auto bytes = serialize_dataset_catalog(artifact);
  if (!bytes.ok())
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,
                                         bytes.error());
  const auto predicate = key_predicate(key);
  // Invalidate before any DELETE, including ambiguous or partially failed
  // replacement of a catalog whose full marker identity remains unchanged.
  query_profile_count("dataset_catalog_parsed_cache.invalidations",
    dataset_catalog_cache_.invalidate_path(key.path, key.version));
  // Marker is removed first and written LAST. Publication is a separate gate.
  for (const char *table :
       {"blob_dataset_catalog", "blob_dataset_entry", "blob_dataset_attribute"})
    if (!execute_nonquery(std::string("DELETE FROM ") + table + " WHERE " +
                          predicate))
      return backend_failure("cannot replace dataset catalog artifacts");
  if (!artifact.records.empty()) {
    Tablet rows("blob_dataset_entry",
                {{"path", TSDataType::STRING},
                 {"object_id", TSDataType::STRING},
                 {"dataset_path", TSDataType::STRING},
                 {"attributes_complete", TSDataType::BOOLEAN}},
                {ColumnCategory::TAG, ColumnCategory::TAG,
                 ColumnCategory::FIELD, ColumnCategory::FIELD},
                artifact.records.size());
    for (const auto &d : artifact.records) {
      const int i = rows.rowSize++;
      rows.addTimestamp(i, key.version);
      rows.addValue(0, i, key.path);
      rows.addValue(1, i, d.object_id);
      rows.addValue(2, i, d.canonical_path);
      rows.addValue(3, i, d.attributes_complete);
    }
    if (!insert_tablet(rows))
      return backend_failure("cannot write dataset catalog directory");
  }
  std::size_t count = 0;
  for (const auto &d : artifact.records)
    count += d.attributes.size();
  if (count) {
    Tablet attrs("blob_dataset_attribute",
                 {{"path", TSDataType::STRING},
                  {"object_id", TSDataType::STRING},
                  {"attribute_name", TSDataType::STRING},
                  {"state", TSDataType::INT32},
                  {"value_type", TSDataType::STRING},
                  {"svalue", TSDataType::STRING},
                  {"ivalue", TSDataType::INT64},
                  {"fvalue", TSDataType::DOUBLE},
                  {"bvalue", TSDataType::BOOLEAN}},
                 {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::TAG,
                  ColumnCategory::FIELD, ColumnCategory::FIELD,
                  ColumnCategory::FIELD, ColumnCategory::FIELD,
                  ColumnCategory::FIELD, ColumnCategory::FIELD},
                 count);
    for (const auto &d : artifact.records)
      for (const auto &a : d.attributes) {
        const int i = attrs.rowSize++;
        attrs.addTimestamp(i, key.version);
        attrs.addValue(0, i, key.path);
        attrs.addValue(1, i, d.object_id);
        attrs.addValue(2, i, a.name);
        attrs.addValue(3, i, static_cast<int>(a.state));
        // Tablet::addValue writes the value buffer but does not maintain null
        // bitmaps. Every unused typed cell must be NULL, including value_type
        // for unsupported attributes; zero/default buffers are not values.
        for (std::size_t column = 4; column != 9; ++column)
          attrs.bitMaps[column].mark(i);
        if (a.value) {
          attrs.addValue(4, i, dataset_attribute_type(*a.value));
          attrs.bitMaps[4].unmark(i);
          std::visit(
              [&](const auto &v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::string>) {
                  attrs.addValue(5, i, v);
                  attrs.bitMaps[5].unmark(i);
                } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                  attrs.addValue(5, i, dataset_attribute_text(*a.value));
                  attrs.bitMaps[5].unmark(i);
                } else if constexpr (std::is_same_v<T, std::int64_t>) {
                  attrs.addValue(6, i, static_cast<long>(v));
                  attrs.bitMaps[6].unmark(i);
                } else if constexpr (std::is_same_v<T, double>) {
                  attrs.addValue(7, i, v);
                  attrs.bitMaps[7].unmark(i);
                } else {
                  attrs.addValue(8, i, v);
                  attrs.bitMaps[8].unmark(i);
                }
              },
              *a.value);
        }
      }
    if (!insert_tablet(attrs))
      return backend_failure("cannot write typed dataset attributes");
  }
  PublicationOperationTimer build_timer(PublicationOperation::TabletBuild);
  Tablet marker(
      "blob_dataset_catalog",
      {{"path", TSDataType::STRING},
       {"data", TSDataType::OBJECT},
       {"size", TSDataType::INT64},
       {"protocol", TSDataType::STRING},
       {"layout_digest", TSDataType::STRING},
       {"layout_protocol", TSDataType::STRING},
       {"directory_complete", TSDataType::BOOLEAN},
       {"record_count", TSDataType::INT64},
       {"attribute_count", TSDataType::INT64}},
      {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD,
       ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
       ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD},
      1);
  marker.rowSize = 1;
  marker.addTimestamp(0, key.version);
  marker.addValue(0, 0, key.path);
  build_timer.finish(true);
  std::vector<std::uint8_t> copy = std::move(bytes.value());
  {
    PublicationOperationTimer encode_timer(PublicationOperation::ObjectEncode);
    marker.addValue(1, 0, true, 0, copy);
    encode_timer.finish(true);
  }
  marker.addValue(2, 0, static_cast<long>(copy.size()));
  marker.addValue(3, 0, artifact.protocol);
  marker.addValue(4, 0, artifact.layout_digest);
  marker.addValue(5, 0, artifact.layout_protocol);
  marker.addValue(6, 0, artifact.directory_complete);
  marker.addValue(7, 0, static_cast<long>(artifact.records.size()));
  marker.addValue(8, 0, static_cast<long>(count));
  return insert_tablet(marker)
             ? StructuredBlobStatus::success()
             : backend_failure("cannot seal dataset catalog");
}

StructuredBlobStatus IoTDBStructuredBlobBackend::validate_dataset_secondary_rows_unlocked(
    const StructuredBlobVersionKey& key, const DatasetCatalogArtifact& artifact) const {
  QueryProfileTimer timer("dataset_index_secondary_validation");
  const auto malformed = [] {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "dataset secondary index returned malformed or excess rows");
  };
  std::vector<DatasetIndexEntryRow> entries;
  auto rows = execute_query("SELECT object_id,dataset_path,attributes_complete FROM blob_dataset_entry WHERE " +
    key_predicate(key) + " LIMIT " + std::to_string(artifact.records.size() + 1));
  if (!rows) return backend_failure("cannot validate dataset directory rows");
  query_profile_count("dataset_index_secondary_validation.calls");
  while (publication_has_next(rows)) {
    const auto fields = publication_next(rows)->fields;
    if (entries.size() >= artifact.records.size() || fields.size() != 3 ||
        !fields[0].stringV || !fields[1].stringV || !fields[2].boolV) return malformed();
    entries.push_back({fields[0].stringV.value(), fields[1].stringV.value(), fields[2].boolV.value()});
  }
  rows.reset();
  std::size_t attribute_count = 0;
  for (const auto& record : artifact.records) attribute_count += record.attributes.size();
  std::vector<DatasetIndexAttributeRow> attributes;
  rows = execute_query("SELECT object_id,attribute_name,state,value_type,svalue,ivalue,fvalue,bvalue "
    "FROM blob_dataset_attribute WHERE " + key_predicate(key) + " LIMIT " +
    std::to_string(attribute_count + 1));
  if (!rows) return backend_failure("cannot validate dataset attribute rows");
  query_profile_count("dataset_index_secondary_validation.calls");
  while (publication_has_next(rows)) {
    const auto fields = publication_next(rows)->fields;
    if (attributes.size() >= attribute_count || fields.size() != 8 ||
        !fields[0].stringV || !fields[1].stringV || !fields[2].intV ||
        fields[2].intV.value() < static_cast<int>(DatasetAttributeState::Complete) ||
        fields[2].intV.value() > static_cast<int>(DatasetAttributeState::Invalid)) return malformed();
    DatasetIndexAttributeRow row;
    row.object_id = fields[0].stringV.value(); row.name = fields[1].stringV.value();
    row.state = static_cast<DatasetAttributeState>(fields[2].intV.value());
    const auto value_fields = unsigned(bool(fields[4].stringV)) + unsigned(bool(fields[5].longV)) +
      unsigned(bool(fields[6].doubleV)) + unsigned(bool(fields[7].boolV));
    if (row.state == DatasetAttributeState::Complete) {
      if (!fields[3].stringV || value_fields != 1) return malformed();
      const auto& type = fields[3].stringV.value();
      if (type == "string" && fields[4].stringV) row.value = fields[4].stringV.value();
      else if (type == "int64" && fields[5].longV)
        row.value = static_cast<std::int64_t>(fields[5].longV.value());
      else if (type == "float64" && fields[6].doubleV) row.value = fields[6].doubleV.value();
      else if (type == "bool" && fields[7].boolV) row.value = fields[7].boolV.value();
      else if (type == "uint64" && fields[4].stringV) {
        const auto& text = fields[4].stringV.value();
        if (text.size() != 20 || text.find_first_not_of("0123456789") != std::string::npos)
          return malformed();
        try {
          row.value = static_cast<std::uint64_t>(std::stoull(text));
          if (dataset_attribute_text(*row.value) != text) return malformed();
        } catch (...) { return malformed(); }
      } else return malformed();
    } else if (fields[3].stringV || value_fields) return malformed();
    attributes.push_back(std::move(row));
  }
  query_profile_count("dataset_index_secondary_validation.entries", entries.size());
  query_profile_count("dataset_index_secondary_validation.attributes", attributes.size());
  return validate_dataset_index_rows(artifact, entries, attributes);
}

StructuredBlobStatus IoTDBStructuredBlobBackend::validate_dataset_publication_unlocked(
    const StructuredBlobManifest& manifest) const {
  const bool catalog = manifest.metadata.count("__iotdbfs.dataset_catalog.protocol") != 0;
  if (!catalog) {
    for (const auto& item : manifest.metadata)
      if (item.first.compare(0, 24, "__iotdbfs.dataset_index.") == 0)
        return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
          "dataset index seal requires its dataset catalog");
    return StructuredBlobStatus::success();
  }
  auto marker_rows = execute_query("SELECT size,protocol,layout_digest,layout_protocol,"
    "directory_complete,record_count,attribute_count FROM blob_dataset_catalog WHERE " +
    key_predicate(manifest.key) + " LIMIT 2");
  if (!marker_rows || !publication_has_next(marker_rows))
    return backend_failure("cannot validate dataset publication marker");
  const auto marker_fields = publication_next(marker_rows)->fields;
  if (marker_fields.size() != 7 || !marker_fields[0].longV || !marker_fields[1].stringV ||
      !marker_fields[2].stringV || !marker_fields[3].stringV || !marker_fields[4].boolV ||
      !marker_fields[5].longV || !marker_fields[6].longV ||
      marker_fields[0].longV.value() <= 0 ||
      static_cast<std::uint64_t>(marker_fields[0].longV.value()) > kMaxDatasetCatalogBytes ||
      publication_has_next(marker_rows))
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "dataset publication marker is malformed");
  DatasetCatalogMarker marker{manifest.key, marker_fields[0].longV.value(),
    marker_fields[1].stringV.value(), marker_fields[2].stringV.value(),
    marker_fields[3].stringV.value(), marker_fields[4].boolV.value(),
    marker_fields[5].longV.value(), marker_fields[6].longV.value()};
  marker_rows.reset();
  auto stored = read_dataset_catalog_object_unlocked(manifest.key,
    static_cast<std::uint64_t>(marker.size));
  if (!stored.ok()) return stored.status();
  const auto& artifact = stored.value();
  auto consistent = validate_dataset_catalog_marker_artifact(marker, manifest, artifact,
    static_cast<std::uint64_t>(marker.size));
  if (!consistent.ok()) return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
    consistent.message());
  const auto protocol = manifest.metadata.find("__iotdbfs.dataset_catalog.protocol");
  const auto digest = manifest.metadata.find("__iotdbfs.dataset_catalog.layout_digest");
  if (protocol->second.type() != MetadataValueType::String ||
      digest == manifest.metadata.end() || digest->second.type() != MetadataValueType::String ||
      std::get<std::string>(protocol->second.value()) != artifact.protocol ||
      std::get<std::string>(digest->second.value()) != artifact.layout_digest)
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "dataset catalog publication identity differs from its manifest");
  auto seal = validate_dataset_substructure_index(manifest, artifact);
  if (!seal.ok()) return seal;
  auto secondary = validate_dataset_secondary_rows_unlocked(manifest.key, artifact);
  if (secondary.ok()) query_profile_count("dataset_substructure_index.publication_validations");
  return secondary;
}

StructuredBlobResult<DatasetCatalogArtifact>
IoTDBStructuredBlobBackend::read_dataset_catalog(
    const StructuredBlobVersionKey &key) const {
  if (owner_) {
    try {
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (!current_query_metadata_budget() && active_query_metadata_trace == nullptr)
        scope = std::make_unique<ScopedQueryMetadataBudget>();
      // Published catalog objects are immutable; only the unpublished lifecycle
      // below needs the staging transaction lock.
      auto encoded = namespace_record_unlocked(key, false);
      if (encoded) {
        auto verified = verified_content_unlocked(versioning::decode_namespace_record(*encoded));
        auto catalog = content_artifacts_->catalog(*verified);
        if (!catalog) return StructuredBlobResult<DatasetCatalogArtifact>::failure(
          StructuredBlobError::NotFound, "published content has no dataset catalog");
        return StructuredBlobResult<DatasetCatalogArtifact>::success(*catalog);
      }
    } catch (const std::exception& error) { return backend_failure_result<DatasetCatalogArtifact>(error.what()); }
  }
  std::lock_guard<std::recursive_mutex> lock(mutation_mu_);
  if (owner_) {
    auto manifest = load_manifest_unlocked(key);
    if (!manifest.ok()) return StructuredBlobResult<DatasetCatalogArtifact>::failure(
      manifest.status().code(), manifest.status().message());
    if (manifest.value().phase != StructuredBlobPhase::Described &&
        manifest.value().phase != StructuredBlobPhase::Published)
      return StructuredBlobResult<DatasetCatalogArtifact>::failure(
        StructuredBlobError::InvalidState, "catalog is not described or published");
    if (auto encoded = namespace_record_unlocked(key, true)) {
      try {
        auto verified = verified_content_unlocked(versioning::decode_namespace_record(*encoded));
        auto catalog = content_artifacts_->catalog(*verified);
        if (!catalog) return StructuredBlobResult<DatasetCatalogArtifact>::failure(
          StructuredBlobError::NotFound, "published content has no dataset catalog");
        return StructuredBlobResult<DatasetCatalogArtifact>::success(*catalog);
      } catch (const std::exception& error) { return backend_failure_result<DatasetCatalogArtifact>(error.what()); }
    }
  }
  return read_staging_catalog_unlocked(key);
}

StructuredBlobResult<DatasetCatalogArtifact>
IoTDBStructuredBlobBackend::read_staging_catalog_unlocked(const StructuredBlobVersionKey& key) const {
  auto marker = execute_query("SELECT size FROM blob_dataset_catalog WHERE " +
                              key_predicate(key) + " LIMIT 1");
  if (!marker)
    return backend_failure_result<DatasetCatalogArtifact>(
        "cannot read dataset catalog marker");
  if (!publication_has_next(marker))
    return StructuredBlobResult<DatasetCatalogArtifact>::failure(
        StructuredBlobError::NotFound,
        "dataset catalog missing; reingestion required");
  auto f = publication_next(marker)->fields;
  if (f.size() != 1 || !f[0].longV || f[0].longV.value() <= 0 ||
      static_cast<std::uint64_t>(f[0].longV.value()) > kMaxDatasetCatalogBytes)
    return backend_failure_result<DatasetCatalogArtifact>(
        "dataset catalog size is invalid");
  marker.reset(); // Release the size-query session before reading the object.
  return read_dataset_catalog_object_unlocked(key, static_cast<std::uint64_t>(f[0].longV.value()));
}

StructuredBlobResult<DatasetCatalogArtifact>
IoTDBStructuredBlobBackend::read_dataset_catalog_object_unlocked(
    const StructuredBlobVersionKey& key, std::uint64_t size) const {
  auto rows = execute_query(
      "SELECT READ_OBJECT(data,0," + std::to_string(size) +
      ") FROM blob_dataset_catalog WHERE " + key_predicate(key) + " LIMIT 1");
  if (!rows || !publication_has_next(rows))
    return backend_failure_result<DatasetCatalogArtifact>(
        "cannot read dataset catalog bytes");
  auto fields = publication_next(rows)->fields;
  if (fields.size() != 1 || !fields[0].stringV ||
      fields[0].stringV.value().size() !=
          size)
    return backend_failure_result<DatasetCatalogArtifact>(
        "dataset catalog read was short");
  const auto &bytes = fields[0].stringV.value();
  auto parsed = deserialize_dataset_catalog({bytes.begin(), bytes.end()});
  if (!parsed.ok())
    return backend_failure_result<DatasetCatalogArtifact>(parsed.error());
  return StructuredBlobResult<DatasetCatalogArtifact>::success(
      std::move(parsed.value()));
}

QueryResult<IoTDBStructuredBlobBackend::DatasetCatalogVerification>
IoTDBStructuredBlobBackend::read_dataset_verification_unlocked(
    const std::vector<StructuredBlobManifest>& manifests,
    const std::vector<StructuredBlobVersionKey>& marker_keys,
    std::uint64_t& sql_queries) const {
  using Result = QueryResult<DatasetCatalogVerification>;
  const auto stale = [] {
    return Result::failure(QueryStatus::failure(QueryError::StalePlan, QueryPhase::Catalog,
      "dataset discovery source changed after authorization"));
  };
  QueryProfileTimer timer("dataset_catalog_verification_read");
  std::vector<StructuredBlobVersionKey> keys;
  std::map<StructuredBlobVersionKey, const StructuredBlobManifest*> selected;
  for (const auto& manifest : manifests) {
    keys.push_back(manifest.key); selected.emplace(manifest.key, &manifest);
  }
  auto batches = compile_dataset_catalog_verification_sql_batches(keys, marker_keys);
  if (!batches.ok()) return Result::failure(batches.status());
  DatasetCatalogVerification result;
  for (const auto& batch : batches.value()) {
    ++sql_queries;
    query_profile_count("dataset_catalog_verification_read.calls");
    query_profile_count("dataset_catalog_verification_read.manifest_keys", batch.keys.size());
    query_profile_count("dataset_catalog_verification_read.marker_keys", batch.marker_keys.size());
    auto rows = execute_query(batch.sql);
    // A failed combined read cannot establish freshness, just as a failed
    // standalone manifest revalidation could not.
    if (!rows) return stale();
    std::vector<StructuredBlobManifest> expected, observed;
    expected.reserve(batch.keys.size()); observed.reserve(batch.keys.size());
    for (const auto& key : batch.keys) {
      const auto& source = *selected.at(key);
      StructuredBlobManifest identity;
      identity.key = source.key; identity.format_id = source.format_id;
      identity.payload_size = source.payload_size; identity.layout_size = source.layout_size;
      identity.phase = source.phase; identity.published = source.published;
      identity.revision = source.revision; expected.push_back(std::move(identity));
    }
    std::size_t returned = 0, marker_rows = 0;
    while (publication_has_next(rows)) {
      auto fields = std::move(publication_next(rows)->fields);
      if (++returned > batch.keys.size() + batch.marker_keys.size() + 2 ||
          fields.size() != 15 || !fields[0].longV) return stale();
      if (fields[0].longV.value() == 0) {
        query_profile_count("dataset_catalog_verification_read.manifest_rows");
        if (observed.size() >= batch.keys.size() || !fields[1].longV ||
            !fields[2].stringV || !fields[4].longV || !fields[5].longV ||
            !fields[6].intV || !fields[7].boolV || !fields[8].longV ||
            fields[1].longV.value() < 0 || fields[4].longV.value() < 0 ||
            fields[5].longV.value() < 0 || fields[8].longV.value() < 0) return stale();
        const auto phase = parse_phase(fields[6].intV.value());
        if (!phase) return stale();
        StructuredBlobManifest manifest;
        manifest.key = {fields[2].stringV.value(), fields[1].longV.value()};
        if (fields[3].stringV && !fields[3].stringV.value().empty()) manifest.format_id = fields[3].stringV.value();
        manifest.payload_size = static_cast<std::uint64_t>(fields[4].longV.value());
        manifest.layout_size = static_cast<std::uint64_t>(fields[5].longV.value());
        manifest.phase = *phase; manifest.published = fields[7].boolV.value();
        manifest.revision = static_cast<std::uint64_t>(fields[8].longV.value());
        observed.push_back(std::move(manifest));
      } else if (fields[0].longV.value() == 1) {
        query_profile_count("dataset_catalog_verification_read.marker_rows");
        if (++marker_rows > batch.marker_keys.size() || !fields[1].longV ||
            !fields[2].stringV || !fields[4].longV || !fields[9].stringV ||
            !fields[10].stringV || !fields[11].stringV || !fields[12].boolV ||
            !fields[13].longV || !fields[14].longV) {
          result.marker_status = QueryStatus::failure(QueryError::CorruptLayout,
            QueryPhase::Catalog, "dataset catalog marker SQL returned malformed or excess rows");
          continue;
        }
        result.markers.push_back({{fields[2].stringV.value(), fields[1].longV.value()},
          fields[4].longV.value(), fields[9].stringV.value(), fields[10].stringV.value(),
          fields[11].stringV.value(), fields[12].boolV.value(), fields[13].longV.value(), fields[14].longV.value()});
      } else return stale();
    }
    auto valid = validate_manifest_revalidation_rows(expected, observed);
    if (!valid.ok()) return Result::failure(valid);
  }
  return Result::success(std::move(result));
}

namespace {
// Kind tags separate retained payload types within the same owner pool.
// Candidate identities never include ROI, principal or a predicate answer.
std::string owner_domain_key(const versioning::Head& head,const SourceSelector& selector,
    std::size_t max_sources,const std::string& kind) {
  auto encoded=versioning::encode_head(head);
  std::string key(encoded.begin(),encoded.end());
  auto text=[&](const std::string& value){key+=std::to_string(value.size())+":"+value;};
  key+="/domain:";text(kind);
  key+=selector.paths.identity_key();
  key+="/version:"+std::to_string(static_cast<int>(selector.versions.kind));
  for(auto value:{selector.versions.exact,selector.versions.lower_inclusive,selector.versions.upper_exclusive})
    key+=value?":"+std::to_string(*value):":none";
  key+="/formats:"+std::to_string(static_cast<int>(selector.formats.adapters));
  key+=selector.formats.include_byte_passthrough?":bytes":":no-bytes";
  auto formats=selector.formats.adapter_ids;
  std::sort(formats.begin(),formats.end());formats.erase(std::unique(formats.begin(),formats.end()),formats.end());
  key+=":"+std::to_string(formats.size());for(const auto& format:formats)text(format);
  key+="/max-sources:"+std::to_string(max_sources);
  return key;
}

// Record bounded per-stage counters once per discovery, without adding work to
// the per-identity loop used as the control path.
struct HotFeeSummaryProfile {
  std::shared_ptr<QueryMetadataBudget> budget;
  QueryMetadataStageSummaryStats before;
  explicit HotFeeSummaryProfile(std::shared_ptr<QueryMetadataBudget> value)
    : budget(std::move(value)),before(budget?budget->stage_summary_stats():QueryMetadataStageSummaryStats{}) {}
  ~HotFeeSummaryProfile() {
    if(!budget)return;
    const auto after=budget->stage_summary_stats();
    query_profile_count("hot_query.fee_summary_stages",after.successful_stages-before.successful_stages);
    query_profile_count("hot_query.fee_summary_entries_skipped",after.borrowed_fee_entries_skipped-before.borrowed_fee_entries_skipped);
    query_profile_count("hot_query.fee_replay_entries",after.full_fee_entries_visited-before.full_fee_entries_visited);
    query_profile_count("hot_query.fee_summary_fallbacks",after.fallbacks-before.fallbacks);
    query_profile_count("hot_query.fee_summary_collector_exits",after.collector_exits-before.collector_exits);
    query_profile_count("hot_query.fee_summary_overlay_entries",after.overlay_entries_inspected-before.overlay_entries_inspected);
  }
};

// Backend bindings are for one immutable selector template. Paths retain their
// shared compiled storage; matching never traverses or compares the P paths.
// Per-query predicate, result limits and ledger are deliberately absent.
struct HotSelectorTemplate {
  PathScopeSet paths;
  VersionSelection versions;
  FormatSelector formats;
  std::optional<std::string> dataset_path;
  std::string format_id;
  std::size_t max_sources;
  explicit HotSelectorTemplate(const DatasetCatalogRequest& request)
    : paths(request.sources.paths),versions(request.sources.versions),formats(request.sources.formats),
      dataset_path(request.dataset_path),format_id(request.format_id),max_sources(request.max_sources) {}
  HotSelectorTemplate(const SourceSelector& selector,std::size_t maximum)
    : paths(selector.paths),versions(selector.versions),formats(selector.formats),max_sources(maximum) {}
  bool matches(const SourceSelector& selector,std::size_t maximum) const {
    const auto& v=selector.versions;
    const auto& f=selector.formats;
    return &paths.identity_key()==&selector.paths.identity_key()&&versions.kind==v.kind&&
      versions.exact==v.exact&&versions.lower_inclusive==v.lower_inclusive&&versions.upper_exclusive==v.upper_exclusive&&
      formats.adapters==f.adapters&&formats.include_byte_passthrough==f.include_byte_passthrough&&
      formats.adapter_ids==f.adapter_ids&&max_sources==maximum;
  }
  bool matches(const DatasetCatalogRequest& request) const {
    return matches(request.sources,request.max_sources)&&dataset_path==request.dataset_path&&format_id==request.format_id;
  }
  static std::uint64_t additional_bytes(const SourceSelector& selector) {
    std::uint64_t bytes=sizeof(HotSelectorTemplate)+512;
    bytes+=2*selector.formats.adapter_ids.size()*sizeof(std::string);
    for(const auto& id:selector.formats.adapter_ids)bytes+=2*id.size();
    return bytes;
  }
  static std::uint64_t additional_bytes(const DatasetCatalogRequest& request) {
    // Shared compiled path allocations belong to the immutable caller template.
    std::uint64_t bytes=additional_bytes(request.sources)+2*request.format_id.size();
    if(request.dataset_path)bytes+=2*request.dataset_path->size();
    return bytes;
  }
};

struct HotCatalogDomain {
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  CatalogSelection selection;
  std::vector<StructuredBlobManifest> manifests;
  std::vector<QuerySourceReference> sources;
  std::shared_ptr<const Hdf5GlobalAttributeIndex> index;
  std::shared_ptr<const PreparedQueryMetadataTrace> selection_fees,manifest_fees,catalog_fees;
  std::uint64_t catalog_bytes{0},candidate_datasets{0};
};
std::string hot_domain_key(const versioning::Head& head,const DatasetCatalogRequest& request) {
  auto key=owner_domain_key(head,request.sources,request.max_sources,"hdf5-dataset-catalog/1");
  auto text=[&](const std::string& value){key+=std::to_string(value.size())+":"+value;};
  key+="/dataset:";if(request.dataset_path)text(*request.dataset_path);else key+="none";
  return key;
}
}

QueryResult<std::shared_ptr<HotQueryCache::Binding>> IoTDBStructuredBlobBackend::prepare_selector_binding() const {
  using Result=QueryResult<std::shared_ptr<HotQueryCache::Binding>>;
  if(!hot_cache_||!owner_)return Result::failure(QueryStatus::failure(
    QueryError::UnsupportedCapability,QueryPhase::Catalog,"selector handles require an owner namespace and hot cache"));
  try {
    const auto view=owner_->view();
    auto reservation=hot_cache_->reserve(HotQueryCache::Kind::Auxiliary,sizeof(HotQueryCache::Binding)+1024);
    if(!reservation)return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Catalog,"selector binding control reservation exhausted"));
    auto binding=std::make_shared<HotQueryCache::Binding>();
    binding->reservation=std::move(reservation);
    binding->owner=hot_cache_;binding->identity=view.head.identity;
    return Result::success(std::move(binding));
  }catch(const std::exception& error){return Result::failure(context_failure(error));}
}

QueryResult<DatasetCatalogResult> IoTDBStructuredBlobBackend::discover_datasets(
    const DatasetCatalogRequest& request,const DatasetCatalogAuthorizer& authorize,
    const std::shared_ptr<HotQueryCache::Binding>& binding) const {
  auto valid=validate_dataset_catalog_request(request);
  if(!valid.ok())return QueryResult<DatasetCatalogResult>::failure(valid);
  const bool hot_eligible=hot_cache_&&owner_&&request.format_id=="hdf5"&&!request.time_range&&
    !request.sources.file_predicate&&Hdf5GlobalAttributeIndex::supports(request.predicate);
  if(!hot_eligible&&!binding)return discover_datasets_impl(request,authorize,nullptr);
  bool frozen_selection=false;
  try {
    const auto budget=current_query_metadata_budget();
    if(budget&&!budget->status().ok())return QueryResult<DatasetCatalogResult>::failure(budget->status());
    if(!owner_||!hot_cache_)return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(
      QueryError::StalePlan,QueryPhase::Catalog,"selector binding owner is unavailable"));
    const auto view=owner_->view();
    if(binding) {
      std::lock_guard<std::mutex> lock(binding->mutex);
      if(!binding->identity||!(view.head.identity==*binding->identity)||binding->owner.lock()!=hot_cache_)
        return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(
          QueryError::StalePlan,QueryPhase::Catalog,"selector binding belongs to another namespace incarnation"));
      if(request.format_id!="hdf5"||request.sources.file_predicate||request.time_range)
        return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
          QueryPhase::Catalog,"selector binding requires the static HDF5 template contract"));
      if(binding->selector_template) {
        if(binding->template_kind!=HotQueryCache::PayloadKind::Hdf5Dataset||
           !std::static_pointer_cast<const HotSelectorTemplate>(binding->selector_template)->matches(request))
          return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(QueryError::InvalidQuery,
            QueryPhase::Catalog,"selector binding cannot be reused with another immutable template"));
      }else {
        const auto bytes=HotSelectorTemplate::additional_bytes(request);
        if(!binding->reservation||!binding->reservation->grow(bytes))
          return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(QueryError::ResourceExhausted,
            QueryPhase::Catalog,"selector binding template reservation exhausted"));
        try {
          binding->selector_template=std::make_shared<const HotSelectorTemplate>(request);
          binding->template_kind=HotQueryCache::PayloadKind::Hdf5Dataset;
        }catch(...){binding->reservation->shrink(bytes);throw;}
      }
    }
    if(!hot_eligible)return discover_datasets_impl(request,authorize,nullptr);
    HotQueryCache::Claim claim;
    constexpr auto payload_kind=HotQueryCache::PayloadKind::Hdf5Dataset;
    if(binding) {
      std::lock_guard<std::mutex> lock(binding->mutex);
      if(!binding->reference)query_profile_count("hot_query.selector_short_ref_miss_empty");
      else if(!(binding->reference->head==view.head))query_profile_count("hot_query.selector_short_ref_miss_head");
      else if(binding->reference->payload_kind!=payload_kind)query_profile_count("hot_query.selector_short_ref_miss_kind");
      else {
        claim=hot_cache_->claim_resident(view.head,*binding->reference,payload_kind);
        query_profile_count(claim.value?"hot_query.selector_short_ref_hits":"hot_query.selector_short_ref_miss_resident");
      }
    }
    if(!claim.value) {
      const auto key=hot_domain_key(view.head,request);
      query_profile_count("hot_query.selector_long_key_bytes",key.size());
      claim=hot_cache_->claim(view.head,key,payload_kind);
      if(binding&&claim.reference)hot_cache_->bind_reference(binding,*claim.reference);
    }
    if(claim.value) {
      frozen_selection=true;
      auto domain=std::static_pointer_cast<const HotCatalogDomain>(claim.value);
      QueryProfileTimer timer("dataset_catalog_discovery");
      HotFeeSummaryProfile summary_profile(budget);
      const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(domain->selection.read_context);
      if(context)context->arm_hot_fee_summary();
      replay_query_metadata_trace(domain->selection_fees);
      if(!authorize)return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(
        QueryError::PermissionDenied,QueryPhase::Catalog,"dataset discovery requires source authorization"));
      const auto permission=authorize(domain->sources,domain->selection);
      if(!permission.ok())return QueryResult<DatasetCatalogResult>::failure(permission);
      replay_query_metadata_trace(domain->manifest_fees);
      if(domain->catalog_bytes>request.max_catalog_bytes)return QueryResult<DatasetCatalogResult>::failure(
        QueryStatus::failure(QueryError::ResourceExhausted,QueryPhase::Catalog,"dataset catalog byte budget exhausted"));
      replay_query_metadata_trace(domain->catalog_fees);
      auto matches=domain->index->lookup(request.predicate);
      if(!matches)return QueryResult<DatasetCatalogResult>::failure(QueryStatus::failure(
        QueryError::Internal,QueryPhase::Catalog,"cached HDF5 index rejected a supported predicate"));
      DatasetCatalogResult result;result.selected_sources=domain->sources.size();
      result.time_candidate_sources=result.selected_sources;result.candidate_datasets=domain->candidate_datasets;
      result.catalog_read_bytes=domain->catalog_bytes;result.read_context=domain->selection.read_context;
      for(const auto& match:*matches) {
        if(result.datasets.size()==request.max_datasets)return QueryResult<DatasetCatalogResult>::failure(
          QueryStatus::failure(QueryError::ResourceExhausted,QueryPhase::Catalog,"dataset discovery result limit exceeded"));
        const auto& manifest=domain->manifests.at(match.source_ordinal);
        const auto& artifact=*domain->index->catalogs().at(match.source_ordinal);
        const auto& record=artifact.records.at(match.record_ordinal);
        result.datasets.push_back({{manifest.key.path,manifest.key.version,request.format_id},manifest.revision,
          record.object_id,record.canonical_path,artifact.layout_digest,artifact.layout_protocol});
      }
      query_profile_count("hot_query.domain_hits");
      query_profile_count("hot_query.index_lookups");
      query_profile_count("dataset_catalog_selected_sources",result.selected_sources);
      query_profile_count("dataset_catalog_candidate_datasets",result.candidate_datasets);
      query_profile_count("dataset_catalog_time_candidate_sources",result.time_candidate_sources);
      query_profile_count("dataset_catalog_artifact_bytes",result.catalog_read_bytes);
      return QueryResult<DatasetCatalogResult>::success(std::move(result));
    }
    frozen_selection=true;
    if(!claim.ticket)return discover_datasets_impl(request,authorize,nullptr);
    HotCatalogBuild build;build.pool=hot_cache_;build.reservation=claim.ticket->reservation;build.begin();
    auto result=discover_datasets_impl(request,authorize,&build);
    build.capture.reset();
    if(result.ok()&&build.cacheable)try {
      // Captured map nodes/identities are already reserved. Add only the three
      // immutable batch objects/control blocks before moving those maps.
      if(!build.reservation->grow(3*PreparedQueryMetadataTrace::management_bytes)) {
        hot_cache_->count(&HotQueryCacheStats::opt_outs);return result;
      }
      auto domain=std::make_shared<HotCatalogDomain>();
      domain->reservation=build.reservation;domain->selection=std::move(build.selection);
      domain->manifests=std::move(build.manifests);
      domain->selection_fees=std::make_shared<const PreparedQueryMetadataTrace>(
        std::move(build.selection_fees),domain->reservation);
      domain->manifest_fees=std::make_shared<const PreparedQueryMetadataTrace>(
        std::move(build.manifest_fees),domain->reservation);
      domain->catalog_fees=std::make_shared<const PreparedQueryMetadataTrace>(
        std::move(build.catalog_fees),domain->reservation);
      domain->catalog_bytes=build.catalog_bytes;
      const auto index_budget=domain->reservation->available();
      struct IndexBudget {
        std::shared_ptr<HotQueryCache::Reservation> reservation;std::uint64_t refundable{0};
        ~IndexBudget(){if(refundable)reservation->shrink(refundable);}
      } index_reservation{domain->reservation,0};
      if(domain->reservation->grow(index_budget)) {
        index_reservation.refundable=index_budget;
        domain->index=Hdf5GlobalAttributeIndex::build(build.catalogs,index_budget,request.dataset_path);
      }
      if(domain->index) {
        domain->reservation->shrink(index_budget-domain->index->estimated_bytes());
        index_reservation.refundable=0;
        for(const auto& manifest:domain->selection.versions())
          domain->sources.push_back({manifest.key.path,manifest.key.version,request.format_id});
        for(auto count:domain->index->candidate_counts())domain->candidate_datasets+=count;
        const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(domain->selection.read_context);
        // This is derived metadata, never an admission. A racing commit makes
        // this head-keyed entry unreachable; every use still freshly admits.
        if(context&&context->captured_head()==view.head) {
          context->bind_hot_fee_batches(domain->selection_fees,domain->manifest_fees,domain->catalog_fees);
          const auto reference=hot_cache_->publish(claim.ticket,std::move(domain));
          if(binding&&reference)hot_cache_->bind_reference(binding,*reference);
          query_profile_count("hot_query.domain_builds");
        }
      }else hot_cache_->count(&HotQueryCacheStats::opt_outs);
    }catch(const versioning::Error& error) {
      if(error.code()!=versioning::ErrorCode::Conflict)throw;
    }catch(const std::bad_alloc&){hot_cache_->count(&HotQueryCacheStats::opt_outs);}
    return result;
  }catch(const std::bad_alloc& error){
    if(!frozen_selection)return discover_datasets_impl(request,authorize,nullptr);
    return QueryResult<DatasetCatalogResult>::failure(context_failure(error));
  }
   catch(const std::exception& error){return QueryResult<DatasetCatalogResult>::failure(context_failure(error));}
}

QueryResult<DatasetCatalogResult> IoTDBStructuredBlobBackend::discover_datasets_impl(
    const DatasetCatalogRequest &request,
    const DatasetCatalogAuthorizer &authorize,HotCatalogBuild* hot_build) const {
  QueryProfileTimer timer("dataset_catalog_discovery");
  const auto fail = [](QueryError code, std::string msg) {
    const auto budget=current_query_metadata_budget();
    if(budget && !budget->status().ok()) return QueryResult<DatasetCatalogResult>::failure(budget->status());
    return QueryResult<DatasetCatalogResult>::failure(
        QueryStatus::failure(code, QueryPhase::Catalog, std::move(msg)));
  };
  auto valid = validate_dataset_catalog_request(request);
  if (!valid.ok())
    return QueryResult<DatasetCatalogResult>::failure(valid);

  // Freeze source selection before dataset predicates; a matching older version
  // must never replace latest. This stage reads inode identity, not attributes.
  auto selector = request.sources;
  const bool permitted_format = selector.formats.adapters == AdapterSelectionKind::All ||
    std::find(selector.formats.adapter_ids.begin(), selector.formats.adapter_ids.end(), request.format_id) != selector.formats.adapter_ids.end();
  selector.formats = {AdapterSelectionKind::Listed, permitted_format ? std::vector<std::string>{request.format_id} : std::vector<std::string>{}, false};
  // Keep the same namespace/identity filtering of request-local permission
  // evidence as the normal published-version catalog. An in-memory manifest
  // catalog would discard source and bounded exact-ancestor observations.
  auto cat = make_refreshing_published_version_catalog(
    [this](const SourceSelector& effective_selector)
      -> QueryResult<PublishedVersionSnapshot> {
      auto snapshot = load_published_snapshot(effective_selector, true);
      if (snapshot.ok())
        return QueryResult<PublishedVersionSnapshot>::success(
          std::move(snapshot.value()));
      QueryErrorDetail detail;
      detail.operation = "load-published-version-catalog";
      detail.retriable = snapshot.status().code() ==
        StructuredBlobError::BackendFailure;
      const auto budget=current_query_metadata_budget();
      if(budget && !budget->status().ok())
        return QueryResult<PublishedVersionSnapshot>::failure(budget->status());
      const auto code = detail.retriable ? QueryError::ReadFailure
                                        : QueryError::Internal;
      return QueryResult<PublishedVersionSnapshot>::failure(
        QueryStatus::failure(code,
          QueryPhase::Catalog, snapshot.status().message(), std::move(detail)));
    }, storage_namespace());
  auto selected = cat->scan(selector, request.max_sources);
  if(hot_build)hot_build->end(hot_build->selection_fees);
  if (!selected.ok())
    return QueryResult<DatasetCatalogResult>::failure(selected.status());
  if(hot_build&&hot_build->cacheable)try {
    const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(selected.value().read_context);
    if(!context||!hot_build->reservation->grow(context->hot_resident_estimate()))hot_build->cacheable=false;
    else {
      hot_build->selection=selected.value();
      context->enable_hot_proofs(hot_build->pool,hot_build->reservation);
    }
  }catch(const std::bad_alloc&){hot_build->cacheable=false;}
  std::vector<QuerySourceReference> refs;
  for (const auto &m : selected.value().versions())
    refs.push_back({m.key.path, m.key.version, request.format_id});
  if (!authorize)
    return fail(QueryError::PermissionDenied,
                "dataset discovery requires source authorization");
  auto permission = authorize(refs, selected.value());
  if (!permission.ok())
    return QueryResult<DatasetCatalogResult>::failure(permission);
  if(hot_build)hot_build->begin();
  DatasetCatalogResult result;
  result.selected_sources = refs.size();
  result.read_context = selected.value().read_context;
  result.sql_queries = owner_ ? 0 : 1;
  query_profile_count("dataset_catalog_selected_sources", refs.size());
  // Legacy mutable SQL remains serialized. Owner reads use pinned immutable
  // facts; the frontend admits result publication against the live dependencies.
  std::unique_lock<std::recursive_mutex> lock(mutation_mu_,std::defer_lock);
  if (!owner_) lock.lock();
  const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(selected.value().read_context);
  if (owner_) {
    // Selection and authorization consumed only committed identity/permissions.
    // Hydrate exactly those authorized keys now, preserving the selected version
    // even when a newer version appeared while authorization was running.
    if (!context) return fail(QueryError::StalePlan,"owner discovery context is missing");
    std::vector<StructuredBlobManifest> hydrated;
    hydrated.reserve(selected.value().versions().size());
    for (const auto& selected_manifest : selected.value().versions()) {
      auto loaded = context->manifest(selected_manifest.key);
      if (!loaded.ok()) return QueryResult<DatasetCatalogResult>::failure(loaded.status());
      hydrated.push_back(std::move(loaded.value()));
    }
    selected.value().set_versions(std::move(hydrated));
  }
  if(hot_build&&hot_build->cacheable)try {
    std::uint64_t bytes=4096;
    for(const auto& manifest:selected.value().versions()) {
      auto wire=serialize_metadata(manifest.metadata,65536);
      if(!wire.ok()){hot_build->cacheable=false;break;}
      bytes+=4096+8*(wire.value().size()+manifest.key.path.capacity());
    }
    if(hot_build->cacheable&&hot_build->reservation->grow(bytes))hot_build->manifests=selected.value().versions();
    else hot_build->cacheable=false;
  }catch(const std::bad_alloc&){hot_build->cacheable=false;}
  // Validate every frozen source, including time misses, before any catalog
  // artifact read. No later latest selection can replace an authorized key.
  const bool combined = !owner_;
  std::vector<StructuredBlobVersionKey> projected_marker_keys;
  QueryStatus projected_time_status;
  if (combined) {
    // Project the marker subset from the frozen metadata without returning any
    // error yet: fresh manifests, then seals, then time evidence keep priority.
    for (const auto& manifest : selected.value().versions()) {
      if (request.time_range) {
        auto match = matches_verified_utc_time(manifest, *request.dataset_path, *request.time_range);
        if (!match.ok()) { projected_time_status = match.status(); projected_marker_keys.clear(); break; }
        if (!match.value()) continue;
      }
      projected_marker_keys.push_back(manifest.key);
    }
  }
  DatasetCatalogVerification verification;
  if (combined) {
    auto read = read_dataset_verification_unlocked(selected.value().versions(),
      projected_marker_keys, result.sql_queries);
    if (!read.ok()) return QueryResult<DatasetCatalogResult>::failure(read.status());
    verification = std::move(read.value());
  }
  std::map<StructuredBlobVersionKey, DatasetSubstructureSeal> seals;
  // Even time-pruned sources must satisfy the requested index contract. A
  // legacy version cannot silently appear as an empty indexed result.
  for (const auto& manifest : selected.value().versions()) {
    auto seal = read_dataset_substructure_seal(manifest);
    if (!seal.ok()) return fail(QueryError::LayoutUnavailable, seal.status().message());
    seals.emplace(manifest.key, std::move(seal.value()));
  }
  query_profile_count("dataset_substructure_index.selected_sources", seals.size());
  if (!projected_time_status.ok()) return QueryResult<DatasetCatalogResult>::failure(projected_time_status);
  struct PreparedCatalog {
    const StructuredBlobManifest* manifest;
    std::shared_ptr<const DatasetCatalogArtifact> artifact;
    std::map<std::string, std::size_t> records;
  };
  std::vector<PreparedCatalog> prepared;
  {
    QueryProfileTimer preparation("dataset_catalog_prepare");
    std::vector<const StructuredBlobManifest*> time_candidates;
    std::map<StructuredBlobVersionKey, const StructuredBlobManifest*> selected_by_key;
    for (const auto& m : selected.value().versions()) {
      if (request.time_range) {
        auto matched = matches_verified_utc_time(m, *request.dataset_path, *request.time_range);
        if (!matched.ok()) return QueryResult<DatasetCatalogResult>::failure(matched.status());
        if (!matched.value()) continue;
      }
      time_candidates.push_back(&m);
      selected_by_key.emplace(m.key, &m);
    }
    result.time_candidate_sources = time_candidates.size();
    std::map<StructuredBlobVersionKey, DatasetCatalogMarker> markers;
    std::map<StructuredBlobVersionKey, std::shared_ptr<const versioning::VerifiedFileContent>> owner_contents;
    if (owner_) {
      try {
        std::vector<DatasetCatalogMarker> observed;
        std::vector<StructuredBlobManifest> expected;
        for (const auto* m : time_candidates) {
          auto verified = verified_content_unlocked(context->record(m->key));
          const auto& proof = verified->proof;
          const auto protocol = m->metadata.find("__iotdbfs.dataset_catalog.protocol");
          if (proof.catalog_id.empty() || protocol == m->metadata.end() ||
              protocol->second.type() != MetadataValueType::String || !versioning::valid_content_id(proof.layout_digest) ||
              proof.catalog_size > kMaxDatasetCatalogBytes || proof.records > INT64_MAX || proof.attributes > INT64_MAX)
            return fail(QueryError::CorruptLayout, "committed content proof has no valid catalog marker");
          DatasetCatalogMarker marker{m->key, static_cast<std::int64_t>(proof.catalog_size),
            std::get<std::string>(protocol->second.value()), proof.layout_digest,
            proof.layout_protocol, proof.directory_complete, static_cast<std::int64_t>(proof.records),
            static_cast<std::int64_t>(proof.attributes)};
          observed.push_back(marker); expected.push_back(*m);
          markers.emplace(m->key, std::move(marker)); owner_contents.emplace(m->key, std::move(verified));
        }
        auto valid_markers = validate_dataset_catalog_marker_rows(expected, observed);
        if (!valid_markers.ok()) return QueryResult<DatasetCatalogResult>::failure(valid_markers);
      } catch (const std::exception& error) { return fail(QueryError::CorruptLayout, error.what()); }
    }
    if (combined) {
      if (!verification.marker_status.ok()) return QueryResult<DatasetCatalogResult>::failure(verification.marker_status);
      std::vector<StructuredBlobManifest> expected;
      for (const auto* manifest : time_candidates) expected.push_back(*manifest);
      auto valid_markers = validate_dataset_catalog_marker_rows(expected, verification.markers);
      if (!valid_markers.ok()) return QueryResult<DatasetCatalogResult>::failure(valid_markers);
      for (auto& marker : verification.markers) markers.emplace(marker.key, std::move(marker));
    }

    // Admit the complete marker byte budget before reading any catalog object.
    // Object reads check exact length, so serialization is not repeated merely
    // to rediscover sizes already supplied by validated immutable markers.
    if(hot_build)hot_build->end(hot_build->manifest_fees);
    std::uint64_t marker_bytes = 0;
    for (const auto& item : markers) {
      const auto size = static_cast<std::uint64_t>(item.second.size);
      if (size > request.max_catalog_bytes - marker_bytes)
        return fail(QueryError::ResourceExhausted, "dataset catalog byte budget exhausted");
      marker_bytes += size;
    }
    // Logical bytes include hits: cache residency cannot evade a request's
    // catalog byte budget. Fresh manifests/markers were checked above.
    result.catalog_read_bytes = marker_bytes;
    if(hot_build){hot_build->catalog_bytes=marker_bytes;hot_build->begin();}
    std::map<StructuredBlobVersionKey, std::shared_ptr<const DatasetCatalogArtifact>> artifacts;
    std::vector<DatasetCatalogMarker> misses;
    for (const auto* m : time_candidates) {
      const auto& marker = markers.at(m->key);
      std::string seal_digest;
      const auto& seal = seals.at(m->key);
      if (seal.bytes != static_cast<std::uint64_t>(marker.size) ||
          seal.records != static_cast<std::uint64_t>(marker.record_count) ||
          seal.attributes != static_cast<std::uint64_t>(marker.attribute_count))
        return fail(QueryError::CorruptLayout, "dataset catalog marker differs from its publication seal");
      seal_digest = seal.digest;

      if (owner_) {
        try {
          auto artifact = content_artifacts_->catalog(*owner_contents.at(m->key));
          if (!artifact) return fail(QueryError::CorruptLayout, "committed catalog reference is missing");
          auto consistent = validate_dataset_catalog_marker_artifact(marker, *m, *artifact,
            static_cast<std::uint64_t>(marker.size));
          if (!consistent.ok()) return QueryResult<DatasetCatalogResult>::failure(consistent);
          artifacts.emplace(m->key, std::move(artifact));
          continue;
        } catch (const std::exception& error) { return fail(QueryError::CorruptLayout, error.what()); }
      }
      const auto identity = make_dataset_catalog_cache_identity(storage_namespace(), *m, marker, seal_digest);
      auto cached = dataset_catalog_cache_.get(identity);
      if (cached) {
        query_profile_count("dataset_catalog_parsed_cache.hits");
        query_profile_count("dataset_substructure_index.cache_hits");
        artifacts.emplace(m->key, std::move(cached));
      } else {
        query_profile_count("dataset_catalog_parsed_cache.misses");
        query_profile_count("dataset_substructure_index.cache_misses");
        misses.push_back(marker);
      }
    }
    auto object_batches = compile_dataset_catalog_object_sql_batches(misses);
    if (!object_batches.ok()) return QueryResult<DatasetCatalogResult>::failure(object_batches.status());
    if (!object_batches.value().empty()) {
      QueryProfileTimer object_timer("dataset_catalog_object_read");
      for (const auto& batch : object_batches.value()) {
        ++result.sql_queries;
        query_profile_count("dataset_catalog_object_read.calls");
        query_profile_count("dataset_catalog_object_read.keys", batch.markers.size());
        auto rows = execute_query(batch.sql);
        if (!rows) return fail(QueryError::ReadFailure, "cannot read dataset catalog objects");
        std::vector<DatasetCatalogObjectRow> observed;
        std::uint64_t returned_bytes = 0;
        while (publication_has_next(rows)) {
          auto fields = std::move(publication_next(rows)->fields);
          query_profile_count("dataset_catalog_object_read.rows");
          if (observed.size() >= batch.markers.size())
            return fail(QueryError::CorruptLayout, "dataset catalog object SQL returned excess rows");
          if (fields.size() != 4 ||
              !fields[0].longV || !fields[1].stringV || !fields[2].longV || !fields[3].stringV)
            return fail(QueryError::ReadFailure, "dataset catalog object SQL returned malformed or excess rows");
          const auto& bytes = fields[3].stringV.value();
          query_profile_count("dataset_catalog_object_read.bytes", bytes.size());
          if (bytes.size() > batch.expected_bytes - returned_bytes)
            return fail(QueryError::ReadFailure, "dataset catalog object batch exceeded its byte bound");
          returned_bytes += bytes.size();
          observed.push_back({{fields[1].stringV.value(), fields[0].longV.value()}, fields[2].longV.value(), std::move(fields[3].stringV.value())});
        }
        auto valid_objects = validate_dataset_catalog_object_rows(batch.markers, observed);
        if (!valid_objects.ok()) return QueryResult<DatasetCatalogResult>::failure(valid_objects);
        for (const auto& row : observed) {
          auto parsed = deserialize_dataset_catalog({row.bytes.begin(), row.bytes.end()});
          if (!parsed.ok()) return fail(QueryError::ReadFailure, parsed.error());
          const auto& marker = markers.at(row.key);
          const auto& manifest = *selected_by_key.at(row.key);
          auto consistent = validate_dataset_catalog_marker_artifact(marker, manifest, parsed.value(), row.bytes.size());
          if (!consistent.ok()) return QueryResult<DatasetCatalogResult>::failure(consistent);
          std::string seal_digest;
          auto sealed = validate_dataset_substructure_index(manifest, parsed.value());
          if (!sealed.ok()) return fail(QueryError::CorruptLayout, sealed.message());
          seal_digest = seals.at(row.key).digest;
          query_profile_count("dataset_substructure_index.seal_validations");
          query_profile_count("dataset_substructure_index.verified");

          auto artifact = std::make_shared<const DatasetCatalogArtifact>(std::move(parsed.value()));
          const auto evictions_before = dataset_catalog_cache_.evictions();
          dataset_catalog_cache_.put_validated(
            make_dataset_catalog_cache_identity(storage_namespace(), manifest, marker, seal_digest), artifact);
          query_profile_count("dataset_catalog_parsed_cache.evictions",
            dataset_catalog_cache_.evictions() - evictions_before);
          artifacts.emplace(row.key, std::move(artifact));
        }
      }
    }
    for (const auto* selected_manifest : time_candidates) {
      const auto& m = *selected_manifest;
      const auto& artifact = artifacts.at(m.key);
      const auto& a = *artifact;
      // Preserve canonical object ordering; the compiled predicate validates
      // every selected record before any matching row is emitted.
      std::map<std::string, std::size_t> records;
      for (std::size_t j = 0; j < a.records.size(); ++j) {
        const auto& d = a.records[j];
        if (request.dataset_path && d.canonical_path != *request.dataset_path)
          continue;
        records.emplace(d.object_id, j);
      }
      result.candidate_datasets += records.size();
      prepared.push_back({selected_manifest, artifact, std::move(records)});
    }
    if(hot_build) {
      hot_build->end(hot_build->catalog_fees);
      if(hot_build->cacheable)try {
        if(!hot_build->reservation->grow(1024+prepared.size()*sizeof(std::shared_ptr<const DatasetCatalogArtifact>)))hot_build->cacheable=false;
        else for(const auto& catalog:prepared)hot_build->catalogs.push_back(catalog.artifact);
      }catch(const std::bad_alloc&){hot_build->cacheable=false;}
    }
  }
  QueryProfileTimer filtering("dataset_substructure_index_predicate");
  auto predicate = DatasetAttributePredicate::compile(request.predicate);
  if (!predicate.ok()) return QueryResult<DatasetCatalogResult>::failure(predicate.status());
  query_profile_count("dataset_substructure_index.predicate_compiles");
  // Bind and validate ALL candidates before emitting a row or enforcing the
  // output limit, just as the previous schema preflight did. Store only a bit
  // per result; no borrowed row values escape evaluate().
  std::vector<std::vector<bool>> matches;
  matches.reserve(prepared.size());
  for (const auto& catalog : prepared) {
    matches.emplace_back(catalog.artifact->records.size(), false);
    // Keep the original schema-preflight order, independently of the
    // object-id order used to emit matches.
    for (std::size_t row = 0; row < catalog.artifact->records.size(); ++row) {
      const auto& record = catalog.artifact->records[row];
      if (request.dataset_path && record.canonical_path != *request.dataset_path) continue;
      query_profile_count("dataset_substructure_index.predicate_evaluations");
      query_profile_count("dataset_substructure_index.predicate_records");
      auto matched = predicate.value().evaluate(record);
      if (!matched.ok()) return QueryResult<DatasetCatalogResult>::failure(matched.status());
      matches.back()[row] = matched.value();
    }
  }
  query_profile_count("dataset_substructure_index.predicate_bindings", result.candidate_datasets);
  // Prepared records are emitted in canonical source and object-id order.
  for (std::size_t source = 0; source < prepared.size(); ++source) {
    const auto& catalog = prepared[source];
    const auto& manifest = *catalog.manifest;
    for (const auto& item : catalog.records) {
      if (!matches[source][item.second]) continue;
      const auto& record = catalog.artifact->records[item.second];
      if (result.datasets.size() == request.max_datasets)
        return fail(QueryError::ResourceExhausted, "dataset discovery result limit exceeded");
      result.datasets.push_back({{manifest.key.path, manifest.key.version, request.format_id},
        manifest.revision, record.object_id, record.canonical_path,
        catalog.artifact->layout_digest, catalog.artifact->layout_protocol});
      query_profile_count("dataset_substructure_index.matches");
    }
  }
  query_profile_count("dataset_catalog_candidate_datasets", result.candidate_datasets);
  query_profile_count("dataset_catalog_time_candidate_sources", result.time_candidate_sources);
  query_profile_count("dataset_catalog_sql_queries", result.sql_queries);
  query_profile_count("dataset_catalog_artifact_bytes", result.catalog_read_bytes);
  return QueryResult<DatasetCatalogResult>::success(std::move(result));
}

namespace {
QueryResult<CatalogSelection> scan_published_uncached(
    const IoTDBStructuredBlobBackend& backend,const SourceSelector& selector,
    std::size_t max_sources) {
  return make_refreshing_published_version_catalog(
    [&backend](const SourceSelector& effective_selector)
      -> QueryResult<PublishedVersionSnapshot> {
      auto snapshot = backend.load_published_snapshot(effective_selector);
      if (snapshot.ok())
        return QueryResult<PublishedVersionSnapshot>::success(
          std::move(snapshot.value()));
      QueryErrorDetail detail;
      detail.operation = "load-published-version-catalog";
      detail.retriable = snapshot.status().code() ==
        StructuredBlobError::BackendFailure;
      const auto budget=current_query_metadata_budget();
      if(budget && !budget->status().ok())
        return QueryResult<PublishedVersionSnapshot>::failure(budget->status());
      const auto code = snapshot.status().code() ==
          StructuredBlobError::BackendFailure
        ? QueryError::ReadFailure : QueryError::Internal;
      return QueryResult<PublishedVersionSnapshot>::failure(
        QueryStatus::failure(code, QueryPhase::Catalog,
          snapshot.status().message(), std::move(detail)));
    }, backend.storage_namespace())->scan(selector,max_sources);
}

// Candidate selection has its own replay window. Never aggregate it into the
// HDF5-specific four-stage counters. Collector replay deliberately bypasses the
// ledger's prepared-batch counters; mark that case explicitly rather than
// interpreting zero tracked visits as a constant-cost collector path.
struct CandidateFeeReplayProfile {
  std::shared_ptr<QueryMetadataBudget> budget;
  QueryMetadataStageSummaryStats before;
  bool enabled,collector;
  explicit CandidateFeeReplayProfile(std::shared_ptr<QueryMetadataBudget> ledger,std::size_t entries)
    :budget(std::move(ledger)),enabled(active_query_profile!=nullptr),collector(active_query_metadata_trace!=nullptr) {
    if(!enabled)return;
    if(budget)before=budget->stage_summary_stats();
    query_profile_count("hot_query.candidate_fee_logical_entries",entries);
  }
  ~CandidateFeeReplayProfile() {
    if(!enabled)return;
    const auto after=budget?budget->stage_summary_stats():QueryMetadataStageSummaryStats{};
    query_profile_count("hot_query.candidate_fee_entries_visited",after.full_fee_entries_visited-before.full_fee_entries_visited);
    query_profile_count("hot_query.candidate_fee_entries_skipped",after.borrowed_fee_entries_skipped-before.borrowed_fee_entries_skipped);
    query_profile_count("hot_query.candidate_fee_summary_stages",after.successful_stages-before.successful_stages);
    query_profile_count("hot_query.candidate_fee_collector_untracked",collector?1:0);
  }
};

struct HotCandidateDomain {
  // The charge outlives all selection, context and derived-artifact pins.
  std::shared_ptr<const void> retained_charge;
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  CatalogSelection selection;
  std::shared_ptr<const PreparedQueryMetadataTrace> selection_fees;
};

std::uint64_t candidate_selection_bytes(const CatalogSelection& selected) {
  // Context accounting includes the entire selected namespace snapshot and
  // Facts, not merely matching sources. Additional copies/permission evidence,
  // callback/control objects and future exact payload capability/proof wrappers
  // are conservatively reserved here. Derived metadata/index bytes are charged
  // separately by QueryDomainCache against this SAME reservation.
  std::uint64_t bytes=16384+selected.versions().capacity()*sizeof(StructuredBlobManifest);
  for(const auto& manifest:selected.versions()) {
    auto encoded=serialize_metadata(manifest.metadata,65536);
    if(!encoded.ok())throw std::bad_alloc();
    bytes+=16384+8*(manifest.key.path.capacity()+encoded.value().size()+
      (manifest.format_id?manifest.format_id->capacity():0));
  }
  if(selected.source_permissions) {
    bytes+=4096;
    for(const auto& item:selected.source_permissions->sources)
      bytes+=1024+4*(item.first.path.capacity()+item.second.format_id.capacity());
    for(const auto& item:selected.source_permissions->parents)bytes+=1024+4*item.first.capacity();
  }
  return bytes;
}

class IoTDBPublishedVersionCatalog final : public PublishedVersionCatalog {
public:
  explicit IoTDBPublishedVersionCatalog(std::shared_ptr<const IoTDBStructuredBlobBackend> backend)
    : backend_(std::move(backend)) {}
  QueryResult<CatalogSelection> scan(const SourceSelector& selector,std::size_t maximum) const override {
    if(!backend_)return QueryResult<CatalogSelection>::failure(QueryStatus::failure(
      QueryError::InvalidQuery,QueryPhase::Catalog,"IoTDB Published Version Catalog requires a backend"));
    return backend_->scan_published_versions(selector,maximum);
  }
private:
  std::shared_ptr<const IoTDBStructuredBlobBackend> backend_;
};
}

QueryResult<CatalogSelection> IoTDBStructuredBlobBackend::scan_published_versions(
    const SourceSelector& selector,std::size_t max_sources,
    const std::shared_ptr<HotQueryCache::Binding>& binding) const {
  auto valid=validate_source_selector(selector);
  if(!valid.ok())return QueryResult<CatalogSelection>::failure(valid);
  const bool eligible=owner_&&hot_cache_&&!selector.file_predicate&&max_sources>0&&
    selector.formats.adapters==AdapterSelectionKind::Listed&&!selector.formats.include_byte_passthrough&&
    selector.formats.adapter_ids.size()==1&&
    (selector.formats.adapter_ids.front()=="grib"||selector.formats.adapter_ids.front()=="hdf5");
  if(!eligible&&!binding)return scan_published_uncached(*this,selector,max_sources);
  bool frozen_selection=false;
  try {
    const auto budget=current_query_metadata_budget();
    if(budget&&!budget->status().ok())return QueryResult<CatalogSelection>::failure(budget->status());
    if(!owner_||!hot_cache_)return QueryResult<CatalogSelection>::failure(QueryStatus::failure(
      QueryError::StalePlan,QueryPhase::Catalog,"selector binding owner is unavailable"));
    const auto view=owner_->view(); // A cache hit never hides a frozen owner.
    constexpr auto payload_kind=HotQueryCache::PayloadKind::CandidateDomain;
    if(binding) {
      std::lock_guard<std::mutex> lock(binding->mutex);
      if(!binding->identity||!(view.head.identity==*binding->identity)||binding->owner.lock()!=hot_cache_)
        return QueryResult<CatalogSelection>::failure(QueryStatus::failure(QueryError::StalePlan,
          QueryPhase::Catalog,"selector binding belongs to another namespace incarnation"));
      if(!eligible)return QueryResult<CatalogSelection>::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
        QueryPhase::Catalog,"candidate selector binding requires one supported metadata adapter and no file predicate"));
      if(binding->selector_template) {
        if(binding->template_kind!=payload_kind||
           !std::static_pointer_cast<const HotSelectorTemplate>(binding->selector_template)->matches(selector,max_sources))
          return QueryResult<CatalogSelection>::failure(QueryStatus::failure(QueryError::InvalidQuery,
            QueryPhase::Catalog,"selector binding cannot be reused with another immutable template"));
      }else {
        const auto bytes=HotSelectorTemplate::additional_bytes(selector);
        if(!binding->reservation||!binding->reservation->grow(bytes))
          return QueryResult<CatalogSelection>::failure(QueryStatus::failure(QueryError::ResourceExhausted,
            QueryPhase::Catalog,"selector binding template reservation exhausted"));
        try {
          binding->selector_template=std::make_shared<const HotSelectorTemplate>(selector,max_sources);
          binding->template_kind=payload_kind;
        }catch(...){binding->reservation->shrink(bytes);throw;}
      }
    }
    // Positive Exact requests are lightweight views of a resident immutable
    // backing. Their first scan still follows the original catalog path so
    // metadata fees, errors and complete selected-candidate evidence agree.
    // Views consume bytes within that backing, never additional domain slots.
    if(selector.versions.kind==VersionSelectionKind::Exact&&!selector.paths.empty()) {
      const auto child_key=owner_domain_key(view.head,selector,max_sources,"published-candidates/1");
      for(const auto& resident:hot_cache_->resident_domains(view.head)) {
        if(!resident.reference)continue;
        std::shared_ptr<const VersionDirectoryReadContext> backing;
        if(resident.reference->payload_kind==HotQueryCache::PayloadKind::Hdf5Dataset)
          backing=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(
            std::static_pointer_cast<const HotCatalogDomain>(resident.value)->selection.read_context);
        else if(resident.reference->payload_kind==HotQueryCache::PayloadKind::CandidateDomain)
          backing=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(
            std::static_pointer_cast<const HotCandidateDomain>(resident.value)->selection.read_context);
        if(!backing||!backing->covers_exact_view(selector))continue;
        // Acquiring the resident pin is separate from the inspection snapshot;
        // a view cannot make an already evicted backing reachable again.
        auto pin=hot_cache_->claim_resident(view.head,*resident.reference,resident.reference->payload_kind);
        if(!pin.value)continue;
        if(auto child=backing->exact_view(child_key)) {
          frozen_selection=true;
          if(child->selection.read_context)child->selection.read_context->arm_domain_metadata_summary();
          {
            QueryProfileTimer timer("candidate_domain_fee_replay");
            CandidateFeeReplayProfile profile(budget,child->selection_fees->fees().size());
            replay_query_metadata_trace(child->selection_fees);
          }
          query_profile_count("hot_query.exact_view_hits");
          query_profile_count("hot_query.candidate_domain_hits");
          query_profile_count("hot_query.domain_hits");
          return QueryResult<CatalogSelection>::success(child->selection);
        }
        if(!backing->can_retain_exact_view())continue;
        const auto reservation=backing->backing_reservation();
        auto charge=HotChildCharge::make(reservation,8192+8*child_key.capacity()+
          PreparedQueryMetadataTrace::management_bytes);
        if(!charge)continue;
        std::unique_ptr<ScopedQueryMetadataTrace> trace;
        try {
          trace=std::make_unique<ScopedQueryMetadataTrace>("",[charge](std::uint64_t bytes) {
            if(!charge->grow(bytes))throw std::bad_alloc();
          },QueryMetadataTracePolicy::CacheStable);
        }catch(const std::bad_alloc&){continue;}
        frozen_selection=true;
        auto result=scan_published_uncached(*this,selector,max_sources);
        const bool cacheable=!trace->failed();
        auto fees=cacheable?trace->release():QueryMetadataTrace{};trace.reset();
        if(!result.ok()||!cacheable)return result;
        try {
          const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(result.value().read_context);
          if(!context||!(context->captured_head()==view.head)||
             !charge->grow(context->hot_resident_estimate()+candidate_selection_bytes(result.value())))return result;
          auto shared=context->share_backing(*backing,charge);
          if(!shared)return result;
          auto child=std::make_shared<HotCandidateDomain>();child->retained_charge=charge;
          child->reservation=reservation;
          child->selection_fees=std::make_shared<const PreparedQueryMetadataTrace>(std::move(fees),charge);
          shared->enable_hot_proofs(hot_cache_,reservation,false);
          shared->bind_candidate_selection_fees(child->selection_fees);
          result.value().read_context=std::move(shared);
          result.value().hot_state=std::make_shared<QueryDomainCache>(
            [charge](std::uint64_t bytes){return charge->grow(bytes);},
            [charge](std::uint64_t bytes){charge->shrink(bytes);},
            [reservation]{return reservation->available();},
            [pool=std::weak_ptr<HotQueryCache>(hot_cache_)](std::shared_ptr<const void> value)
                ->std::shared_ptr<QueryDomainRetention> {
              const auto owner=pool.lock();
              return owner?owner->retain(std::move(value),HotQueryCache::Retention::Derived):nullptr;
            });
          child->selection=result.value();
          // The bounded parent map owns its children, while no child owns the
          // parent context or index. In-flight selections retain just backing
          // bytes and their own view charge through ordinary shared ownership.
          if(backing->retain_exact_view(child_key,std::move(child)))
            query_profile_count("hot_query.exact_view_builds");
        }catch(const std::bad_alloc&){hot_cache_->count(&HotQueryCacheStats::opt_outs);}
        return result;
      }
    }
    HotQueryCache::Claim claim;
    if(binding) {
      std::lock_guard<std::mutex> lock(binding->mutex);
      if(!binding->reference)query_profile_count("hot_query.selector_short_ref_miss_empty");
      else if(!(binding->reference->head==view.head))query_profile_count("hot_query.selector_short_ref_miss_head");
      else if(binding->reference->payload_kind!=payload_kind)query_profile_count("hot_query.selector_short_ref_miss_kind");
      else {
        claim=hot_cache_->claim_resident(view.head,*binding->reference,payload_kind);
        query_profile_count(claim.value?"hot_query.selector_short_ref_hits":"hot_query.selector_short_ref_miss_resident");
      }
    }
    if(!claim.value) {
      const auto key=owner_domain_key(view.head,selector,max_sources,"published-candidates/1");
      query_profile_count("hot_query.selector_long_key_bytes",key.size());
      claim=hot_cache_->claim(view.head,key,payload_kind);
      if(binding&&claim.reference)hot_cache_->bind_reference(binding,*claim.reference);
    }
    if(claim.value) {
      frozen_selection=true;
      auto domain=std::static_pointer_cast<const HotCandidateDomain>(claim.value);
      if(domain->selection.read_context)domain->selection.read_context->arm_domain_metadata_summary();
      {
        QueryProfileTimer timer("candidate_domain_fee_replay");
        CandidateFeeReplayProfile profile(budget,domain->selection_fees->fees().size());
        replay_query_metadata_trace(domain->selection_fees);
      }
      query_profile_count("hot_query.domain_hits");
      query_profile_count("hot_query.candidate_domain_hits");
      return QueryResult<CatalogSelection>::success(domain->selection);
    }
    if(!claim.ticket)return scan_published_uncached(*this,selector,max_sources);
    frozen_selection=true;
    HotCatalogBuild build;build.pool=hot_cache_;build.reservation=claim.ticket->reservation;build.begin();
    auto result=scan_published_uncached(*this,selector,max_sources);
    build.end(build.selection_fees);
    if(!result.ok()||!build.cacheable)return result;
    try {
      const auto context=std::dynamic_pointer_cast<const VersionDirectoryReadContext>(result.value().read_context);
      if(!context||!(context->captured_head()==view.head))return result;
      const auto bytes=context->hot_resident_estimate()+candidate_selection_bytes(result.value())+
        PreparedQueryMetadataTrace::management_bytes;
      if(!build.reservation->grow(bytes)) {hot_cache_->count(&HotQueryCacheStats::opt_outs);return result;}
      auto domain=std::make_shared<HotCandidateDomain>();domain->reservation=build.reservation;
      const auto reservation=domain->reservation;
      auto state=std::make_shared<QueryDomainCache>(
        [reservation](std::uint64_t count){return reservation->grow(count);},
        [reservation](std::uint64_t count){reservation->shrink(count);},
        [reservation]{return reservation->available();},
        [pool=std::weak_ptr<HotQueryCache>(hot_cache_)](std::shared_ptr<const void> value)
            ->std::shared_ptr<QueryDomainRetention> {
          const auto owner=pool.lock();
          return owner?owner->retain(std::move(value),HotQueryCache::Retention::Derived):nullptr;
        });
      domain->selection_fees=std::make_shared<const PreparedQueryMetadataTrace>(
        std::move(build.selection_fees),reservation);
      context->enable_hot_proofs(hot_cache_,reservation);
      context->bind_candidate_selection_fees(domain->selection_fees);
      result.value().hot_state=std::move(state);
      domain->selection=result.value();
      const auto reference=hot_cache_->publish(claim.ticket,std::move(domain));
      if(binding&&reference)hot_cache_->bind_reference(binding,*reference);
      query_profile_count("hot_query.domain_builds");
      query_profile_count("hot_query.candidate_domain_builds");
    }catch(const std::bad_alloc&) {hot_cache_->count(&HotQueryCacheStats::opt_outs);}
    return result;
  }catch(const std::bad_alloc& error) {
    // Failure of optional key/claim bookkeeping before selection can decline.
    // Once a domain is frozen, never reselect Latest merely to retry caching.
    if(!frozen_selection)return scan_published_uncached(*this,selector,max_sources);
    return QueryResult<CatalogSelection>::failure(context_failure(error));
  }catch(const std::exception& error) {
    return QueryResult<CatalogSelection>::failure(context_failure(error));
  }
}

std::shared_ptr<const PublishedVersionCatalog>
make_iotdb_published_version_catalog(std::shared_ptr<const IoTDBStructuredBlobBackend> backend) {
  return std::make_shared<const IoTDBPublishedVersionCatalog>(std::move(backend));
}

} // namespace iotdbfs
