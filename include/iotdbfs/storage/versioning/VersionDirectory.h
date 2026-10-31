#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace iotdbfs::versioning {

using Bytes = std::vector<std::uint8_t>;
using ContentId = std::string; // Canonical lower-case SHA-256 hex, never an authorization token.
inline constexpr std::size_t kHeadBytes = 4096;
inline constexpr std::size_t kPageBytes = 65536;
inline constexpr std::size_t kPageEntries = 256;
inline constexpr std::size_t kTreeHeight = 8;
inline constexpr std::uint64_t kMetadataBytes = 64ULL * 1024 * 1024;
inline constexpr std::size_t kScanEntries = 65536;

enum class ErrorCode { Invalid, Corrupt, Limit, Missing, Conflict, Unavailable, Frozen };
class Error : public std::runtime_error {
public:
  Error(ErrorCode code, const std::string& message) : std::runtime_error(message), code_(code) {}
  ErrorCode code() const { return code_; }
private:
  ErrorCode code_;
};

struct NamespaceIdentity {
  std::string namespace_id;
  std::string incarnation;
  friend bool operator==(const NamespaceIdentity& a, const NamespaceIdentity& b) {
    return a.namespace_id == b.namespace_id && a.incarnation == b.incarnation;
  }
};
std::string random_uuid();
bool valid_uuid(const std::string&);
bool valid_content_id(const ContentId&);

struct ObjectRef {
  std::string type;
  std::string protocol;
  ContentId id;
};
struct ObjectReadRequest {
  ObjectRef ref;
  std::uint64_t size{0}; // Exact encoded size, not an upper bound.
};
inline constexpr std::size_t kPayloadBlockBytes = 1024 * 1024;
inline constexpr std::size_t kObjectBatchEntries = 64;
inline constexpr std::uint64_t kObjectBatchBytes = 8ULL * 1024 * 1024;
ContentId content_id(const std::string& type, const std::string& protocol, const Bytes&);
ObjectRef object_ref(const std::string& type, const Bytes&);

enum class Table : std::uint8_t { Current = 0, Exact = 1, Liveness = 2, Permission = 3 };
using Roots = std::array<ContentId, 4>;
struct Head {
  NamespaceIdentity identity;
  std::uint64_t sequence{0};
  ContentId commit;
  ContentId root;
  friend bool operator==(const Head& a, const Head& b) {
    return a.identity == b.identity && a.sequence == b.sequence && a.commit == b.commit && a.root == b.root;
  }
};
struct Commit {
  NamespaceIdentity identity;
  std::uint64_t sequence{0};
  ContentId previous_commit;
  ContentId root;
  std::string nonce;
  std::string operation;
  std::vector<std::string> dependencies; // sorted, unique table-byte + exact key
};
struct PageEntry { std::string key; ContentId value; };
struct Page {
  std::uint8_t level{0}; // leaf=0, internal children have exactly level-1.
  std::vector<PageEntry> entries; // internal key is inclusive maximum child key.
};
Bytes encode_head(const Head&);
Head decode_head(const Bytes&);
Bytes encode_commit(const Commit&);
Commit decode_commit(const Bytes&);
Bytes encode_roots(const Roots&);
Roots decode_roots(const Bytes&);
Bytes encode_page(const Page&);
Page decode_page(const Bytes&);

enum class HeadWriteResult { Acknowledged, Indeterminate };
class Store {
public:
  virtual ~Store() = default;
  virtual std::optional<Bytes> read_head() = 0;
  virtual Bytes get_object(const NamespaceIdentity&, const ObjectRef&) = 0;
  // One native batch, unique references, exact sizes, results in request order.
  virtual std::vector<Bytes> get_objects(const NamespaceIdentity&,
    const std::vector<ObjectReadRequest>&) = 0;
  virtual void put_object(const NamespaceIdentity&, const ObjectRef&, const Bytes&) = 0;
  // A single shot at the fixed cell. This is NOT a database CAS.
  virtual HeadWriteResult write_head(const Bytes&) = 0;
  // Called only after independently reading exactly the attempted target head.
  virtual void accept_reconciled_head() {}
};

struct JournalState {
  NamespaceIdentity identity;
  std::optional<Bytes> committed;
  std::optional<Bytes> pending;
};
class Journal {
public:
  virtual ~Journal() = default;
  virtual std::optional<JournalState> load() = 0;
  virtual void save(const JournalState&) = 0; // durable before return
};

// Mandatory private, host-local owner registry + flock + fsync journal. The
// registry survives process restarts and detects database rollback against its
// high-water mark. It does not claim distributed fencing or survive rollback
// of BOTH database and this external registry.
class FileJournal final : public Journal {
public:
  explicit FileJournal(const std::string& directory);
  ~FileJournal() override;
  FileJournal(const FileJournal&) = delete;
  FileJournal& operator=(const FileJournal&) = delete;
  std::optional<JournalState> load() override;
  void save(const JournalState&) override;
private:
  std::string directory_;
  int directory_fd_{-1};
  int lock_fd_{-1};
};

struct View { Head head; Roots roots; };
struct OwnerConfig {
  std::size_t cache_bytes{64ULL * 1024 * 1024};
  std::size_t change_commits{256}, change_keys{65536}, change_bytes{8ULL * 1024 * 1024};
  bool coalesce_payload_reads{true};
  std::size_t payload_cache_bytes{8ULL * 1024 * 1024};
};
struct OwnerStats {
  std::uint64_t object_reads{0}, object_cache_hits{0}, page_cache_hits{0}, hash_bytes{0};
  std::uint64_t cache_bytes{0}, cache_entries{0}, evictions{0};
  std::uint64_t head_control_calls{0}, dependency_checks{0}, changed_dependencies{0}, history_gaps{0};
  std::uint64_t active_execution_leases{0};
  std::uint64_t payload_coalesce_leaders{0}, payload_coalesce_joins{0};
  std::uint64_t payload_coalesce_bypasses{0}, payload_coalesce_fallbacks{0};
  std::uint64_t payload_coalesce_active_flights{0}, payload_coalesce_retained_entries{0};
  std::uint64_t payload_coalesce_retained_bytes{0};
  std::uint64_t payload_cache_hits{0},payload_cache_misses{0},payload_cache_insertions{0};
  std::uint64_t payload_cache_evictions{0},payload_cache_pressure_reclaims{0};
  std::uint64_t payload_cache_resident_entries{0},payload_cache_resident_bytes{0};
};
enum class DependencyState { Unchanged, Changed, Unknown };
struct DependencyCheck { DependencyState state; View current; std::size_t changed_dependencies{0}; };
class Owner;
class Transaction {
public:
  ~Transaction();
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;
  void put(Table, const std::string& key, const Bytes& record);
  void erase(Table, const std::string& key);
  std::optional<Bytes> get(Table, const std::string& key);
  ObjectRef put_artifact(const std::string& type, const Bytes&);
  ObjectRef put_payload_block(const Bytes&);
  void commit();
  void abort();
private:
  friend class Owner;
  Transaction(Owner&, std::string operation);
  Owner& owner_;
  // Serializes writers only; immutable preparation does not retain admission.
  std::unique_lock<std::recursive_mutex> writer_lock_;
  View base_;
  Roots roots_;
  std::string operation_;
  std::vector<std::string> dependencies_;
  std::uint64_t bytes_{0};
  bool done_{false};
};

class Owner {
public:
  Owner(std::shared_ptr<Store>, std::shared_ptr<Journal>, OwnerConfig config = {});
  ~Owner();
  class Admission {
    friend class Owner;
    explicit Admission(std::recursive_mutex& mutex) : lock_(mutex) {}
    std::unique_lock<std::recursive_mutex> lock_;
  };
  // Caller prepares evidence without a gate, then admits only that exact head.
  // Conflict means retry/revalidate; a returned guard serializes short publish/open.
  std::unique_ptr<Admission> admit(const Head& expected) const;
  DependencyCheck check_dependencies(const View&, const std::vector<std::string>& dependencies) const;
  std::shared_ptr<void> acquire_execution_lease() const;
  OwnerStats stats() const;
  void clear_read_cache();
  // Explicit fresh initialization only. Never converts missing head into a new
  // namespace during open/recovery, and never imports legacy inode tables.
  using Seed = std::tuple<Table, std::string, Bytes>;
  void initialize(const NamespaceIdentity&, const std::vector<Seed>& seeds = {});
  void recover();
  View view() const;
  std::unique_ptr<Transaction> begin(const std::string& operation);
  std::optional<Bytes> get(const View&, Table, const std::string& key) const;
  // Enumerate/validate the same prefix references and pre-filter limit before
  // optionally excluding record values. Page fees and readiness checks remain
  // unchanged. The internal predicate never evaluates content or permissions.
  std::vector<std::pair<std::string, Bytes>> scan(const View&, Table,
    const std::string& prefix = {}, std::size_t limit = kScanEntries,
    const std::function<bool(const std::string&)>& key_predicate = {}) const;
  bool frozen() const;
  Bytes read_artifact(const View&, const ObjectRef&, std::uint64_t& metadata_bytes) const;
  std::vector<Bytes> read_artifacts(const View&, const std::vector<ObjectReadRequest>&,
    std::uint64_t& metadata_bytes, bool payload = false, bool force = false,
    std::shared_ptr<void> temporary_pin = {}) const;
private:
  friend class Transaction;
  void require_ready() const;
  void publish(const Roots&, const std::string& operation,
    const std::vector<std::string>& dependencies = {});
  Bytes checked_object(const NamespaceIdentity&, const ObjectRef&, std::uint64_t&, bool force = false) const;
  std::shared_ptr<const Page> checked_page(const NamespaceIdentity&, const ContentId&, std::uint64_t&, bool force = false) const;
  struct Runtime;
  std::unique_ptr<Runtime> runtime_;
  ContentId put_object(const std::string& type, const Bytes&, std::uint64_t&);
  std::shared_ptr<Store> store_;
  std::shared_ptr<Journal> journal_;
  // Lock order: writer_mutex_ before mutex_. Readers/admissions never acquire
  // writer_mutex_; callers must not begin a writer while holding Admission.
  mutable std::recursive_mutex writer_mutex_;
  mutable std::recursive_mutex mutex_;
  std::optional<View> current_;
  std::optional<JournalState> state_;
  bool frozen_{true};
  bool transaction_active_{false};
};

// Binary key: normalized absolute POSIX path bytes, NUL, nonnegative version
// as big-endian u64. Path components are byte-preserving, not Unicode folded.
// No history traversal is needed to address an exact binding.
std::string exact_key(const std::string& path, std::int64_t version);
std::pair<std::string, std::int64_t> decode_exact_key(const std::string&);

} // namespace iotdbfs::versioning
