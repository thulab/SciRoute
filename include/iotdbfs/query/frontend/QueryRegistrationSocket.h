#pragma once

#include "iotdbfs/query/frontend/TypedQueryFrontend.h"
#include "iotdbfs/formats/hdf5/Hdf5FragmentProvider.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/query/frontend/FuseTypedQueryBridge.h"
#include "iotdbfs/query/frontend/BoundedResultSender.h"
#include "iotdbfs/query/core/QueryRuntime.h"
#include "iotdbfs/query/core/DatasetCatalogQuery.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/frontend/PreparedTrainingRegistry.h"
#include "iotdbfs/query/frontend/PreparedSelectorRegistry.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <thread>
#include <vector>

namespace iotdbfs {

// Chooses a private control socket location independent of the mountpoint's
// parent directory. An empty runtime_dir uses the system runtime fallback.
QueryResult<std::string> default_query_registration_socket_path(
  std::string_view mount_root, std::string_view runtime_dir);

// A local control endpoint for registering immutable queries in the mount
// process. Each connection carries one big-endian uint32 length followed by
// one JSON request, and receives one frame with a JSON response. Frames are
// capped at 1 MiB. Peer credentials are obtained from the Unix socket; a
// shared socket must be explicitly enabled and must authorize every source.
// typed_batch carries 1..64 complete typed declarations under items, each
// with Exact paths/versions. It returns indexed published_routes only after
// all items succeed; resources are accounted across the entire request.
struct QueryRegistrationPeer {
  uid_t uid{};
  gid_t gid{};
  std::vector<gid_t> supplementary_groups;
};

struct QueryControlResponse {
  enum class Kind { PublishedRoute, PublishedRoutes, PreparedWindow, ReleasedWindow, ReleasedResult, DatasetCatalog, FormatCatalog, PreparedCatalogSelector, ReleasedCatalogSelector, PreparedQueryTemplate, ReleasedQueryTemplate };
  struct Item { std::size_t index{0}; std::string path; };
  Kind kind{Kind::PublishedRoute};
  std::string path;
  std::string prepared_id;
  std::string selector_id;
  std::string template_id;
  // Request-local publication pin; never serialized or accepted from a client.
  PreparedSelectorRegistry::Payload prepared_payload;
  std::string snapshot_fingerprint;
  std::uint64_t ttl_ms{0};
  bool released{false};
  DatasetCatalogResult datasets;
  FormatCatalogResult format_units;
  std::vector<Item> items;
};

class QueryRegistrationSocketServer {
public:
  // The optional selection is an immediate typed-registration or dataset-
  // discovery observation. It must never be retained as permission evidence
  // for later route access. Training and fallback materialization pass nullptr.
  using Authorization = std::function<QueryStatus(const QueryRegistrationPeer&,
    const std::vector<QuerySourceReference>&, const CatalogSelection*)>;

  // Capture before a permission check, then commit only after that check and
  // publication succeed. The commit receives a mount-relative immutable route.
  // Empty callbacks disable this optional optimization.
  using AuthorizationPublicationCommit = std::function<bool(std::string_view)>;
  using BeginAuthorizationPublication = std::function<AuthorizationPublicationCommit(
    const QueryRegistrationPeer&, const CatalogSelection*)>;

  using DatasetDiscovery = std::function<QueryResult<DatasetCatalogResult>(
    const DatasetCatalogRequest&, const DatasetCatalogAuthorizer&,
    const std::shared_ptr<HotQueryCache::Binding>&)>;
  using PrepareSelectorBinding = std::function<
    QueryResult<std::shared_ptr<HotQueryCache::Binding>>()>;
  using SelectSources = std::function<QueryResult<CatalogSelection>(
    const SourceSelector&, std::size_t,
    const std::shared_ptr<HotQueryCache::Binding>&)>;

  QueryRegistrationSocketServer(std::string socket_path, std::string mount_root,
    std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<const PublishedVersionCatalog> catalog,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    std::shared_ptr<FuseTypedQueryBridge> bridge,
    Authorization authorize = {}, mode_t socket_mode = 0600,
    TrainingExecutionOptions training_execution = {},
    PreparedTrainingRegistry::Config prepared_config = {},
    Hdf5QueryKernelConfig hdf5_execution = {},
    DatasetDiscovery discover = {},
    std::shared_ptr<Hdf5ValidatedLayoutCache> hdf5_layout_cache = {},
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>> hdf5_profiles = {},
    BeginAuthorizationPublication begin_authorization_publication = {},
    bool owner_metadata_budget = false,
    Hdf5QueryMode hdf5_mode = Hdf5QueryMode::IoTDBIndexed,
    std::shared_ptr<const QueryModule> query_module = {},
    std::shared_ptr<HotQueryCache> selector_pool = {},
    PrepareSelectorBinding prepare_selector_binding = {},
    SelectSources select_sources = {},
    BoundedResultSender::Config result_sender_config = {});
  ~QueryRegistrationSocketServer();

  QueryRegistrationSocketServer(const QueryRegistrationSocketServer&) = delete;
  QueryRegistrationSocketServer& operator=(const QueryRegistrationSocketServer&) = delete;

  QueryStatus start();
  void stop();
  const std::string& socket_path() const { return socket_path_; }

private:
  struct ParsedTypedRequest;
  struct ParsedControlRequest;
  struct Connection;
  struct TaskResourcePlan;
  void serve();
  void serve_worker();
  void serve_connection(std::shared_ptr<Connection> connection);
  QueryStatus serve_fetch(int fd, const ParsedControlRequest& parsed,
    const QueryRegistrationPeer& peer,
    const std::shared_ptr<BoundedResultSender::Reservation>& reservation,
    std::shared_ptr<void> lifetime,
    std::shared_ptr<BoundedResultSender::TimingRecord> timing = {});
  void record_fetch_timing(BoundedResultSender::TimingRecord&& timing) noexcept;
  void flush_fetch_timings() noexcept;
  TaskResourcePlan task_resources(
    const ParsedControlRequest& parsed, uid_t uid) const;
  QueryStatus serve_request(int fd, const ParsedControlRequest& parsed,
    const QueryRegistrationPeer& peer,
    std::chrono::steady_clock::time_point queued_at, const TaskResourcePlan* plan);
  QueryResult<QueryControlResponse> register_json(const std::string& json,
    const QueryRegistrationPeer& peer, std::string& operation, std::string& prepared_id,
    bool record_total = true, const TaskResourcePlan* plan = nullptr,
    const ParsedControlRequest* parsed = nullptr);
  QueryResult<QueryControlResponse> execute_validated_typed(
    ParsedTypedRequest declaration, const QueryRegistrationPeer& peer);
  QueryResult<QueryControlResponse> discover_format_catalog(
    const QueryRequest& request, const QueryRegistrationPeer& peer,
    const std::shared_ptr<HotQueryCache::Binding>& binding = {});
  QueryResult<std::string> publish_route(std::string route,
    const QueryRegistrationPeer& peer, const CatalogSelection* authorized_selection = nullptr,
    AuthorizationPublicationCommit observed_authorization = {});
  AuthorizationPublicationCommit begin_authorization_publication(
    const QueryRegistrationPeer& peer, const CatalogSelection* selection);
  void retire_path(const std::string& path);

  std::string socket_path_;
  std::string mount_root_;
  std::shared_ptr<TypedQueryFrontend> frontend_;
  std::shared_ptr<const PublishedVersionCatalog> catalog_;
  std::shared_ptr<const StructuredBlobStorage> storage_;
  std::shared_ptr<QueryRuntime> runtime_;
  std::shared_ptr<FuseTypedQueryBridge> bridge_;
  Authorization authorize_;
  BeginAuthorizationPublication begin_authorization_publication_;
  DatasetDiscovery discover_;
  PrepareSelectorBinding prepare_selector_binding_;
  SelectSources select_sources_;
  std::unique_ptr<PreparedSelectorRegistry> selectors_;
  std::shared_ptr<HotQueryCache> profile_hot_pool_;
  mode_t socket_mode_;
  // Resource scheduling is chosen by the daemon, never by socket clients.
  const TrainingExecutionOptions training_execution_;
  const Hdf5QueryKernelConfig hdf5_execution_;
  std::shared_ptr<Hdf5ValidatedLayoutCache> hdf5_layout_cache_;
  const std::vector<std::shared_ptr<const Hdf5SemanticProfile>> hdf5_profiles_;
  std::unique_ptr<PreparedTrainingRegistry> prepared_;
  std::mutex preparation_mutex_;
  const bool owner_metadata_budget_;
  const Hdf5QueryMode hdf5_mode_;
  std::shared_ptr<const QueryModule> query_module_;
  std::atomic<bool> running_{false};
  int listener_{-1};
  int lock_fd_{-1};
  dev_t socket_dev_{};
  ino_t socket_ino_{};
  std::thread accept_thread_;
  std::vector<std::thread> workers_;
  std::mutex queue_mutex_;
  std::condition_variable queue_ready_;
  std::deque<std::pair<int, std::uint64_t>> pending_;
  // Accepted query tasks may outlive their transport worker. stop() joins
  // transport workers, then drains just this server's tasks before registries.
  std::condition_variable requests_finished_;
  std::size_t outstanding_requests_{0};
  // A separate bounded delivery admission covers queued authorization and the
  // entire send. Immutable result pins survive scheduler-slot release.
  BoundedResultSender result_sender_;
  const std::size_t result_sender_threads_;
  std::ofstream fetch_timing_output_;
  bool fetch_timing_enabled_{false};
  std::mutex fetch_timing_mutex_;
  std::vector<BoundedResultSender::TimingRecord> fetch_timings_;
  std::atomic<std::uint64_t> fetch_timings_dropped_{0};
  // Opt-in diagnostics only; never included in the registration protocol.
  std::ofstream profile_output_;
  bool profile_enabled_{false};
  std::mutex profile_mutex_;
  std::atomic<std::uint64_t> profile_sequence_{0};
};

// Sends a full declaration to the mount process and returns an absolute FUSE
// path. The server prepares and validates the declaration before registration.
// Window preparation may read every shard, so the response timeout is separate
// from the socket's short request-send timeout.
QueryResult<std::string> submit_query_registration(
  const std::string& socket_path, const std::string& json_request,
  std::chrono::milliseconds response_timeout = std::chrono::minutes(5));
QueryResult<QueryControlResponse> submit_query_control(
  const std::string& socket_path, const std::string& json_request,
  std::chrono::milliseconds response_timeout = std::chrono::minutes(5));

} // namespace iotdbfs
