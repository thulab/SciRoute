#include "iotdbfs/storage/versioning/NamespaceSelection.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/storage/NativeFileStore.h"

#include <algorithm>
#include <set>
#include <utility>

namespace iotdbfs::versioning {
namespace {

bool in_subtree(const std::string& path, const std::string& root) {
  return root == "/" || path == root ||
    (path.size() > root.size() && path.compare(0, root.size(), root) == 0 &&
     path[root.size()] == '/');
}

bool canonical_scope(const std::string& path) {
  // SourceSelector permits lexical scopes which cannot match any canonical
  // namespace path. They remain empty selections, rather than new errors.
  try { (void)exact_key(path, 0); return true; }
  catch (const Error&) { return false; }
}

std::vector<PathScope> disjoint_scopes(const SourceSelector& selector) {
  std::vector<PathScope> normalized;
  std::set<std::string> subtrees;
  for (const auto& input : selector.paths.normalized_paths()) {
    const auto& path = input.path;
    if (!canonical_scope(path)) continue;
    normalized.push_back({input.kind, path});
    if (input.kind == PathScopeKind::Subtree) subtrees.insert(path);
  }
  std::vector<PathScope> scopes;
  for (const auto& input : normalized) {
    const auto& path = input.path;
    bool covered = false;
    // A canonical path's covering subtree can only be itself or one of its
    // component ancestors. Do not compare every pair of client OR scopes.
    auto ancestor = input.kind == PathScopeKind::Exact ? path : path_utils::parent_of(path);
    for (;;) {
      if (!(input.kind == PathScopeKind::Subtree && ancestor == path) &&
          subtrees.count(ancestor)) {
        covered = true;
        break;
      }
      if (ancestor == "/") break;
      ancestor = path_utils::parent_of(ancestor);
    }
    if (!covered) scopes.push_back({input.kind, path});
  }
  std::sort(scopes.begin(), scopes.end(), [](const auto& a, const auto& b) {
    if (a.path != b.path) return a.path < b.path;
    return a.kind < b.kind;
  });
  return scopes;
}

bool path_matches(const std::string& path, const SourceSelector& selector) {
  return selector.paths.matches_normalized_path(path);
}

bool format_matches(const NamespaceRecord& record, const FormatSelector& formats) {
  if (!record.manifest.format_id) return formats.include_byte_passthrough;
  return formats.adapters == AdapterSelectionKind::All ||
    std::find(formats.adapter_ids.begin(), formats.adapter_ids.end(),
              *record.manifest.format_id) != formats.adapter_ids.end();
}

bool version_matches(std::int64_t version, const VersionSelection& selection) {
  switch (selection.kind) {
  case VersionSelectionKind::Latest:
  case VersionSelectionKind::All: return true;
  case VersionSelectionKind::Exact: return version == *selection.exact;
  case VersionSelectionKind::Between:
    return version >= *selection.lower_inclusive && version < *selection.upper_exclusive;
  }
  return false;
}

class SelectionReader {
public:
  SelectionReader(Owner& owner, const View& view, const SourceSelector& selector,
      const std::function<void(Table, const std::string&, const Bytes&)>& observe,
      const std::map<StructuredBlobVersionKey, NamespaceRecord>* parsed_exact = nullptr)
    : owner_(owner), view_(view), selector_(selector), observe_(observe), parsed_exact_(parsed_exact) {}

  NamespaceSelection run() {
    // Empty-format and zero-source requests cannot hide an exhausted ledger.
    const auto budget = current_query_metadata_budget();
    if (budget && !budget->status().ok()) throw QueryMetadataBudgetExceeded();
    const auto valid = validate_source_selector(selector_);
    if (!valid.ok()) throw Error(ErrorCode::Invalid, valid.message());
    const bool no_formats = selector_.formats.adapters == AdapterSelectionKind::Listed &&
      selector_.formats.adapter_ids.empty() && !selector_.formats.include_byte_passthrough;
    if (!no_formats) {
      if (parsed_exact_) {
        // Exact keys are ordered by path then ascending version. Visit newest
        // first so Latest copies only the final eligible identity per path.
        // Evidence and control fees for ALL records were replayed by caller.
        for (auto it = parsed_exact_->rbegin(); it != parsed_exact_->rend(); ++it) {
          if (!(it->first == it->second.manifest.key))
            throw Error(ErrorCode::Corrupt, "parsed exact identity differs from its key");
          if (selector_.versions.kind == VersionSelectionKind::Latest &&
              latest_.count(it->first.path)) continue;
          consider(it->second);
        }
      } else if (selector_.paths.empty()) {
        scan(selector_.versions.kind == VersionSelectionKind::Latest ? Table::Current : Table::Exact, "");
      } else {
        const auto scopes = disjoint_scopes(selector_);
        for (const auto& scope : scopes) {
          if (selector_.versions.kind == VersionSelectionKind::Latest) {
            latest_path(scope.path);
            if (scope.kind == PathScopeKind::Subtree)
              scan(Table::Current, scope.path == "/" ? "/" : scope.path + "/");
          } else if (scope.kind == PathScopeKind::Exact &&
                     selector_.versions.kind == VersionSelectionKind::Exact) {
            const auto key = exact_key(scope.path, *selector_.versions.exact);
            if (const auto* record = get(Table::Exact, key)) consider(*record);
          } else {
            scan(Table::Exact, scope.path + std::string(1, '\0'));
            if (scope.kind == PathScopeKind::Subtree)
              scan(Table::Exact, scope.path == "/" ? "/" : scope.path + "/");
          }
        }
      }
    }
    // Current is a lookup index, not a substitute for an observed exact
    // binding. Freeze the final candidates only after checking membership and
    // the complete canonical record; never fall back to another version.
    if (!parsed_exact_ && selector_.versions.kind == VersionSelectionKind::Latest) {
      for (auto& source : result_.sources)
        source.second = *checked_current_binding(source.second);
    }
    // Ancestors are needed by ReadContext even when a caller does not request
    // permission hints. Missing/non-directory parents remain explicit facts.
    for (const auto& source : result_.sources) {
      for (auto path = path_utils::parent_of(source.first.path);;
           path = path_utils::parent_of(path)) {
        if (!result_.parents.count(path)) {
          const auto* record = get(Table::Current, path);
          if (record) record = checked_current_binding(*record);
          result_.parents.emplace(path, record ? std::optional<NamespaceRecord>(*record) : std::nullopt);
        }
        if (path == "/") break;
      }
    }
    return std::move(result_);
  }

private:
  using ReadKey = std::pair<Table, std::string>;

  const NamespaceRecord* decode(Table table, const std::string& key, const Bytes& bytes) {
    if (table == Table::Current && key == content_profile_key()) return nullptr;
    if (table == Table::Current && is_native_file_location_key(key)) {
      const auto location = decode_native_file_location(bytes);
      if (key != native_file_location_key(location.key) || !(location.namespace_identity == view_.head.identity))
        throw Error(ErrorCode::Corrupt, "native file locator differs from namespace identity/key");
      return nullptr;
    }
    // Callers may overlap an exact get with a subtree scan at '/'. The actual
    // record read is still observed; parsed identity is retained only once.
    if (observe_) observe_(table, key, bytes);
    const auto existing = read_.find({table, key});
    if (existing != read_.end()) {
      if (!existing->second) throw Error(ErrorCode::Corrupt, "immutable namespace key changed inside one view");
      return &*existing->second;
    }
    auto record = decode_namespace_record(bytes);
    if (table == Table::Exact) {
      if (key != exact_key(record.inode.path, record.inode.row_time))
        throw Error(ErrorCode::Corrupt, "namespace exact index differs from record");
    } else if (table == Table::Current) {
      if (key != record.inode.path &&
          key != record.inode.path + std::string(1, '\0') + record.inode.format_id)
        throw Error(ErrorCode::Corrupt, "namespace current index differs from record");
    } else {
      throw Error(ErrorCode::Invalid, "namespace selection used an unsupported table");
    }
    if (!record.inode.is_dir) {
      record.inode.metadata = "{}";
      record.manifest.metadata_materialized = false;
    }
    const auto inserted = read_.emplace(ReadKey{table, key}, std::move(record));
    return &*inserted.first->second;
  }

  const NamespaceRecord* get(Table table, const std::string& key) {
    const auto found = read_.find({table, key});
    if (found != read_.end()) return found->second ? &*found->second : nullptr;
    const auto bytes = owner_.get(view_, table, key);
    if (!bytes) {
      read_.emplace(ReadKey{table, key}, std::nullopt);
      return nullptr;
    }
    return decode(table, key, *bytes);
  }

  const NamespaceRecord* checked_current_binding(const NamespaceRecord& current) {
    const auto* exact = get(Table::Exact,
      exact_key(current.inode.path, current.inode.row_time));
    if (!exact || encode_namespace_record(current) != encode_namespace_record(*exact))
      throw Error(ErrorCode::Corrupt, "namespace current index differs from exact binding");
    return exact;
  }

  void consider(const NamespaceRecord& record) {
    if (record.inode.is_dir || !path_matches(record.inode.path, selector_) ||
        !format_matches(record, selector_.formats) ||
        !version_matches(record.inode.row_time, selector_.versions)) return;
    if (selector_.versions.kind == VersionSelectionKind::Latest) {
      const auto found = latest_.find(record.inode.path);
      if (found != latest_.end()) {
        if (found->second.version >= record.inode.row_time) return;
        result_.sources.erase(found->second);
      }
      latest_[record.inode.path] = record.manifest.key;
    }
    result_.sources.emplace(record.manifest.key, record);
  }

  void consider_current(const std::string& key, const NamespaceRecord& record) {
    const bool plain = key == record.inode.path;
    const bool use_plain = selector_.formats.adapters == AdapterSelectionKind::All &&
      selector_.formats.include_byte_passthrough;
    if (plain == use_plain) consider(record);
  }

  bool eligible_key(Table table, const std::string& key) const {
    if (table == Table::Exact) {
      const auto binding = decode_exact_key(key);
      return path_matches(binding.first, selector_) &&
        version_matches(binding.second, selector_.versions);
    }
    if (table != Table::Current)
      throw Error(ErrorCode::Invalid, "namespace selection used an unsupported table");
    if (key == content_profile_key()) return false;
    // Read and validate the reserved typed extension before excluding it from
    // namespace candidates. Never accept arbitrary non-path Current entries.
    if (is_native_file_location_key(key)) return true;
    const auto separator = key.find('\0');
    const auto path = key.substr(0, separator);
    if (!canonical_scope(path))
      throw Error(ErrorCode::Corrupt, "namespace current index path is not canonical");
    if (!path_matches(path, selector_)) return false;
    const auto& formats = selector_.formats;
    const bool use_plain = formats.adapters == AdapterSelectionKind::All &&
      formats.include_byte_passthrough;
    if (use_plain) return separator == std::string::npos;
    if (separator == std::string::npos) return false;
    const auto format = key.substr(separator + 1);
    if (format.find('\0') != std::string::npos)
      throw Error(ErrorCode::Corrupt, "namespace current index format contains NUL");
    if (format.empty()) return formats.include_byte_passthrough;
    return formats.adapters == AdapterSelectionKind::All ||
      std::find(formats.adapter_ids.begin(), formats.adapter_ids.end(), format) !=
        formats.adapter_ids.end();
  }

  void scan(Table table, const std::string& prefix) {
    for (const auto& item : owner_.scan(view_, table, prefix, kScanEntries,
        [this, table](const std::string& key) { return eligible_key(table, key); })) {
      if (const auto* record = decode(table, item.first, item.second)) {
        if (table == Table::Current) consider_current(item.first, *record);
        else consider(*record);
      }
    }
  }

  void latest_path(const std::string& path) {
    const auto& formats = selector_.formats;
    if (formats.adapters == AdapterSelectionKind::All && formats.include_byte_passthrough) {
      if (const auto* record = get(Table::Current, path)) consider(*record);
    } else if (formats.adapters == AdapterSelectionKind::Listed) {
      for (const auto& format : formats.adapter_ids) {
        if (const auto* record = get(Table::Current, path + std::string(1, '\0') + format))
          consider(*record);
      }
      if (formats.include_byte_passthrough) {
        if (const auto* record = get(Table::Current, path + std::string(1, '\0')))
          consider(*record);
      }
    } else {
      scan(Table::Current, path + std::string(1, '\0'));
    }
  }

  Owner& owner_;
  const View& view_;
  const SourceSelector& selector_;
  const std::function<void(Table, const std::string&, const Bytes&)>& observe_;
  const std::map<StructuredBlobVersionKey, NamespaceRecord>* parsed_exact_;
  std::map<ReadKey, std::optional<NamespaceRecord>> read_;
  std::map<std::string, StructuredBlobVersionKey> latest_;
  NamespaceSelection result_;
};

} // namespace

NamespaceSelection select_namespace(Owner& owner, const View& view,
    const SourceSelector& selector,
    const std::function<void(Table, const std::string&, const Bytes&)>& observe) {
  return SelectionReader(owner, view, selector, observe).run();
}

NamespaceSelection project_namespace(Owner& owner, const View& view,
    const SourceSelector& selector,
    const std::map<StructuredBlobVersionKey, NamespaceRecord>& parsed_exact,
    const std::function<void(Table, const std::string&, const Bytes&)>& observe) {
  return SelectionReader(owner, view, selector, observe, &parsed_exact).run();
}

void validate_current_index(Owner& owner, const View& view,
    const std::vector<std::pair<std::string, Bytes>>& exact_records) {
  struct Expected { std::int64_t version; const Bytes* bytes; };
  std::map<std::string, Expected> expected;
  const auto add = [&](const std::string& key, std::int64_t version, const Bytes& bytes) {
    const auto found = expected.find(key);
    if (found == expected.end()) {
      expected.emplace(key, Expected{version, &bytes});
    } else if (found->second.version < version) {
      found->second = Expected{version, &bytes};
    } else if (found->second.version == version && *found->second.bytes != bytes) {
      throw Error(ErrorCode::Corrupt, "exact scan contains conflicting namespace identities");
    }
  };
  for (const auto& item : exact_records) {
    const auto record = decode_namespace_record(item.second);
    if (item.first != exact_key(record.inode.path, record.inode.row_time))
      throw Error(ErrorCode::Corrupt, "namespace exact index differs from record");
    add(record.inode.path, record.inode.row_time, item.second);
    add(record.inode.path + std::string(1, '\0') + record.inode.format_id,
        record.inode.row_time, item.second);
  }
  for (const auto& item : owner.scan(view, Table::Current)) {
    // configure_version_directory has already checked the profile's exact
    // bytes in this view. Native-file records are a separately typed extension.
    if (item.first == content_profile_key()) continue;
    if (is_native_file_location_key(item.first)) {
      const auto location = decode_native_file_location(item.second);
      if (item.first != native_file_location_key(location.key) || !(location.namespace_identity == view.head.identity))
        throw Error(ErrorCode::Corrupt, "native file locator differs from namespace identity/key");
      continue;
    }
    const auto found = expected.find(item.first);
    if (found == expected.end() || *found->second.bytes != item.second)
      throw Error(ErrorCode::Corrupt, "namespace current index differs from latest exact binding");
    expected.erase(found);
  }
  if (!expected.empty())
    throw Error(ErrorCode::Corrupt, "namespace current index omits an exact binding");
}

} // namespace iotdbfs::versioning
