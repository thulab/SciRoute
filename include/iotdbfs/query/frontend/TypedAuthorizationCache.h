#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

namespace iotdbfs {

inline std::vector<gid_t> normalized_authorization_groups(gid_t primary,
    std::vector<gid_t> groups) {
  groups.erase(std::remove(groups.begin(), groups.end(), primary), groups.end());
  std::sort(groups.begin(), groups.end());
  groups.erase(std::unique(groups.begin(), groups.end()), groups.end());
  return groups;
}

// Used only after the frontend has resolved the immutable route and verified
// its version/snapshot preconditions. Those equivalent spellings must not
// create separate permission entries for the same registration and result.
inline std::string canonical_published_authorization_route(std::string_view route) {
  const auto query = route.find('?');
  if (query == std::string_view::npos) return {};
  const auto path = route.substr(0, query);
  if (path != "/.iotdbfs/query/v1/typed.ipc" && path != "/.iotdbfs/query/v1/windows.ipc")
    return {};
  auto hex = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  auto decode = [&](std::string_view value) {
    std::string decoded;
    for (std::size_t i = 0; i < value.size(); ++i) {
      if (value[i] != '%') decoded.push_back(value[i]);
      else {
        if (i + 2 >= value.size() || hex(value[i + 1]) < 0 || hex(value[i + 2]) < 0)
          return std::string{};
        decoded.push_back(static_cast<char>((hex(value[i + 1]) << 4) | hex(value[i + 2])));
        i += 2;
      }
    }
    return decoded;
  };
  std::string id, result;
  bool seen_precondition = false;
  auto parameters = route.substr(query + 1);
  while (!parameters.empty()) {
    const auto end = parameters.find('&');
    const auto pair = parameters.substr(0, end);
    const auto equal = pair.find('=');
    if (equal == std::string_view::npos || pair.find('=', equal + 1) != std::string_view::npos)
      return {};
    const auto name = decode(pair.substr(0, equal));
    const auto value = decode(pair.substr(equal + 1));
    if (name == "id") {
      if (!id.empty()) return {};
      id = value;
    } else if (name == "result") {
      if (!result.empty()) return {};
      result = value;
    } else if (name == "version" || name == "snapshot") {
      if (seen_precondition || value.empty()) return {};
      seen_precondition = true;
    } else return {};
    if (end == std::string_view::npos) break;
    parameters.remove_prefix(end + 1);
    if (parameters.empty()) return {};
  }
  auto valid = [](const std::string& value, std::size_t length) {
    return value.size() == length && std::all_of(value.begin(), value.end(), [](char c) {
      return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    });
  };
  if (!valid(id, 48) || !valid(result, 64)) return {};
  return std::string(path) + "?id=" + id + "&result=" + result;
}

inline std::string typed_authorization_cache_key(uid_t uid, gid_t gid,
    const std::string& route, const std::vector<gid_t>& groups) {
  const auto canonical = canonical_published_authorization_route(route);
  if (canonical.empty()) return {};
  std::string key = std::to_string(uid) + ":" + std::to_string(gid) + ":" + canonical;
  for (const auto group : normalized_authorization_groups(gid, groups))
    key += ":" + std::to_string(group);
  return key;
}

class TypedAuthorizationCache {
public:
  using Clock = std::chrono::steady_clock;
  struct Observation {
    std::uint64_t generation{0};
    Clock::time_point observed_at{};
  };
  explicit TypedAuthorizationCache(std::size_t max_entries = 1024)
    : max_entries_(max_entries) {}
  std::chrono::milliseconds ttl{1000};

  Observation capture(Clock::time_point now = Clock::now()) {
    std::lock_guard<std::mutex> lock(mu_);
    return {generation_, now};
  }
  std::uint64_t snapshot() { return capture().generation; }
  bool contains(const std::string& key, std::uint64_t expected,
      Clock::time_point now = Clock::now()) {
    std::lock_guard<std::mutex> lock(mu_);
    if (generation_ != expected || ttl.count() <= 0) return false;
    const auto found = allowed_.find(key);
    if (found == allowed_.end()) return false;
    if (now >= found->second) { allowed_.erase(found); return false; }
    return true;
  }
  bool prefill(const std::string& key, const Observation& observed,
      Clock::time_point now = Clock::now()) {
    std::lock_guard<std::mutex> lock(mu_);
    return admit_locked(key, observed, now);
  }
  // A completed check can succeed even when caching is disabled or its
  // observation has expired. A changed generation requires the caller retry.
  bool store(const std::string& key, const Observation& observed,
      Clock::time_point now = Clock::now()) {
    std::lock_guard<std::mutex> lock(mu_);
    if (generation_ != observed.generation) return false;
    admit_locked(key, observed, now);
    return true;
  }
  void invalidate() {
    std::lock_guard<std::mutex> lock(mu_);
    ++generation_;
    allowed_.clear();
  }
  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return allowed_.size();
  }
private:
  bool admit_locked(const std::string& key, const Observation& observed,
      Clock::time_point now) {
    const auto bounded_ttl = std::min(ttl, std::chrono::milliseconds(1000));
    if (generation_ != observed.generation || bounded_ttl.count() <= 0 ||
        !max_entries_ || key.empty() || key.size() > 4096 ||
        now < observed.observed_at || now >= observed.observed_at + bounded_ttl)
      return false;
    if (allowed_.size() >= max_entries_ && allowed_.find(key) == allowed_.end())
      allowed_.clear();
    allowed_[key] = observed.observed_at + bounded_ttl;
    return true;
  }
  const std::size_t max_entries_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, Clock::time_point> allowed_;
  std::uint64_t generation_{0};
};

} // namespace iotdbfs
