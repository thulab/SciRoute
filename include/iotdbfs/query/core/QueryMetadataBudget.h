#pragma once

#include "iotdbfs/query/core/QueryModel.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace iotdbfs {

using QueryMetadataTrace = std::map<std::string,std::uint64_t>;

// A cache publishes one immutable batch. Its residency pin keeps the original
// cache reservation charged even after eviction, while a request still uses it.
class PreparedQueryMetadataTrace final {
public:
  // Map nodes/keys are reserved by the collector; this covers the wrapper and
  // shared ownership bookkeeping without duplicating the moved trace storage.
  static constexpr std::uint64_t management_bytes =
    sizeof(QueryMetadataTrace) + sizeof(std::shared_ptr<const void>) + 128;
  explicit PreparedQueryMetadataTrace(QueryMetadataTrace fees,
      std::shared_ptr<const void> residency_pin = {})
    : residency_pin_(std::move(residency_pin)), fees_(std::move(fees)) {}
  PreparedQueryMetadataTrace(const PreparedQueryMetadataTrace&) = delete;
  PreparedQueryMetadataTrace& operator=(const PreparedQueryMetadataTrace&) = delete;
  const QueryMetadataTrace& fees() const { return fees_; }
private:
  const std::shared_ptr<const void> residency_pin_; // destroyed after the fees
  const QueryMetadataTrace fees_;
};

// Exact stage deltas borrow the cache's existing immutable batches. No fee
// identity is copied into the retained plan. The cache must supply an independent
// RAII residency pin and transactional reserve/release callbacks; every optional
// allocation is reserved first, including the temporary union used during build.
class PreparedQueryMetadataStageSummary final {
public:
  using Batch = std::shared_ptr<const PreparedQueryMetadataTrace>;
  using Buckets = std::array<std::uint64_t,3>;
  static constexpr std::size_t maximum_stages = 8;
  static constexpr std::size_t maximum_union_entries = 65536;
  static constexpr std::uint64_t temporary_node_bytes =
    sizeof(std::pair<const std::string* const,std::uint64_t>) + 64;
  static constexpr std::uint64_t management_bytes =
    sizeof(std::shared_ptr<const void>) +
    maximum_stages * (2 * sizeof(Batch) + sizeof(Buckets) + 3 * sizeof(std::size_t)) + 256;

  static std::shared_ptr<const PreparedQueryMetadataStageSummary> compile(
      const std::vector<Batch>& events,
      const std::function<void(std::uint64_t)>& reserve_memory,
      const std::function<void(std::uint64_t)>& release_memory,
      std::shared_ptr<const void> residency_pin) noexcept {
    return compile_events(events,reserve_memory,release_memory,std::move(residency_pin));
  }
  static std::shared_ptr<const PreparedQueryMetadataStageSummary> compile(
      std::initializer_list<Batch> events,
      const std::function<void(std::uint64_t)>& reserve_memory,
      const std::function<void(std::uint64_t)>& release_memory,
      std::shared_ptr<const void> residency_pin) noexcept {
    return compile_events(events,reserve_memory,release_memory,std::move(residency_pin));
  }
private:
  template<class Events>
  static std::shared_ptr<const PreparedQueryMetadataStageSummary> compile_events(
      const Events& events,
      const std::function<void(std::uint64_t)>& reserve_memory,
      const std::function<void(std::uint64_t)>& release_memory,
      std::shared_ptr<const void> residency_pin) noexcept {
    if(events.size()==0 || events.size()>maximum_stages || !reserve_memory ||
        !release_memory || !residency_pin) return {};
    for(const auto& batch:events) if(!batch) return {};
    struct Less {
      bool operator()(const std::string* a,const std::string* b) const { return *a<*b; }
    };
    std::map<const std::string*,std::uint64_t,Less> united;
    std::shared_ptr<PreparedQueryMetadataStageSummary> plan;
    std::uint64_t reserved=0,temporary=0;
    try {
      reserve_memory(management_bytes);reserved=management_bytes;
      // Retain the function pin if shared_ptr control-block allocation fails.
      plan=std::shared_ptr<PreparedQueryMetadataStageSummary>(
        new PreparedQueryMetadataStageSummary(residency_pin));
      for(const auto& batch:events) {
        auto& edge=plan->edges_[plan->edge_count_++];
        edge.before=plan->prefix_count_;edge.batch=batch;
        bool changed=false;
        plan->build_fee_entries_visited_+=batch->fees().size();
        for(const auto& fee:batch->fees()) {
          const auto found=united.lower_bound(&fee.first);
          const bool present=found!=united.end() && *found->first==fee.first;
          const auto previous=present?found->second:0;
          if(fee.second<=previous) continue;
          const auto k=bucket(fee.first);
          const auto increase=fee.second-previous;
          if(increase>std::numeric_limits<std::uint64_t>::max()-edge.delta[k])
            throw std::overflow_error("metadata stage delta overflow");
          edge.delta[k]+=increase;changed=true;
          if(present) found->second=fee.second;
          else {
            if(united.size()>=maximum_union_entries)
              throw std::length_error("metadata stage union exceeds cache bound");
            reserve_memory(temporary_node_bytes);
            temporary+=temporary_node_bytes;reserved+=temporary_node_bytes;
            united.emplace_hint(found,&fee.first,fee.second);
          }
        }
        if(changed) plan->prefix_batches_[plan->prefix_count_++]=batch;
        edge.after=plan->prefix_count_;
      }
      plan->unique_entries_=united.size();
      united.clear();
      if(temporary) {release_memory(temporary);reserved-=temporary;temporary=0;}
      return plan;
    } catch(...) {
      // Optional cache state is abandoned without changing request accounting.
      united.clear();
      // Keep the reservation owner alive while its charges are released, even
      // when the caller's callbacks borrow rather than own that reservation.
      if(plan) residency_pin=plan->residency_pin_;
      plan.reset();
      if(reserved) {try {release_memory(reserved);} catch(...) {}}
      return {};
    }
  }
public:
  std::size_t stage_count() const { return edge_count_; }
  std::size_t unique_entries() const { return unique_entries_; }
  std::uint64_t build_fee_entries_visited() const { return build_fee_entries_visited_; }
  static std::size_t bucket(const std::string& identity) {
    if(identity.compare(0,18,"v04-payload-bytes/")==0) return 1;
    if(identity.compare(0,12,"v04-extents/")==0) return 2;
    return 0;
  }
private:
  friend class QueryMetadataBudget;
  struct Edge {
    Batch batch;
    Buckets delta{};
    std::size_t before{0},after{0};
  };
  explicit PreparedQueryMetadataStageSummary(std::shared_ptr<const void> pin)
    : residency_pin_(std::move(pin)) {}
  std::uint64_t lookup(std::size_t prefix,const std::string& identity) const {
    std::uint64_t result=0;
    for(std::size_t i=0;i<prefix;++i) {
      const auto& fees=prefix_batches_[i]->fees();
      const auto found=fees.find(identity);
      if(found!=fees.end())result=std::max(result,found->second);
    }
    return result;
  }
  const std::shared_ptr<const void> residency_pin_; // destroyed after batch pins
  std::array<Batch,maximum_stages> prefix_batches_;
  std::array<Edge,maximum_stages> edges_;
  std::size_t prefix_count_{0},edge_count_{0},unique_entries_{0};
  std::uint64_t build_fee_entries_visited_{0};
};

static_assert(sizeof(PreparedQueryMetadataStageSummary)+128 <=
  PreparedQueryMetadataStageSummary::management_bytes,"stage summary reservation is too small");

struct QueryMetadataStageSummaryStats {
  std::uint64_t successful_stages{0},borrowed_fee_entries_skipped{0},full_fee_entries_visited{0};
  std::uint64_t collector_exits{0},fallbacks{0},overlay_entries_inspected{0},memo_hits{0};
};
class ScopedQueryMetadataTrace;
inline thread_local ScopedQueryMetadataTrace* active_query_metadata_trace = nullptr;

class QueryMetadataBudgetExceeded final : public std::runtime_error {
public:
  explicit QueryMetadataBudgetExceeded(const std::string& message = "request metadata byte budget exhausted") : std::runtime_error(message) {}
};

// One external request owns this ledger. Immutable evidence is charged once
// for each source-qualified identity; a different exact version is a distinct
// logical demand even when it shares physical content. Failure is sticky.
class QueryMetadataBudget {
public:
  static constexpr std::uint64_t maximum_bytes = 64ULL * 1024 * 1024;
  static constexpr std::uint64_t maximum_payload_bytes = 1ULL << 30;
  static constexpr std::uint64_t maximum_extents = 65536;
  static constexpr std::size_t maximum_replayed_batches = 256;
  void observe(const std::string& identity, std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (failed_) throw QueryMetadataBudgetExceeded(failure_message_);
    observe_unlocked(identity,bytes);
  }
  void observe_unique(std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (failed_) throw QueryMetadataBudgetExceeded(failure_message_);
    charge_unlocked(bytes);
  }
  QueryStatus status(QueryPhase phase = QueryPhase::Catalog) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return failed_ ? QueryStatus::failure(QueryError::ResourceExhausted, phase,
      failure_message_) : QueryStatus::success();
  }
  std::uint64_t consumed_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_); return consumed_;
  }
  // Re-arming for another principal is allowed only while the already charged
  // immutable prefix is pointer-identical. The ledger and counters never reset.
  bool arm_summary(std::shared_ptr<const PreparedQueryMetadataStageSummary> plan) {
    if(!plan || active_query_metadata_trace) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if(failed_ || !summary_eligible_) return false;
    if(summary_plan_) {
      if(base_prefix_>plan->prefix_count_) return false;
      for(std::size_t i=0;i<base_prefix_;++i)
        if(summary_plan_->prefix_batches_[i].get()!=plan->prefix_batches_[i].get())
          return false;
    }
    summary_plan_=std::move(plan);return true;
  }
  void exit_summary_for_collector() {
    std::lock_guard<std::mutex> lock(mutex_);
    if(summary_plan_ && summary_eligible_) {
      summary_eligible_=false;++summary_stats_.collector_exits;
    }
  }
  QueryMetadataStageSummaryStats stage_summary_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);return summary_stats_;
  }
private:
  friend void replay_query_metadata_trace(const std::shared_ptr<const PreparedQueryMetadataTrace>&);
  // Only the replay dispatcher may enter this path, without active collectors.
  // Otherwise each update must precede observation by every nested collector.
  void replay_prepared(const std::shared_ptr<const PreparedQueryMetadataTrace>& batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (failed_) throw QueryMetadataBudgetExceeded(failure_message_);
    for(std::size_t i=0;i<replayed_count_;++i)
      if(replayed_[i].get()==batch.get()) {++summary_stats_.memo_hits;return;}
    if(summary_plan_ && summary_eligible_) {
      const PreparedQueryMetadataStageSummary::Edge* edge=nullptr;
      if(observations_.size()<=maximum_summary_overlay_entries) {
        for(std::size_t i=0;i<summary_plan_->edge_count_;++i) {
          const auto& candidate=summary_plan_->edges_[i];
          if(candidate.before==base_prefix_ && candidate.batch.get()==batch.get()) {
            edge=&candidate;break;
          }
        }
      }
      if(edge) {
        auto delta=edge->delta;
        for(const auto& item:observations_) {
          ++summary_stats_.overlay_entries_inspected;
          const auto old=summary_plan_->lookup(edge->before,item.first);
          const auto next=summary_plan_->lookup(edge->after,item.first);
          const auto correction=(next-old)-
            (std::max(next,item.second)-std::max(old,item.second));
          delta[PreparedQueryMetadataStageSummary::bucket(item.first)]-=correction;
        }
        if(delta[0]<=maximum_bytes-consumed_ &&
            delta[1]<=maximum_payload_bytes-payload_bytes_ &&
            delta[2]<=maximum_extents-extents_) {
          consumed_+=delta[0];payload_bytes_+=delta[1];extents_+=delta[2];
          base_prefix_=edge->after;
          for(auto it=observations_.begin();it!=observations_.end();) {
            if(it->second<=summary_plan_->lookup(base_prefix_,it->first))
              it=observations_.erase(it);
            else ++it;
          }
          ++summary_stats_.successful_stages;
          summary_stats_.borrowed_fee_entries_skipped+=batch->fees().size();
          memoize_unlocked(batch);return;
        }
        // Preflight must not report a combined-stage failure: replay the exact
        // sorted batch to preserve its successful prefix and first error text.
      } else summary_eligible_=false;
      ++summary_stats_.fallbacks;
    }
    std::uint64_t visited=0;
    try {
      for(const auto& fee:batch->fees()) {
        ++visited;observe_unlocked(fee.first,fee.second);
      }
    } catch(...) {summary_stats_.full_fee_entries_visited+=visited;throw;}
    summary_stats_.full_fee_entries_visited+=visited;
    memoize_unlocked(batch);
  }
  void memoize_unlocked(const std::shared_ptr<const PreparedQueryMetadataTrace>& batch) {
    // Strong pins prevent address reuse. Fixed storage makes this optional
    // optimization allocation-free; full memo tables simply replay as usual.
    if(replayed_count_<maximum_replayed_batches) replayed_[replayed_count_++]=batch;
  }
  void observe_unlocked(const std::string& identity, std::uint64_t bytes) {
    const auto found = observations_.lower_bound(identity);
    const bool present = found != observations_.end() && found->first == identity;
    const auto base=summary_plan_?summary_plan_->lookup(base_prefix_,identity):0;
    const auto previous = std::max(base,present?found->second:0);
    if (bytes <= previous) return;
    // Reserved internal fee domains survive producer trace capture/replay.
    // They are never supplied by a client and never consume metadata bytes.
    if (identity.compare(0,18,"v04-payload-bytes/")==0)
      charge_demand(payload_bytes_,maximum_payload_bytes,bytes-previous,"request payload byte budget exhausted");
    else if (identity.compare(0,12,"v04-extents/")==0)
      charge_demand(extents_,maximum_extents,bytes-previous,"request extent budget exhausted");
    else charge_unlocked(bytes - previous);
    if(present) found->second=bytes;
    else observations_.emplace_hint(found,identity,bytes);
  }
  void charge_demand(std::uint64_t& used,std::uint64_t maximum,std::uint64_t amount,const char* message) {
    if(amount>maximum-used){failed_=true;failure_message_=message;throw QueryMetadataBudgetExceeded(failure_message_);}
    used+=amount;
  }
  void charge_unlocked(std::uint64_t bytes) {
    if (bytes > maximum_bytes - consumed_) { failed_ = true; throw QueryMetadataBudgetExceeded(); }
    consumed_ += bytes;
  }
  static constexpr std::size_t maximum_summary_overlay_entries = 128;
  mutable std::mutex mutex_;
  std::shared_ptr<const PreparedQueryMetadataStageSummary> summary_plan_;
  std::size_t base_prefix_{0};
  bool summary_eligible_{true};
  QueryMetadataStageSummaryStats summary_stats_;
  std::map<std::string, std::uint64_t> observations_;
  std::array<std::shared_ptr<const PreparedQueryMetadataTrace>,maximum_replayed_batches> replayed_;
  std::size_t replayed_count_{0};
  std::uint64_t consumed_{0};
  std::uint64_t payload_bytes_{0},extents_{0};
  std::string failure_message_{"request metadata byte budget exhausted"};
  bool failed_{false};
};

inline thread_local std::shared_ptr<QueryMetadataBudget> active_query_metadata_budget;
inline std::shared_ptr<QueryMetadataBudget> current_query_metadata_budget() {
  return active_query_metadata_budget;
}
class ScopedQueryMetadataBudget {
public:
  explicit ScopedQueryMetadataBudget(std::shared_ptr<QueryMetadataBudget> budget = {})
    : previous_(active_query_metadata_budget), budget_(budget ? std::move(budget) :
        (previous_ ? previous_ : std::make_shared<QueryMetadataBudget>())) {
    active_query_metadata_budget = budget_;
    if(active_query_metadata_trace) budget_->exit_summary_for_collector();
  }
  ~ScopedQueryMetadataBudget() { active_query_metadata_budget = previous_; }
  ScopedQueryMetadataBudget(const ScopedQueryMetadataBudget&) = delete;
  ScopedQueryMetadataBudget& operator=(const ScopedQueryMetadataBudget&) = delete;
  const std::shared_ptr<QueryMetadataBudget>& budget() const { return budget_; }
  QueryStatus status(QueryPhase phase = QueryPhase::Catalog) const { return budget_->status(phase); }
private:
  std::shared_ptr<QueryMetadataBudget> previous_, budget_;
};
// Shared producers collect fees without borrowing any waiter's ledger.
class ScopedQueryMetadataProducer {
public:
  ScopedQueryMetadataProducer() : previous_(std::move(active_query_metadata_budget)) {}
  ~ScopedQueryMetadataProducer() { active_query_metadata_budget=std::move(previous_); }
  ScopedQueryMetadataProducer(const ScopedQueryMetadataProducer&)=delete;
  ScopedQueryMetadataProducer& operator=(const ScopedQueryMetadataProducer&)=delete;
private:
  std::shared_ptr<QueryMetadataBudget> previous_;
};
// A cache stores only logical fee identities, never a request ledger. This
// collector is thread-local and prefix-filtered so parsed directory snapshots
// can replay their cold scan's control-object fees on later cache hits.
// Optional cache bookkeeping must never turn an otherwise valid request into
// a failure. CacheStable also refuses operation-specific payload/unique fees:
// those cannot be reused as a stable immutable-metadata proof.
enum class QueryMetadataTracePolicy { Required, CacheStable };
class ScopedQueryMetadataTrace {
public:
  explicit ScopedQueryMetadataTrace(std::string prefix,
      std::function<void(std::uint64_t)> reserve_memory = {},
      QueryMetadataTracePolicy policy = QueryMetadataTracePolicy::Required)
    : previous_(active_query_metadata_trace), prefix_(std::move(prefix)),
      reserve_memory_(std::move(reserve_memory)), policy_(policy) {
    if(active_query_metadata_budget) active_query_metadata_budget->exit_summary_for_collector();
    active_query_metadata_trace=this;
  }
  ~ScopedQueryMetadataTrace() { active_query_metadata_trace=previous_; }
  ScopedQueryMetadataTrace(const ScopedQueryMetadataTrace&)=delete;
  ScopedQueryMetadataTrace& operator=(const ScopedQueryMetadataTrace&)=delete;
  QueryMetadataTrace release() { return std::move(trace_); }
  std::uint64_t estimated_bytes() const { return estimated_bytes_; }
  bool failed() const { return failed_; }
  void observe(const std::string& identity,std::uint64_t bytes) {
    if(failed_) {
      if(policy_==QueryMetadataTracePolicy::CacheStable) return;
      throw QueryMetadataBudgetExceeded();
    }
    if(identity.compare(0,prefix_.size(),prefix_)!=0) return;
    if(policy_==QueryMetadataTracePolicy::CacheStable) {
      if(identity.compare(0,14,"unique-demand/")==0 ||
          identity.compare(0,18,"v04-payload-bytes/")==0 ||
          identity.compare(0,12,"v04-extents/")==0) {
        abandon_cache_trace();return;
      }
      try { observe_stable(identity,bytes); }
      catch(...) { abandon_cache_trace(); }
      return;
    }
    observe_stable(identity,bytes);
  }
  ScopedQueryMetadataTrace* previous() const { return previous_; }
private:
  void abandon_cache_trace() noexcept {
    failed_=true;trace_.clear();
    // Keep the conservative reservation total until the caller releases its
    // RAII cache reservation. It is not a charge to a request ledger.
  }
  void observe_stable(const std::string& identity,std::uint64_t bytes) {
    const auto found=trace_.find(identity);
    if(found!=trace_.end()) {found->second=std::max(found->second,bytes);return;}
    constexpr auto overhead=sizeof(QueryMetadataTrace::value_type)+64;
    const auto maximum=QueryMetadataBudget::maximum_bytes;
    const bool oversized=identity.size()>(maximum-overhead)/2;
    const auto added=oversized?maximum+1:2*identity.size()+overhead;
    if(trace_.size()>=65536 || added>QueryMetadataBudget::maximum_bytes-estimated_bytes_) {
      failed_=true;
      if(policy_==QueryMetadataTracePolicy::Required && active_query_metadata_budget)
        active_query_metadata_budget->observe_unique(QueryMetadataBudget::maximum_bytes+1);
      throw QueryMetadataBudgetExceeded();
    }
    try {
      if(reserve_memory_) reserve_memory_(added);
      estimated_bytes_+=added;trace_.emplace(identity,bytes);
    } catch(...) {failed_=true;throw;}
  }
  ScopedQueryMetadataTrace* previous_;
  std::string prefix_;
  QueryMetadataTrace trace_;
  std::uint64_t estimated_bytes_{0};
  bool failed_{false};
  std::function<void(std::uint64_t)> reserve_memory_;
  QueryMetadataTracePolicy policy_;
};
inline bool arm_query_metadata_stage_summary(
    const std::shared_ptr<const PreparedQueryMetadataStageSummary>& plan) {
  return active_query_metadata_budget && active_query_metadata_budget->arm_summary(plan);
}
inline void require_query_metadata(const std::string& identity, std::uint64_t bytes) {
  if (active_query_metadata_budget) active_query_metadata_budget->observe(identity, bytes);
  for(auto* trace=active_query_metadata_trace;trace;trace=trace->previous()) trace->observe(identity,bytes);
}
inline void replay_query_metadata_trace(const QueryMetadataTrace& trace) {
  for(const auto& fee:trace) require_query_metadata(fee.first,fee.second);
}
inline void replay_query_metadata_trace(const std::shared_ptr<const PreparedQueryMetadataTrace>& batch) {
  if(!batch) throw std::invalid_argument("missing prepared metadata trace");
  if(active_query_metadata_trace) {
    if(active_query_metadata_budget) active_query_metadata_budget->exit_summary_for_collector();
    // Do not memoize this path: a Required collector may fail partway through,
    // and a newly installed collector must see even an already charged batch.
    if(batch->fees().empty() && active_query_metadata_budget)
      active_query_metadata_budget->observe_unique(0);
    replay_query_metadata_trace(batch->fees());
  } else if(active_query_metadata_budget) active_query_metadata_budget->replay_prepared(batch);
}
inline void require_query_payload_demand(std::uint64_t bytes,std::uint64_t extents) {
  static std::atomic<std::uint64_t> sequence{0};
  auto id=sequence.load(std::memory_order_relaxed);
  do {if(id==std::numeric_limits<std::uint64_t>::max())throw QueryMetadataBudgetExceeded();}
  while(!sequence.compare_exchange_weak(id,id+1,std::memory_order_relaxed));
  const auto suffix=std::to_string(id);
  require_query_metadata("v04-payload-bytes/"+suffix,bytes);
  require_query_metadata("v04-extents/"+suffix,extents);
}
inline void require_query_metadata_unique(std::uint64_t bytes) {
  if(active_query_metadata_trace) {
    // A producer's explicit demand remains one stable fee across all joins of
    // that cached result. This sequence retains no process-wide identity map.
    static std::atomic<std::uint64_t> sequence{0};
    auto id=sequence.load(std::memory_order_relaxed);
    do {
      if(id==std::numeric_limits<std::uint64_t>::max()) throw QueryMetadataBudgetExceeded();
    } while(!sequence.compare_exchange_weak(id,id+1,std::memory_order_relaxed));
    require_query_metadata("unique-demand/"+std::to_string(id),bytes);
  } else if (active_query_metadata_budget) active_query_metadata_budget->observe_unique(bytes);
}
} // namespace iotdbfs
