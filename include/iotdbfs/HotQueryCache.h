#pragma once

#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/query/core/QueryDomainCache.h"
#include <algorithm>
#include <array>
#include <condition_variable>
#include <limits>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

struct HotQueryCacheStats {
  std::uint64_t resident_bytes{0}, domains{0}, proofs{0}, handles{0}, handle_bytes{0};
  std::uint64_t domain_hits{0}, domain_builds{0}, domain_waits{0};
  std::uint64_t proof_hits{0}, proof_builds{0}, opt_outs{0}, evictions{0};
  // Disjoint charge classes; auxiliary_bytes includes handle_bytes. Gauges
  // include live retired pins and are neither per-query deltas nor RSS.
  std::uint64_t domain_bytes{0}, proof_bytes{0}, auxiliary_bytes{0};
  std::uint64_t resident_domains{0}, inflight_domains{0}, retired_domains{0};
  std::uint64_t resident_domain_bytes{0}, inflight_domain_bytes{0}, retired_domain_bytes{0};
  std::uint64_t pressure_requests{0}, slot_evictions{0}, pressure_domain_evictions{0};
  std::uint64_t admission_bypasses{0}, retention_slots{0}, retained_values{0}, refunded_bytes{0};
  std::uint64_t retention_registry_bytes{0};
};

// Only the new hot-cache pool: existing Owner/Content caches have their own
// unchanged bounds. Reservations survive eviction and account retired pins.
class HotQueryCache final : public std::enable_shared_from_this<HotQueryCache> {
public:
  class RetainedSlot;
private:
  struct State {
    std::mutex mutex;
    HotQueryCacheStats stats;
    std::weak_ptr<HotQueryCache> owner;
    std::vector<std::weak_ptr<RetainedSlot>> slots;
  };
public:
  static constexpr std::uint64_t maximum_bytes = 64ULL*1024*1024;
  static constexpr std::uint64_t maximum_domains = 16, maximum_proofs = 64, maximum_handles = 64;
  static constexpr std::uint64_t maximum_handle_bytes = 8ULL*1024*1024;
  enum class Kind { Domain, Proof, Handle, Auxiliary };
  enum class PayloadKind { Generic, Hdf5Dataset, CandidateDomain };
  enum class Retention { Derived, ExactView, Proof, ParentProof };
  struct Reference {
    std::size_t slot{0};
    std::uint64_t generation{0};
    versioning::Head head;
    PayloadKind payload_kind{PayloadKind::Generic};
    std::weak_ptr<const void> cache_identity;
  };
  // A binding retains no candidate domain. Residency must be checked atomically
  // against its originating cache before acquiring an execution pin.
  class Reservation;
  struct Binding {
    std::shared_ptr<Reservation> reservation;
    std::mutex mutex;
    std::optional<Reference> reference;
    std::weak_ptr<HotQueryCache> owner;
    std::optional<versioning::NamespaceIdentity> identity;
    std::optional<PayloadKind> template_kind;
    std::shared_ptr<const void> selector_template;
  };
  class Reservation final {
  public:
    ~Reservation() {
      if(!active_)return;
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->stats.resident_bytes-=bytes_;
      state_->stats.refunded_bytes+=bytes_;
      category_bytes()-=bytes_;
      if(kind_==Kind::Domain)--state_->stats.domains;
      else if(kind_==Kind::Proof)--state_->stats.proofs;
      else if(kind_==Kind::Handle){--state_->stats.handles;state_->stats.handle_bytes-=bytes_;}
    }
    bool grow(std::uint64_t bytes) noexcept {
      for(std::size_t attempt=0;attempt<=maximum_retention_slots+maximum_domains;++attempt) {
        {
          std::lock_guard<std::mutex> lock(state_->mutex);
          if(bytes<=maximum_bytes-state_->stats.resident_bytes &&
             (kind_!=Kind::Handle||bytes<=maximum_handle_bytes-state_->stats.handle_bytes)) {
            if(kind_==Kind::Handle)state_->stats.handle_bytes+=bytes;
            state_->stats.resident_bytes+=bytes;category_bytes()+=bytes;bytes_+=bytes;return true;
          }
        }
        if(bytes>maximum_bytes-4096||!relieve(state_,kind_,this,false))return false;
      }
      return false;
    }
    void shrink(std::uint64_t bytes) noexcept {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if(bytes>bytes_)return;
      state_->stats.resident_bytes-=bytes;bytes_-=bytes;
      state_->stats.refunded_bytes+=bytes;
      category_bytes()-=bytes;
      if(kind_==Kind::Handle)state_->stats.handle_bytes-=bytes;
    }
    std::uint64_t available() const noexcept {
      std::lock_guard<std::mutex> lock(state_->mutex);
      const auto total=maximum_bytes-state_->stats.resident_bytes;
      return kind_==Kind::Handle?std::min(total,maximum_handle_bytes-state_->stats.handle_bytes):total;
    }
  private:
    friend class HotQueryCache;
    Reservation(std::shared_ptr<State> state,Kind kind):state_(std::move(state)),kind_(kind){}
    std::shared_ptr<State> state_;
    Kind kind_;
    std::uint64_t bytes_{0};
    bool active_{false};
    std::uint64_t& category_bytes() noexcept {
      return kind_==Kind::Domain?state_->stats.domain_bytes:
        kind_==Kind::Proof?state_->stats.proof_bytes:state_->stats.auxiliary_bytes;
    }
  };
  // The slot owns only an optional cache reference. acquire() supplies an
  // independent execution pin; clear() never invalidates or refunds that pin.
  // No storage/context callback runs under the pool or slot mutex.
  class RetainedSlot final : public QueryDomainRetention {
  public:
    ~RetainedSlot() override {
      release(false,true);
      if(registered_) {
        std::lock_guard<std::mutex> lock(state_->mutex);
        --state_->stats.retention_slots;
      }
    }
    std::shared_ptr<const void> acquire() noexcept override {
      std::lock_guard<std::mutex> lock(mutex_);
      if(value_)recent_=true;
      return value_;
    }
    bool present() const noexcept {
      std::lock_guard<std::mutex> lock(mutex_);return bool(value_);
    }
    void clear() noexcept {release(true,true);}
  private:
    friend class HotQueryCache;
    RetainedSlot(std::shared_ptr<State> state,std::shared_ptr<Reservation> management,
        std::shared_ptr<const void> value,Retention priority)
      :state_(std::move(state)),management_(std::move(management)),value_(std::move(value)),priority_(priority){}
    bool release(bool eviction,bool force) noexcept {
      std::shared_ptr<const void> retired;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!value_)return false;
        if(!force&&recent_){recent_=false;return false;}
        retired=std::move(value_);
      }
      if(registered_) {
        std::lock_guard<std::mutex> lock(state_->mutex);
        --state_->stats.retained_values;
        if(eviction)++state_->stats.slot_evictions;
      }
      // Destructors may refund an Allocation or reenter cache statistics.
      retired.reset();return true;
    }
    std::shared_ptr<State> state_;
    // Independent Auxiliary charge keeps empty-slot/key metadata accounted.
    std::shared_ptr<Reservation> management_;
    mutable std::mutex mutex_;
    std::shared_ptr<const void> value_;
    Retention priority_;
    bool recent_{false},registered_{false};
  };
  struct Ticket {
    ~Ticket() {if(auto cache=owner.lock())cache->finish(*this);}
    std::shared_ptr<Reservation> reservation;
    std::weak_ptr<HotQueryCache> owner;
    std::string key;
    versioning::Head head;
    PayloadKind payload_kind{PayloadKind::Generic};
  };
  struct Claim {
    std::shared_ptr<const void> value;
    std::shared_ptr<Ticket> ticket;
    std::optional<Reference> reference;
  };

  HotQueryCache():state_(std::make_shared<State>()){state_->stats.resident_bytes=4096;}
  HotQueryCacheStats stats() const {
    // Same lock order as claim/reserve; snapshot gauges, not query deltas.
    std::lock_guard<std::mutex> cache_lock(mutex_);
    std::lock_guard<std::mutex> lock(state_->mutex);
    auto result=state_->stats;
    result.resident_domains=entries_.size();
    for(const auto& item:entries_)result.resident_domain_bytes+=item.second.reservation->bytes_;
    for(const auto& item:flights_) {
      if(entries_.find(item.first)!=entries_.end())continue;
      ++result.inflight_domains;
      result.inflight_domain_bytes+=item.second->reservation->bytes_;
    }
    result.retired_domains=result.domains-result.resident_domains-result.inflight_domains;
    result.retired_domain_bytes=result.domain_bytes-result.resident_domain_bytes-result.inflight_domain_bytes;
    return result;
  }
  void count(std::uint64_t HotQueryCacheStats::*counter) const {
    std::lock_guard<std::mutex> lock(state_->mutex);++(state_->stats.*counter);
  }
  std::shared_ptr<Reservation> reserve(Kind kind,std::uint64_t bytes) noexcept {
    try {
      auto result=std::shared_ptr<Reservation>(new Reservation(state_,kind));
      for(std::size_t attempt=0;attempt<=maximum_retention_slots+maximum_domains;++attempt) {
        bool count_pressure=false;
        {
          std::lock_guard<std::mutex> lock(state_->mutex);
          state_->owner=weak_from_this();
          auto* count=kind==Kind::Domain?&state_->stats.domains:
            kind==Kind::Proof?&state_->stats.proofs:kind==Kind::Handle?&state_->stats.handles:nullptr;
          const auto limit=kind==Kind::Domain?maximum_domains:kind==Kind::Proof?maximum_proofs:maximum_handles;
          count_pressure=count&&*count>=limit;
          if(!count_pressure&&bytes<=maximum_bytes-state_->stats.resident_bytes &&
             (kind!=Kind::Handle||bytes<=maximum_handle_bytes-state_->stats.handle_bytes)) {
            if(count)++*count;
            if(kind==Kind::Handle)state_->stats.handle_bytes+=bytes;
            state_->stats.resident_bytes+=bytes;result->bytes_=bytes;result->active_=true;
            result->category_bytes()+=bytes;return result;
          }
        }
        if(bytes>maximum_bytes-4096||!relieve(state_,kind,nullptr,count_pressure))return {};
      }
      return {};
    } catch(...) {return {};}
  }
  std::shared_ptr<RetainedSlot> retain(std::shared_ptr<const void> value,Retention priority,
      std::uint64_t management_bytes=0) noexcept {
    try {
      if(!value||management_bytes>maximum_bytes-512)return {};
      auto management=reserve(Kind::Auxiliary,512+management_bytes);
      if(!management)return {};
      auto slot=std::shared_ptr<RetainedSlot>(new RetainedSlot(state_,std::move(management),std::move(value),priority));
      {
        std::lock_guard<std::mutex> lock(state_->mutex);
        auto& slots=state_->slots;
        slots.erase(std::remove_if(slots.begin(),slots.end(),[](const auto& item){return item.expired();}),slots.end());
        if(slots.size()>=maximum_retention_slots)return {};
        if(slots.size()==slots.capacity()) {
          const auto capacity=std::min(maximum_retention_slots,slots.capacity()+64);
          const auto bytes=(capacity-slots.capacity())*sizeof(std::weak_ptr<RetainedSlot>);
          if(bytes>maximum_bytes-state_->stats.resident_bytes)return {};
          slots.reserve(capacity);
          state_->stats.retention_registry_bytes+=bytes;
          state_->stats.auxiliary_bytes+=bytes;state_->stats.resident_bytes+=bytes;
        }
        slots.push_back(slot);slot->registered_=true;
        ++state_->stats.retention_slots;++state_->stats.retained_values;
      }
      return slot;
    }catch(...){return {};}
  }
  bool bind_reference(const std::shared_ptr<Binding>& binding,const Reference& reference) noexcept {
    try {
      if(!binding||reference.cache_identity.lock().get()!=state_.get())return false;
      std::lock_guard<std::mutex> lock(binding->mutex);
      if(binding->owner.lock().get()!=this||!binding->identity||!(*binding->identity==reference.head.identity)||
         (binding->template_kind&&*binding->template_kind!=reference.payload_kind))return false;
      if(binding->reference&&(reference.generation<binding->reference->generation||
         reference.head.sequence<binding->reference->head.sequence))return false;
      binding->reference=reference;return true;
    }catch(...){return false;}
  }
  Claim claim_resident(const versioning::Head& head,const Reference& ref,PayloadKind kind) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      if(ref.cache_identity.lock().get()!=state_.get()||!head_||!(*head_==head)||!(ref.head==head)||ref.payload_kind!=kind||ref.slot>=slots_.size())return {};
      const auto* entry=slots_[ref.slot];
      if(!entry||entry->generation!=ref.generation||entry->payload_kind!=kind)return {};
      entry->touch=++clock_;count(&HotQueryCacheStats::domain_hits);
      return {entry->value,{},ref};
    }catch(...){return {};}
  }
  // Bounded snapshot for storage-owned exact child views. Inspect immutable
  // payloads outside the cache lock; these pins retain all original charges.
  // A caller must still check head/coverage and perform ordinary admission.
  std::vector<Claim> resident_domains(const versioning::Head& head) noexcept {
    try {
      std::vector<Claim> result;
      std::lock_guard<std::mutex> lock(mutex_);
      if(!head_||!(*head_==head))return result;
      result.reserve(entries_.size());
      for(const auto& item:entries_) {
        const auto& entry=item.second;
        result.push_back({entry.value,{},Reference{entry.slot,entry.generation,
          head,entry.payload_kind,state_}});
      }
      return result;
    }catch(...){return {};}
  }
  Claim claim(const versioning::Head& head,const std::string& key,
              PayloadKind kind=PayloadKind::Generic) noexcept {
    try {
      std::map<std::string,Entry> retired;
      std::map<std::string,const Ticket*> retired_flights;
      std::unique_lock<std::mutex> lock(mutex_);
      if(head_&&head_->identity==head.identity&&head_->sequence>head.sequence)return {};
      if(!head_||!(*head_==head)){head_=head;slots_.fill(nullptr);retired.swap(entries_);retired_flights.swap(flights_);changed_.notify_all();}
      // Destruction may release full catalogs; never do that under this lock.
      lock.unlock();retired.clear();retired_flights.clear();lock.lock();
      for(;;) {
        if(!head_||!(*head_==head))return {};
        if(auto found=entries_.find(key);found!=entries_.end()) {
          found->second.touch=++clock_;count(&HotQueryCacheStats::domain_hits);
          if(found->second.payload_kind!=kind)return {};
          return {found->second.value,{},Reference{found->second.slot,found->second.generation,head,kind,state_}};
        }
        if(flights_.find(key)!=flights_.end()) {
          count(&HotQueryCacheStats::domain_waits);
          changed_.wait(lock,[&]{return flights_.find(key)==flights_.end()||!head_||!(*head_==head);});
          continue;
        }
        // Reserving may retire arbitrary optional payloads, whose destructors
        // may reenter this cache. Never carry the map lock into reclamation.
        lock.unlock();
        auto reservation=reserve(Kind::Domain,4096+4*key.capacity());
        lock.lock();
        if(!head_||!(*head_==head)) {lock.unlock();reservation.reset();return {};}
        if(entries_.find(key)!=entries_.end()||flights_.find(key)!=flights_.end()) {
          lock.unlock();reservation.reset();lock.lock();continue;
        }
        if(!reservation){count(&HotQueryCacheStats::opt_outs);return {};}
        const auto self=shared_from_this();
        auto ticket=std::make_shared<Ticket>();ticket->key=key;
        ticket->head=head;ticket->payload_kind=kind;ticket->reservation=std::move(reservation);
        flights_.emplace(key,ticket.get());ticket->owner=self;count(&HotQueryCacheStats::domain_builds);
        return {{},std::move(ticket)};
      }
    } catch(...) {count(&HotQueryCacheStats::opt_outs);return {};}
  }
  std::optional<Reference> publish(const std::shared_ptr<Ticket>& ticket,std::shared_ptr<const void> value) noexcept {
    // Keep payload destruction outside the map lock, including allocation errors.
    const auto keep_alive=value;
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      if(!ticket||!value||!head_||!(*head_==ticket->head)||generation_==std::numeric_limits<std::uint64_t>::max())return {};
      const auto flight=flights_.find(ticket->key);
      if(flight==flights_.end()||flight->second!=ticket.get())return {};
      std::size_t slot=0;while(slot<slots_.size()&&slots_[slot])++slot;
      if(slot==slots_.size())return {};
      const auto generation=++generation_; // Never wrap, including failed publication.
      auto inserted=entries_.emplace(ticket->key,Entry{std::move(value),++clock_,slot,generation,ticket->payload_kind,ticket->reservation});
      if(!inserted.second)return {};
      slots_[slot]=&inserted.first->second;
      return Reference{slot,generation,ticket->head,ticket->payload_kind,state_};
    }catch(...){count(&HotQueryCacheStats::opt_outs);return {};}
  }
private:
  static constexpr std::size_t maximum_retention_slots=4096;
  static bool relieve(const std::shared_ptr<State>& state,Kind kind,
      const Reservation* growing,bool count_pressure) noexcept {
    static thread_local bool active=false;
    if(active)return false;
    struct Scope {bool& active;explicit Scope(bool& value):active(value){active=true;}~Scope(){active=false;}} scope(active);
    try {
      std::vector<std::weak_ptr<RetainedSlot>> slots;
      std::weak_ptr<HotQueryCache> owner;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        ++state->stats.pressure_requests;
        if(count_pressure&&(kind==Kind::Proof||kind==Kind::Handle)) {
          // Misses are not evidence that resident proofs are cold. Repeated
          // count sweeps turn a scan just larger than the cap into zero hits,
          // including proofs freshly built and used without a cache acquire.
          // Bypass optional admission; bytes and owner retirement still reclaim.
          ++state->stats.admission_bypasses;return false;
        }
        slots=state->slots;owner=state->owner;
      }
      // Count pressure cannot be fixed by dropping unrelated object classes.
      if(!(count_pressure&&kind==Kind::Domain)) {
        for(unsigned pass=0;pass<2;++pass) {
          for(unsigned priority=0;priority<=unsigned(Retention::ParentProof);++priority) {
            for(const auto& weak:slots)if(auto slot=weak.lock()) {
              if(unsigned(slot->priority_)==priority&&slot->release(true,pass!=0))return true;
            }
          }
        }
      }
      if(auto cache=owner.lock())return cache->retire_domain(growing);
    }catch(...){}
    return false;
  }
  bool retire_domain(const Reservation* growing) noexcept {
    std::shared_ptr<const void> retired;
    {
      std::unique_lock<std::mutex> lock(mutex_,std::try_to_lock);
      if(!lock.owns_lock())return false;
      auto oldest=entries_.end();
      for(auto it=entries_.begin();it!=entries_.end();++it)
        if(it->second.reservation.get()!=growing&&
           (oldest==entries_.end()||it->second.touch<oldest->second.touch))oldest=it;
      if(oldest==entries_.end())return false;
      retired=std::move(oldest->second.value);slots_[oldest->second.slot]=nullptr;entries_.erase(oldest);
      count(&HotQueryCacheStats::evictions);count(&HotQueryCacheStats::pressure_domain_evictions);
    }
    retired.reset();return true;
  }
  struct Entry {
    std::shared_ptr<const void> value;
    mutable std::uint64_t touch;
    std::size_t slot;
    std::uint64_t generation;
    PayloadKind payload_kind;
    std::shared_ptr<Reservation> reservation;
  };
  void finish(const Ticket& ticket) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it=flights_.find(ticket.key);
    if(it!=flights_.end()&&it->second==&ticket)flights_.erase(it);
    changed_.notify_all();
  }
  std::shared_ptr<State> state_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::optional<versioning::Head> head_;
  std::map<std::string,Entry> entries_;
  std::map<std::string,const Ticket*> flights_;
  std::array<Entry*,maximum_domains> slots_{};
  std::uint64_t clock_{0},generation_{0};
};
} // namespace iotdbfs
