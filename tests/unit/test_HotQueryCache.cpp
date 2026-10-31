#include "iotdbfs/HotQueryCache.h"
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

using namespace iotdbfs;
namespace {
using namespace std::chrono_literals;
versioning::Head hot_head(std::uint64_t sequence = 1) {
  return {{"namespace", "incarnation"}, sequence, std::string(64, 'a'), std::string(64, 'b')};
}
// The last payload owner must release its data before refunding its charge.
// A published payload always retains the reservation, as real callers do.
struct HotPayload {
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  std::vector<unsigned char> data;
  HotPayload(std::shared_ptr<HotQueryCache::Reservation> charge, std::size_t bytes)
      : reservation(std::move(charge)), data(bytes, 42) {}
};
std::shared_ptr<const HotPayload> hot_payload(const std::shared_ptr<HotQueryCache::Ticket>& ticket,
                                            std::size_t bytes = 512) {
  if (!ticket || !ticket->reservation->grow(bytes)) return {};
  return std::make_shared<const HotPayload>(ticket->reservation, bytes);
}
bool waiting(const std::shared_ptr<HotQueryCache>& cache, std::uint64_t count) {
  const auto until = std::chrono::steady_clock::now() + 2s;
  while (cache->stats().domain_waits < count && std::chrono::steady_clock::now() < until)
    std::this_thread::sleep_for(1ms);
  return cache->stats().domain_waits >= count;
}
void bounded(const HotQueryCacheStats& stats) {
  EXPECT_LE(stats.resident_bytes, HotQueryCache::maximum_bytes);
  EXPECT_LE(stats.domains, HotQueryCache::maximum_domains);
  EXPECT_LE(stats.proofs, HotQueryCache::maximum_proofs);
  EXPECT_LE(stats.handles, HotQueryCache::maximum_handles);
  EXPECT_LE(stats.handle_bytes, HotQueryCache::maximum_handle_bytes);
}
} // namespace

TEST(HotQueryCache, BaseAndReservationsShareOneHardByteLimit) {
  auto cache = std::make_shared<HotQueryCache>();
  EXPECT_EQ(cache->stats().resident_bytes, 4096u);
  auto whole = cache->reserve(HotQueryCache::Kind::Domain, HotQueryCache::maximum_bytes - 4096);
  ASSERT_TRUE(whole);
  EXPECT_EQ(cache->stats().resident_bytes, HotQueryCache::maximum_bytes);
  EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Proof, 1));
  EXPECT_FALSE(whole->grow(1));
  whole->shrink(1024); EXPECT_EQ(whole->available(), 1024u);
  EXPECT_TRUE(whole->grow(1024)); bounded(cache->stats());
  whole.reset();
  EXPECT_EQ(cache->stats().resident_bytes, 4096u);
  EXPECT_EQ(cache->stats().domains, 0u); EXPECT_EQ(cache->stats().proofs, 0u);
}

TEST(HotQueryCache, InflightDomainsAndProofPinsCountTowardEntryCaps) {
  auto cache = std::make_shared<HotQueryCache>();
  std::vector<std::shared_ptr<HotQueryCache::Ticket>> domains;
  for (unsigned i = 0; i < 16; ++i) {
    auto claim = cache->claim(hot_head(), "domain-" + std::to_string(i));
    ASSERT_TRUE(claim.ticket); domains.push_back(std::move(claim.ticket)); bounded(cache->stats());
  }
  auto overflow = cache->claim(hot_head(), "seventeenth");
  EXPECT_FALSE(overflow.ticket); EXPECT_FALSE(overflow.value);
  EXPECT_EQ(cache->stats().domains, 16u); EXPECT_EQ(cache->stats().opt_outs, 1u);
  std::vector<std::shared_ptr<HotQueryCache::Reservation>> proofs;
  for (unsigned i = 0; i < 64; ++i) {
    auto proof = cache->reserve(HotQueryCache::Kind::Proof, 64);
    ASSERT_TRUE(proof); proofs.push_back(std::move(proof)); bounded(cache->stats());
  }
  const auto before = cache->stats();
  EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Proof, 64));
  EXPECT_EQ(cache->stats().resident_bytes, before.resident_bytes);
  EXPECT_EQ(cache->stats().proofs, 64u);
  domains.clear(); proofs.clear();
  EXPECT_EQ(cache->stats().domains, 0u); EXPECT_EQ(cache->stats().proofs, 0u);
  EXPECT_EQ(cache->stats().resident_bytes, 4096u);
}

TEST(HotQueryCache, EightWaitersJoinOneProducerAndTheIdenticalPublishedPayload) {
  auto cache = std::make_shared<HotQueryCache>();
  auto first = cache->claim(hot_head(), "shared"); ASSERT_TRUE(first.ticket);
  auto payload = hot_payload(first.ticket); ASSERT_TRUE(payload);
  std::vector<std::future<HotQueryCache::Claim>> futures;
  for (unsigned i = 0; i < 8; ++i)
    futures.push_back(std::async(std::launch::async, [cache] { return cache->claim(hot_head(), "shared"); }));
  const bool all_waiting = waiting(cache, 8);
  // Release every producer ticket before joining, including timeout failures.
  cache->publish(first.ticket, payload); first.ticket.reset();
  bool all_ready = true;
  for (auto& future : futures) all_ready &= future.wait_for(2s) == std::future_status::ready;
  EXPECT_TRUE(all_waiting); EXPECT_TRUE(all_ready);
  for (auto& future : futures) {
    auto value = future.get(); EXPECT_FALSE(value.ticket); EXPECT_EQ(value.value.get(), payload.get());
  }
  EXPECT_EQ(cache->stats().domain_builds, 1u); EXPECT_EQ(cache->stats().domain_hits, 8u);
  EXPECT_EQ(cache->stats().domains, 1u); bounded(cache->stats());
}

TEST(HotQueryCache, FailedBuilderTicketDestructionWakesWaiterToRetry) {
  auto cache = std::make_shared<HotQueryCache>();
  auto first = cache->claim(hot_head(), "retry"); ASSERT_TRUE(first.ticket);
  auto future = std::async(std::launch::async, [cache] {return cache->claim(hot_head(), "retry");});
  const bool was_waiting = waiting(cache, 1);
  first.ticket.reset();
  const bool ready = future.wait_for(2s) == std::future_status::ready;
  EXPECT_TRUE(was_waiting); EXPECT_TRUE(ready);
  auto retry = future.get(); ASSERT_TRUE(retry.ticket); EXPECT_FALSE(retry.value);
  EXPECT_EQ(cache->stats().domain_builds, 2u); EXPECT_EQ(cache->stats().domains, 1u);
  retry.ticket.reset(); EXPECT_EQ(cache->stats().domains, 0u);
  EXPECT_EQ(cache->stats().resident_bytes, 4096u);
}

TEST(HotQueryCache, HeadChangeWakesOldWaiterWithoutWaitingForOldBuilderOrReusingNewValue) {
  auto cache = std::make_shared<HotQueryCache>();
  auto old = cache->claim(hot_head(), "same-key"); ASSERT_TRUE(old.ticket);
  auto old_payload = hot_payload(old.ticket); ASSERT_TRUE(old_payload);
  auto old_waiter = std::async(std::launch::async, [cache] {return cache->claim(hot_head(), "same-key");});
  const bool was_waiting = waiting(cache, 1);
  auto switching = std::async(std::launch::async, [cache] {return cache->claim(hot_head(2), "same-key");});
  const bool new_head_did_not_wait_for_old_builder = switching.wait_for(500ms) == std::future_status::ready;
  if (!new_head_did_not_wait_for_old_builder) old.ticket.reset();
  auto fresh = switching.get();
  const bool old_woke_before_finish = old_waiter.wait_for(500ms) == std::future_status::ready;
  // Cleanup also wakes a buggy old implementation that forgot head notification.
  old.ticket.reset();
  auto retired = old_waiter.get();
  EXPECT_TRUE(was_waiting); EXPECT_TRUE(new_head_did_not_wait_for_old_builder); EXPECT_TRUE(old_woke_before_finish);
  EXPECT_FALSE(retired.ticket); EXPECT_FALSE(retired.value);
  ASSERT_TRUE(fresh.ticket);
  EXPECT_EQ(cache->stats().domains, 2u); // Old payload still pins its reservation.
  const auto old_charge = cache->stats().resident_bytes;
  auto fresh_waiter = std::async(std::launch::async, [cache] {return cache->claim(hot_head(2), "same-key");});
  const bool new_waited = waiting(cache, 2);
  auto fresh_payload = hot_payload(fresh.ticket);
  cache->publish(fresh.ticket, fresh_payload); fresh.ticket.reset();
  EXPECT_EQ(fresh_waiter.wait_for(2s), std::future_status::ready);
  auto hit = fresh_waiter.get(); EXPECT_TRUE(new_waited);
  EXPECT_FALSE(hit.ticket); EXPECT_EQ(hit.value.get(), fresh_payload.get());
  old_payload.reset();
  EXPECT_EQ(cache->stats().domains, 1u); EXPECT_LT(cache->stats().resident_bytes, old_charge + 512);
  EXPECT_EQ(cache->stats().domain_builds, 2u); bounded(cache->stats());
}

TEST(HotQueryCache, RetiredPayloadsHoldAllSixteenSlotsUntilFinalExternalPinsRelease) {
  auto cache = std::make_shared<HotQueryCache>();
  std::vector<std::shared_ptr<const HotPayload>> pins;
  for (unsigned i = 0; i < 16; ++i) {
    auto claim = cache->claim(hot_head(), "old-" + std::to_string(i)); ASSERT_TRUE(claim.ticket);
    auto payload = hot_payload(claim.ticket); ASSERT_TRUE(payload);
    cache->publish(claim.ticket, payload); pins.push_back(std::move(payload)); claim.ticket.reset();
  }
  const auto resident = cache->stats().resident_bytes;
  auto no_room = cache->claim(hot_head(2), "new-head");
  EXPECT_FALSE(no_room.ticket); EXPECT_FALSE(no_room.value);
  EXPECT_EQ(cache->stats().domains, 16u); EXPECT_EQ(cache->stats().resident_bytes, resident);
  pins.clear(); EXPECT_EQ(cache->stats().domains, 0u); EXPECT_EQ(cache->stats().resident_bytes, 4096u);
  auto admitted = cache->claim(hot_head(2), "new-head"); EXPECT_TRUE(admitted.ticket); bounded(cache->stats());
}

TEST(HotQueryCache, LruEvictionCannotRefundAnExternallyPinnedPayload) {
  auto cache = std::make_shared<HotQueryCache>();
  std::shared_ptr<const HotPayload> pinned;
  std::weak_ptr<const HotPayload> second;
  for (unsigned i = 0; i < 16; ++i) {
    auto claim = cache->claim(hot_head(), "lru-" + std::to_string(i)); ASSERT_TRUE(claim.ticket);
    auto payload = hot_payload(claim.ticket); ASSERT_TRUE(payload);
    if (i == 0) pinned = payload;
    if (i == 1) second = payload;
    cache->publish(claim.ticket, std::move(payload)); claim.ticket.reset();
  }
  EXPECT_FALSE(second.expired());
  auto newest = cache->claim(hot_head(), "seventeenth"); ASSERT_TRUE(newest.ticket);
  // Oldest is removed but stays charged; the next unpinned entry must also go.
  EXPECT_EQ(cache->stats().evictions, 2u); EXPECT_TRUE(second.expired());
  EXPECT_EQ(cache->stats().domains, 16u);
  const auto resident = cache->stats().resident_bytes;
  pinned.reset(); EXPECT_EQ(cache->stats().domains, 15u); EXPECT_LT(cache->stats().resident_bytes, resident);
  bounded(cache->stats());
}

TEST(HotQueryCache, EqualSequenceWithDifferentFullHeadMissesAndOlderHeadCannotReplaceNewer) {
  auto cache = std::make_shared<HotQueryCache>();
  auto first = cache->claim(hot_head(), "domain"); ASSERT_TRUE(first.ticket);
  auto payload = hot_payload(first.ticket); cache->publish(first.ticket, payload); first.ticket.reset();
  auto changed = hot_head(); changed.root = std::string(64, 'c');
  auto different = cache->claim(changed, "domain"); ASSERT_TRUE(different.ticket); EXPECT_FALSE(different.value);
  different.ticket.reset();
  auto newer = cache->claim(hot_head(2), "domain"); ASSERT_TRUE(newer.ticket);
  auto stale = cache->claim(hot_head(), "domain");
  EXPECT_FALSE(stale.ticket); EXPECT_FALSE(stale.value); bounded(cache->stats());
}

TEST(HotQueryCache, HandleSubquotaAndAuxiliaryAccountingSurviveRetiredPins) {
  auto cache=std::make_shared<HotQueryCache>();
  auto handle=cache->reserve(HotQueryCache::Kind::Handle,HotQueryCache::maximum_handle_bytes);
  ASSERT_TRUE(handle); EXPECT_EQ(cache->stats().handles,1u);
  EXPECT_EQ(cache->stats().handle_bytes,HotQueryCache::maximum_handle_bytes);
  EXPECT_FALSE(handle->grow(1)); EXPECT_EQ(handle->available(),0u);
  EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Handle,1));
  auto auxiliary=cache->reserve(HotQueryCache::Kind::Auxiliary,4096);
  ASSERT_TRUE(auxiliary);EXPECT_EQ(cache->stats().domains,0u);EXPECT_EQ(cache->stats().proofs,0u);
  handle->shrink(1024);EXPECT_EQ(handle->available(),1024u);EXPECT_TRUE(handle->grow(1024));
  auto pin=handle;handle.reset();EXPECT_EQ(cache->stats().handles,1u);
  pin.reset();EXPECT_EQ(cache->stats().handles,0u);EXPECT_EQ(cache->stats().handle_bytes,0u);
  auxiliary.reset();EXPECT_EQ(cache->stats().resident_bytes,4096u);
  std::vector<std::shared_ptr<HotQueryCache::Reservation>> handles;
  for(unsigned i=0;i<64;++i){auto h=cache->reserve(HotQueryCache::Kind::Handle,1);ASSERT_TRUE(h);handles.push_back(h);}
  EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Handle,0));
  EXPECT_TRUE(cache->reserve(HotQueryCache::Kind::Auxiliary,1));
}

TEST(HotQueryCache, ResidentReferenceChecksPayloadFullHeadAndActualResidency) {
  auto cache=std::make_shared<HotQueryCache>();
  const auto kind=HotQueryCache::PayloadKind::Hdf5Dataset;
  auto build=cache->claim(hot_head(),"domain",kind);ASSERT_TRUE(build.ticket);
  auto payload=hot_payload(build.ticket);auto reference=cache->publish(build.ticket,payload);
  ASSERT_TRUE(reference);build.ticket.reset();
  auto hit=cache->claim_resident(hot_head(),*reference,kind);EXPECT_EQ(hit.value.get(),payload.get());
  EXPECT_FALSE(cache->claim_resident(hot_head(),*reference,HotQueryCache::PayloadKind::CandidateDomain).value);
  auto changed=hot_head();changed.commit=std::string(64,'d');
  EXPECT_FALSE(cache->claim_resident(changed,*reference,kind).value);
  auto next=cache->claim(hot_head(2),"next",kind);ASSERT_TRUE(next.ticket);
  EXPECT_FALSE(cache->claim_resident(hot_head(),*reference,kind).value);
  EXPECT_FALSE(cache->claim_resident(hot_head(2),*reference,kind).value);
  // A live payload pin does not make its retired cache entry reusable.
  EXPECT_TRUE(payload);EXPECT_EQ(hit.value.get(),payload.get());
  auto next_payload=hot_payload(next.ticket);auto new_reference=cache->publish(next.ticket,next_payload);
  ASSERT_TRUE(new_reference);EXPECT_NE(reference->generation,new_reference->generation);
  EXPECT_EQ(reference->slot,new_reference->slot);
  EXPECT_FALSE(cache->claim_resident(hot_head(2),*reference,kind).value);
  EXPECT_EQ(cache->claim_resident(hot_head(2),*new_reference,kind).value.get(),next_payload.get());
}

TEST(HotQueryCache, ResidentReferenceTouchesLruAndCannotReviveEvictedPinnedEntry) {
  auto cache=std::make_shared<HotQueryCache>();
  const auto kind=HotQueryCache::PayloadKind::Hdf5Dataset;
  std::vector<HotQueryCache::Reference> refs;
  std::shared_ptr<const HotPayload> pinned;
  for(unsigned i=0;i<16;++i){
    auto build=cache->claim(hot_head(),"d-"+std::to_string(i),kind);ASSERT_TRUE(build.ticket);
    auto payload=hot_payload(build.ticket);if(i==1)pinned=payload;
    auto ref=cache->publish(build.ticket,payload);ASSERT_TRUE(ref);refs.push_back(*ref);
  }
  EXPECT_TRUE(cache->claim_resident(hot_head(),refs[0],kind).value);
  auto extra=cache->claim(hot_head(),"extra",kind);ASSERT_TRUE(extra.ticket);
  EXPECT_TRUE(cache->claim_resident(hot_head(),refs[0],kind).value);
  EXPECT_FALSE(cache->claim_resident(hot_head(),refs[1],kind).value);
  EXPECT_TRUE(pinned);EXPECT_EQ(cache->stats().evictions,2u);
  auto replacement=hot_payload(extra.ticket);auto ref=cache->publish(extra.ticket,replacement);ASSERT_TRUE(ref);
  EXPECT_EQ(ref->slot,refs[1].slot);EXPECT_NE(ref->generation,refs[1].generation);
  EXPECT_FALSE(cache->claim_resident(hot_head(),refs[1],kind).value);
}

TEST(HotQueryCache, IdenticalSlotGenerationAndHeadInAnotherCacheCannotClaimReference) {
  const auto kind=HotQueryCache::PayloadKind::Hdf5Dataset;
  auto first=std::make_shared<HotQueryCache>();auto second=std::make_shared<HotQueryCache>();
  auto a=first->claim(hot_head(),"a",kind);auto b=second->claim(hot_head(),"b",kind);
  ASSERT_TRUE(a.ticket);ASSERT_TRUE(b.ticket);
  auto pa=hot_payload(a.ticket);auto pb=hot_payload(b.ticket);
  auto ra=first->publish(a.ticket,pa);auto rb=second->publish(b.ticket,pb);
  ASSERT_TRUE(ra);ASSERT_TRUE(rb);EXPECT_EQ(ra->slot,rb->slot);EXPECT_EQ(ra->generation,rb->generation);
  EXPECT_FALSE(second->claim_resident(hot_head(),*ra,kind).value);
  EXPECT_FALSE(first->claim_resident(hot_head(),*rb,kind).value);
}

TEST(HotQueryCache, BindingReferenceUpdatesNeverRegressHeadOrGeneration) {
  auto cache=std::make_shared<HotQueryCache>();
  auto binding=std::make_shared<HotQueryCache::Binding>();
  binding->owner=cache;binding->identity=hot_head().identity;
  const auto kind=HotQueryCache::PayloadKind::Hdf5Dataset;
  auto first=cache->claim(hot_head(),"first",kind);auto p1=hot_payload(first.ticket);
  auto old=cache->publish(first.ticket,p1);ASSERT_TRUE(old);ASSERT_TRUE(cache->bind_reference(binding,*old));
  auto second=cache->claim(hot_head(2),"second",kind);auto p2=hot_payload(second.ticket);
  auto fresh=cache->publish(second.ticket,p2);ASSERT_TRUE(fresh);ASSERT_TRUE(cache->bind_reference(binding,*fresh));
  EXPECT_FALSE(cache->bind_reference(binding,*old));
  auto lower_head=*old;lower_head.generation=fresh->generation+1;
  EXPECT_FALSE(cache->bind_reference(binding,lower_head));
  auto lower_generation=*fresh;lower_generation.head.sequence=3;lower_generation.generation=old->generation;
  EXPECT_FALSE(cache->bind_reference(binding,lower_generation));
  auto other_namespace=*fresh;other_namespace.head.identity.incarnation="another";
  EXPECT_FALSE(cache->bind_reference(binding,other_namespace));
  EXPECT_EQ(binding->reference->generation,fresh->generation);EXPECT_EQ(binding->reference->head.sequence,2u);
  auto another=std::make_shared<HotQueryCache>();
  EXPECT_FALSE(another->bind_reference(binding,*fresh));
}

TEST(HotQueryCache, BoundTemplatePayloadKindRejectsCrossEndpointReference) {
  auto cache=std::make_shared<HotQueryCache>();
  auto binding=std::make_shared<HotQueryCache::Binding>();
  binding->owner=cache;binding->identity=hot_head().identity;
  binding->template_kind=HotQueryCache::PayloadKind::Hdf5Dataset;
  auto build=cache->claim(hot_head(),"candidate",HotQueryCache::PayloadKind::CandidateDomain);
  auto payload=hot_payload(build.ticket);auto ref=cache->publish(build.ticket,payload);ASSERT_TRUE(ref);
  EXPECT_FALSE(cache->bind_reference(binding,*ref));EXPECT_FALSE(binding->reference);
  binding->template_kind=HotQueryCache::PayloadKind::CandidateDomain;
  EXPECT_TRUE(cache->bind_reference(binding,*ref));ASSERT_TRUE(binding->reference);
  EXPECT_EQ(binding->reference->payload_kind,HotQueryCache::PayloadKind::CandidateDomain);
}

TEST(HotQueryCache, ResidentDomainSnapshotIsBoundedAndKeepsRetiredAccounting) {
  auto cache=std::make_shared<HotQueryCache>();
  auto build=cache->claim(hot_head(),"backing",HotQueryCache::PayloadKind::CandidateDomain);
  ASSERT_TRUE(build.ticket);
  auto payload=hot_payload(build.ticket);
  auto reference=cache->publish(build.ticket,payload);ASSERT_TRUE(reference);
  payload.reset();build.ticket.reset();
  auto snapshot=cache->resident_domains(hot_head());ASSERT_EQ(snapshot.size(),1u);
  EXPECT_EQ(snapshot.front().reference->payload_kind,HotQueryCache::PayloadKind::CandidateDomain);
  const auto live=cache->stats();
  EXPECT_EQ(live.resident_domains,1u);EXPECT_EQ(live.retired_domains,0u);
  EXPECT_EQ(live.domain_bytes,live.resident_domain_bytes);
  auto next=cache->claim(hot_head(2),"new");ASSERT_TRUE(next.ticket);
  EXPECT_TRUE(cache->resident_domains(hot_head()).empty());
  EXPECT_FALSE(cache->claim_resident(hot_head(),*reference,reference->payload_kind).value);
  const auto retired=cache->stats();
  EXPECT_EQ(retired.retired_domains,1u);EXPECT_EQ(retired.inflight_domains,1u);
  EXPECT_EQ(retired.retired_domain_bytes,live.domain_bytes);
  snapshot.clear();
  EXPECT_EQ(cache->stats().retired_domains,0u);
  EXPECT_EQ(cache->stats().retired_domain_bytes,0u);bounded(cache->stats());
}

TEST(HotQueryCache, CategoryBytesFollowGrowthShrinkAndExternalPins) {
  auto cache=std::make_shared<HotQueryCache>();
  auto domain=cache->reserve(HotQueryCache::Kind::Domain,100);
  auto proof=cache->reserve(HotQueryCache::Kind::Proof,200);
  auto auxiliary=cache->reserve(HotQueryCache::Kind::Auxiliary,300);
  auto handle=cache->reserve(HotQueryCache::Kind::Handle,400);
  ASSERT_TRUE(domain&&proof&&auxiliary&&handle);
  ASSERT_TRUE(proof->grow(50));auxiliary->shrink(20);
  auto stats=cache->stats();
  EXPECT_EQ(stats.domain_bytes,100u);EXPECT_EQ(stats.proof_bytes,250u);
  EXPECT_EQ(stats.auxiliary_bytes,680u);
  EXPECT_EQ(stats.resident_bytes,4096u+100+250+680);
  auto pin=domain;domain.reset();EXPECT_EQ(cache->stats().domain_bytes,100u);
  pin.reset();proof.reset();auxiliary.reset();handle.reset();
  stats=cache->stats();EXPECT_EQ(stats.domain_bytes,0u);EXPECT_EQ(stats.proof_bytes,0u);
  EXPECT_EQ(stats.auxiliary_bytes,0u);EXPECT_EQ(stats.resident_bytes,4096u);
}

TEST(HotQueryCache, AuxiliaryPressureReclaimsDerivedValueBeforeResidentParent) {
  auto cache=std::make_shared<HotQueryCache>();
  auto parent=cache->claim(hot_head(),"parent",HotQueryCache::PayloadKind::CandidateDomain);
  auto payload=hot_payload(parent.ticket);auto ref=cache->publish(parent.ticket,payload);
  ASSERT_TRUE(ref);payload.reset();parent.ticket.reset();
  auto charge=cache->reserve(HotQueryCache::Kind::Auxiliary,4096);ASSERT_TRUE(charge);
  auto derived=std::make_shared<HotPayload>(charge,0);
  auto slot=cache->retain(derived,HotQueryCache::Retention::Derived,64);ASSERT_TRUE(slot);
  derived.reset();charge.reset();
  const auto before=cache->stats();
  auto filler=cache->reserve(HotQueryCache::Kind::Auxiliary,
    HotQueryCache::maximum_bytes-before.resident_bytes-1024);ASSERT_TRUE(filler);
  auto admitted=cache->reserve(HotQueryCache::Kind::Auxiliary,2048);ASSERT_TRUE(admitted);
  EXPECT_FALSE(slot->present());EXPECT_EQ(cache->stats().slot_evictions,1u);
  EXPECT_EQ(cache->stats().pressure_domain_evictions,0u);
  EXPECT_TRUE(cache->claim_resident(hot_head(),*ref,ref->payload_kind).value);
  bounded(cache->stats());
}

TEST(HotQueryCache, ReservationGrowthCanReclaimAWholeUnpinnedDomain) {
  auto cache=std::make_shared<HotQueryCache>();
  auto parent=cache->claim(hot_head(),"old-domain");
  auto payload=hot_payload(parent.ticket);auto ref=cache->publish(parent.ticket,payload);
  ASSERT_TRUE(ref);payload.reset();parent.ticket.reset();
  auto growing=cache->reserve(HotQueryCache::Kind::Auxiliary,
    HotQueryCache::maximum_bytes-cache->stats().resident_bytes-32);ASSERT_TRUE(growing);
  ASSERT_TRUE(growing->grow(4096));
  EXPECT_FALSE(cache->claim_resident(hot_head(),*ref,ref->payload_kind).value);
  EXPECT_EQ(cache->stats().pressure_domain_evictions,1u);bounded(cache->stats());
}

TEST(HotQueryCache, ProofCountPressureBypassesCyclicScanInsteadOfEvictingEveryHotProof) {
  auto cache=std::make_shared<HotQueryCache>();
  std::vector<std::shared_ptr<HotQueryCache::RetainedSlot>> slots;
  for(unsigned i=0;i<64;++i) {
    auto charge=cache->reserve(HotQueryCache::Kind::Proof,64);ASSERT_TRUE(charge);
    auto proof=std::make_shared<HotPayload>(charge,0);
    auto slot=cache->retain(proof,i?HotQueryCache::Retention::Proof:HotQueryCache::Retention::ParentProof);
    ASSERT_TRUE(slot);slots.push_back(std::move(slot));
  }
  for(unsigned scan=0;scan<3;++scan) {
    // Even a cold parent proof is protected from child-count overflow.
    for(std::size_t i=1;i<slots.size();++i)EXPECT_TRUE(slots[i]->acquire());
    EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Proof,64));
    EXPECT_EQ(cache->stats().proofs,64u);EXPECT_EQ(cache->stats().slot_evictions,0u);
  }
  EXPECT_EQ(cache->stats().admission_bypasses,3u);
  // Several consecutive misses must not age the hot set and start a scan.
  EXPECT_FALSE(cache->reserve(HotQueryCache::Kind::Proof,64));
  EXPECT_TRUE(slots.front()->present());EXPECT_TRUE(slots[1]->present());
  EXPECT_EQ(cache->stats().admission_bypasses,4u);
  EXPECT_EQ(cache->stats().slot_evictions,0u);bounded(cache->stats());
}

TEST(HotQueryCache, ColdProofScansWithMultipleOverflowMissesPreserveAdmittedWorkingSet) {
  // Real admission constructs and uses a proof without a later cache acquire.
  // Old frozen query contexts may also retain several parent proofs. Every
  // traversal then has several misses, not merely one after all resident hits.
  for(const std::size_t parents:{1u,2u,16u}) {
    auto cache=std::make_shared<HotQueryCache>();
    std::vector<std::shared_ptr<HotQueryCache::RetainedSlot>> pinned_parents;
    for(std::size_t i=0;i<parents;++i) {
      auto charge=cache->reserve(HotQueryCache::Kind::Proof,64);ASSERT_TRUE(charge);
      auto proof=std::make_shared<HotPayload>(charge,0);
      auto slot=cache->retain(proof,HotQueryCache::Retention::ParentProof);ASSERT_TRUE(slot);
      pinned_parents.push_back(std::move(slot));
    }
    std::vector<std::shared_ptr<HotQueryCache::RetainedSlot>> children(64);
    auto build=[&](std::size_t i) {
      auto charge=cache->reserve(HotQueryCache::Kind::Proof,64);
      if(charge)children[i]=cache->retain(std::make_shared<HotPayload>(charge,0),
        HotQueryCache::Retention::Proof);
    };
    for(std::size_t i=0;i<children.size();++i)build(i);
    std::size_t hits=0;
    for(unsigned round=0;round<2;++round)for(std::size_t i=0;i<children.size();++i) {
      if(children[i]&&children[i]->acquire())++hits;
      else build(i);
    }
    EXPECT_EQ(hits,2*(64-parents));
    EXPECT_EQ(cache->stats().slot_evictions,0u);
    EXPECT_EQ(cache->stats().admission_bypasses,3*parents);
    for(const auto& parent:pinned_parents)EXPECT_TRUE(parent->present());
    bounded(cache->stats());
  }
}

TEST(HotQueryCache, ClearingSlotRetainsExecutionPinsAndRefundsOnlyAtLastRelease) {
  auto cache=std::make_shared<HotQueryCache>();
  auto charge=cache->reserve(HotQueryCache::Kind::Proof,2048);ASSERT_TRUE(charge);
  auto proof=std::make_shared<HotPayload>(charge,0);
  auto slot=cache->retain(proof,HotQueryCache::Retention::Proof,1024);ASSERT_TRUE(slot);
  proof.reset();charge.reset();auto pin=slot->acquire();ASSERT_TRUE(pin);
  const auto before=cache->stats();slot->clear();
  EXPECT_FALSE(slot->present());EXPECT_EQ(cache->stats().proofs,1u);
  EXPECT_EQ(cache->stats().resident_bytes,before.resident_bytes);
  pin.reset();EXPECT_EQ(cache->stats().proofs,0u);
  EXPECT_EQ(cache->stats().resident_bytes,before.resident_bytes-2048);
  EXPECT_EQ(cache->stats().retention_slots,1u);
  slot.reset();auto after=cache->stats();
  EXPECT_EQ(after.retention_slots,0u);EXPECT_EQ(after.retained_values,0u);
  EXPECT_EQ(after.resident_bytes,4096+after.retention_registry_bytes);
  EXPECT_EQ(after.auxiliary_bytes,after.retention_registry_bytes);bounded(after);
}

TEST(HotQueryCache, PressureDestructionCanReenterPoolAndCannotCycleItsLifetime) {
  struct Reentrant {
    std::weak_ptr<HotQueryCache> cache;
    std::shared_ptr<HotQueryCache::Reservation> charge;
    bool* destroyed;
    ~Reentrant(){if(auto owner=cache.lock())(void)owner->stats();*destroyed=true;}
  };
  auto cache=std::make_shared<HotQueryCache>();bool destroyed=false;
  auto charge=cache->reserve(HotQueryCache::Kind::Proof,1024);
  auto payload=std::make_shared<Reentrant>();payload->cache=cache;payload->charge=charge;payload->destroyed=&destroyed;
  auto slot=cache->retain(payload,HotQueryCache::Retention::Proof);ASSERT_TRUE(slot);
  payload.reset();charge.reset();slot->clear();EXPECT_TRUE(destroyed);
  std::weak_ptr<HotQueryCache> weak=cache;cache.reset();EXPECT_TRUE(weak.expired());
  EXPECT_FALSE(slot->present());slot.reset();
}

TEST(HotQueryCache, ClaimPressureDestroysRetiredValuesOutsideDomainMapLock) {
  struct Reentrant {
    std::weak_ptr<HotQueryCache> cache;
    std::shared_ptr<HotQueryCache::Reservation> charge;
    bool* destroyed;
    ~Reentrant(){if(auto owner=cache.lock())(void)owner->stats();*destroyed=true;}
  };
  auto cache=std::make_shared<HotQueryCache>();bool destroyed=false;
  auto charge=cache->reserve(HotQueryCache::Kind::Proof,16384);ASSERT_TRUE(charge);
  auto value=std::make_shared<Reentrant>();value->cache=cache;value->charge=charge;value->destroyed=&destroyed;
  auto slot=cache->retain(value,HotQueryCache::Retention::Proof);ASSERT_TRUE(slot);
  charge.reset();value.reset();
  auto filler=cache->reserve(HotQueryCache::Kind::Auxiliary,
    HotQueryCache::maximum_bytes-cache->stats().resident_bytes-32);ASSERT_TRUE(filler);
  auto admitted=cache->claim(hot_head(),"new-domain");
  EXPECT_TRUE(admitted.ticket);EXPECT_TRUE(destroyed);EXPECT_FALSE(slot->present());
  bounded(cache->stats());
}
