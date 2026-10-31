"""Offline protocol checks; no service, corpus or resource mutations."""
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
import unittest
from experiments.fixed_matrix import fee_stage_summary as definition
from experiments.fixed_matrix.tests.test_hdf5_native_cpu import row

REPO=Path(__file__).resolve().parents[3]
class FeeStageSummaryProtocolTests(unittest.TestCase):
    def test_sealed_independent_sizes_and_480_rows(self):
        self.assertEqual(definition.PROTOCOL,'iotdbfs-fixed-fee-stage-summary/1')
        for tier,n in (('n8',8),('n128',128)):
            g=definition.runner.groups(REPO,tier)[0]
            self.assertEqual((g['N'],g['C_paths'],g['V_stored'],g['K_units']),(n,n,1,2))
            self.assertEqual(definition.runner.expected_rows(SimpleNamespace(repo=REPO,tier=tier)),240)
        self.assertEqual([definition.runner.configuration_order(c) for c in range(4)],
                         [('A','B'),('B','A'),('B','A'),('A','B')])
    def test_only_approved_two_product_files_and_immutable_protocol(self):
        self.assertEqual(definition.PRODUCT_DIFF,frozenset({'include/iotdbfs/query/core/QueryMetadataBudget.h','src/IoTDBStructuredBlobBackend.cpp'}))
        self.assertEqual(len(definition.REQUIRED_REGRESSION_TESTS),14)
        with self.assertRaises(FrozenInstanceError):definition.SPEC.protocol='changed'
    def candidate(self):
        group=definition.runner.groups(REPO,'n8')[0];r=row(group);r['phase']='measurement'
        r['discovery_call']['server_profile']['counters'].update({
            'hot_query.fee_summary_stages':4,'hot_query.fee_summary_entries_skipped':427,
            'hot_query.fee_replay_entries':0,'hot_query.fee_summary_fallbacks':0,
            'hot_query.fee_summary_collector_exits':0})
        return group,r
    def test_formal_candidate_requires_explicit_mechanism(self):
        group,r=self.candidate();definition.runner.validate_row(r,group,'B')
        del r['discovery_call']['server_profile']['counters']['hot_query.fee_replay_entries']
        with self.assertRaises(RuntimeError):definition.runner.validate_row(r,group,'B')
    def test_fallback_collector_partial_stage_and_zero_skip_rejected(self):
        for key,value in [('hot_query.fee_summary_stages',3),('hot_query.fee_summary_entries_skipped',0),
                          ('hot_query.fee_replay_entries',1),('hot_query.fee_summary_fallbacks',1),
                          ('hot_query.fee_summary_collector_exits',1),('hot_query.fee_replay_entries',False)]:
            group,r=self.candidate();r['discovery_call']['server_profile']['counters'][key]=value
            with self.assertRaises(RuntimeError):definition.runner.validate_row(r,group,'B')
    def test_baseline_missing_new_counters_is_unobserved(self):
        group=definition.runner.groups(REPO,'n8')[0];r=row(group);r['phase']='measurement'
        definition.runner.validate_row(r,group,'A')
    def test_new_n8_complete_restored_gate_required_for_n128(self):
        gate=dict(protocol=definition.PROTOCOL,tier='n8',query_records=240,
                  product_sha256='b'*64,baseline_sha256=definition.BASELINE_SHA)
        flags=('complete','scientific_complete','ready_for_analysis','original_service_restored',
               'resources_restored','controller_restoration_verified')
        gate.update({k:True for k in flags})
        definition.runner.validate_n8_gate(gate,definition.BASELINE_SHA,'b'*64)
        for key in flags:
            invalid=dict(gate,**{key:False})
            with self.assertRaises(RuntimeError):definition.runner.validate_n8_gate(invalid,definition.BASELINE_SHA,'b'*64)
if __name__=='__main__':unittest.main()
