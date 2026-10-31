"""Offline declaration/coverage gates for attribution-only small real traces."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest

from experiments.fixed_matrix import fee_summary_trace as m
from experiments.fixed_matrix.tests.test_cscale_diagnosis import row as old_row, EXPECTED

REPO = Path(__file__).resolve().parents[3]


def fixture(tier='n8'):
    group, = m.runner.groups(REPO, tier)
    gid = group['group_id']
    binding = dict(scope=['/one', '/two'], expected_matches=deepcopy(EXPECTED),
                   corpus_id='independent-'+tier, case_id='case-'+tier,
                   oracle_sha256='oracle-'+tier, run_ids={'0': {'B': 'run-'+tier}})
    rows = []
    for r in range(m.WARMUPS+m.MEASURED):
        item = old_row(128, 0, 'B', r)
        item.update(group_id=gid, N=group['N'], C=group['C_paths'],
                    corpus_id=binding['corpus_id'], case_id=binding['case_id'],
                    oracle_sha256=binding['oracle_sha256'], run_id='run-'+tier,
                    product_sha256=m.PROBE_SHA, phase='warmup' if r < m.WARMUPS else 'measurement')
        counters = item['discovery_call']['server_profile']['counters']
        counters.update(dataset_catalog_selected_sources=group['C_paths'],
                        dataset_catalog_candidate_datasets=4*group['C_paths'])
        counters['cdiag.scope_paths'] = group['C_paths']
        counters['cdiag.selector.validated_paths'] = 2*group['C_paths']
        rows.append(item)
    return rows, {gid: binding}, group


class SmallTraceTests(unittest.TestCase):
    def analyze(self, tier='n8', mutate=None):
        rows, bindings, group = fixture(tier)
        if mutate:
            mutate(rows)
        return m.analyze_rows(rows, bindings, group, 'resource', m.PROBE_SHA)

    def test_independent_sealed_cases_and_probe_only_schedule(self):
        for tier, n in (('n8', 8), ('n128', 128)):
            group, = m.runner.groups(REPO, tier)
            self.assertEqual((group['N'], group['C_paths'], group['D'], group['K_units']), (n, n, 4, 2))
            self.assertEqual(m.runner.expected_rows(SimpleNamespace(tier=tier)), 15)
            self.assertEqual(m.runner.campaigns(group), (0,))
        self.assertEqual(m.runner.configuration_order(0), ('B',))
        with self.assertRaises(RuntimeError):
            m.runner.groups(REPO, 'n512')
        with self.assertRaises(RuntimeError):
            m.runner.configuration_order(1)

    def test_all_fifteen_rows_and_ten_formal_are_attribution_only(self):
        for tier in ('n8', 'n128'):
            out = self.analyze(tier)
            self.assertEqual((out['query_rows'], out['warmup_rows'], out['formal_rows'], out['mounts']), (15, 5, 10, 1))
            self.assertFalse(out['performance_claim'])
            self.assertEqual(out['configurations'], ['B'])
            self.assertNotIn('effects', out)

    def test_reject_missing_duplicate_reordered_wrong_namespace_and_product(self):
        mutations = (lambda r: r.pop(), lambda r: r.append(deepcopy(r[0])),
                     lambda r: r.reverse(), lambda r: r[0].update(N=512),
                     lambda r: r[0].update(configuration='A'),
                     lambda r: r[0].update(product_sha256='wrong'),
                     lambda r: r[0].update(resource_id='other'),
                     lambda r: r[0].update(phase='measurement'))
        for mutate in mutations:
            with self.assertRaises(RuntimeError):
                self.analyze(mutate=mutate)

    def test_formal_profile_requires_complete_actual_warm_fee_path(self):
        for key, value in (('hot_query.domain_hits', 0), ('cdiag.captured_batches', 3),
                           ('cdiag.capture_events', 4), ('cdiag.trace_exports', 1),
                           ('cdiag.budget.post_first_charge_delta', 1),
                           ('cdiag.fee.active_collector', 1)):
            def mutate(rows, key=key, value=value):
                rows[5]['discovery_call']['server_profile']['counters'][key] = value
            with self.assertRaises(RuntimeError):
                self.analyze(mutate=mutate)
        def mutate(rows):
            rows[5]['discovery_call']['server_profile']['iotdb_delta']['query_calls'] = 1
        with self.assertRaises(RuntimeError):
            self.analyze(mutate=mutate)

    def test_differing_actual_fee_counters_are_not_pooled(self):
        def mutate(rows):
            rows[6]['discovery_call']['server_profile']['counters']['cdiag.fee.visited_entries'] += 1
        with self.assertRaises(RuntimeError):
            self.analyze(mutate=mutate)

    def test_warmup_time_not_pooled_and_no_slow_formal_exclusion(self):
        expected = self.analyze()['discovery_ns']['mean']
        def warm(rows):
            for row in rows[:5]:
                row['discovery_ns'] *= 100
                row['client_components'] = {k: v*100 for k, v in row['client_components'].items()}
        self.assertEqual(self.analyze(mutate=warm)['discovery_ns']['mean'], expected)
        def slow(rows):
            row = rows[-1]
            row['discovery_ns'] *= 100
            row['client_components'] = {k: v*100 for k, v in row['client_components'].items()}
        self.assertGreater(self.analyze(mutate=slow)['discovery_ns']['mean'], expected)

    def test_n128_gate_requires_complete_restored_same_product_n8(self):
        gate = dict(protocol=m.PROTOCOL, tier='n8', query_records=15, expected_queries=15,
                    product_sha256=m.PROBE_SHA, baseline_sha256=m.diag.BASELINE_SHA,
                    diagnostic_only=True, performance_claim=False,
                    **{k: True for k in m.RESTORATION_FLAGS})
        m.runner.validate_n8_gate(gate, m.diag.BASELINE_SHA, m.PROBE_SHA)
        for key, value in (('query_records', 14), ('tier', 'n128'), ('protocol', m.diag.PROTOCOL),
                           ('resources_restored', False), ('performance_claim', True),
                           ('product_sha256', 'wrong'), ('failure', {})):
            bad = dict(gate, **{key: value})
            with self.assertRaises(RuntimeError):
                m.runner.validate_n8_gate(bad, m.diag.BASELINE_SHA, m.PROBE_SHA)


if __name__ == '__main__':
    unittest.main()
