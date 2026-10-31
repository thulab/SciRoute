import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_layout_system_control import ARMS, enrich_preparation_profiles, summarize


def observations(repetitions=2):
    rows = []
    for layout, mode in sorted(ARMS):
        # Original and derived gain different absolute amounts by backend.
        base = {'original': 12, 'cm8192': 10, 'cm128': 7, 'sm128': 4}[layout]
        if mode == 'iotdb':
            base += 4 if layout == 'original' else 2
        if mode == 'typed':
            base += 7
        for rep in range(repetitions):
            for repeat in range(4):
                # Three unequal warm observations, to exercise arithmetic mean
                # (not trimming) and independent-repetition min/max semantics.
                wall = float(base + rep * 2 + (0, 1, 2, 9)[repeat])
                row = dict(layout=layout, mode=mode, repetition=rep, pass_index=repeat,
                           temperature='cold' if repeat == 0 else 'warm', passed=True,
                           wall_seconds=wall, setup_seconds=.1, request_seconds=[wall / 51] * 51)
                cold = int(repeat == 0)
                if mode == 'typed':
                    row.update(preparation_seconds=float(cold),
                        runtime_delta=dict(read_bytes=100*cold, read_ranges=cold,
                            payload_read_calls=cold, decode_count=cold, decoded_bytes=100*cold),
                        server_seconds=dict(server_total=wall-1, publish_route=wall-2,
                            window_execute=wall-3, window_assemble=wall-4, ipc_encode=.5),
                        metrics=dict(requests=51, samples=102, preparation_seconds=float(cold),
                                     request_timings=[{}]*51, ipc_bytes=4096))
                else:
                    row.update(raw_window_seconds=wall-1, normalization_seconds=1,
                        metrics=dict(read_calls=cold, read_ranges=cold, read_bytes=100*cold,
                            batch_decodes=cold, decoded_bytes=100*cold, read_windows_calls=51))
                rows.append(row)
    return rows


class LayoutSystemSummaryTest(unittest.TestCase):
    def test_paired_factorial_and_warm_repetition_collapse(self):
        result = summarize(observations(), repetitions=2)
        self.assertEqual(result['observations'], 14 * 2 * 4)
        group = next(g for g in result['groups'] if
                     (g['layout'],g['mode'],g['temperature']) == ('original','local','warm'))
        self.assertEqual(group['raw_observations'], 6)
        self.assertEqual(group['independent_repetitions'], 2)
        self.assertEqual(group['metrics']['wall_seconds'],
                         dict(count=2, mean=17., min=16., max=18., by_repetition=[16.,18.]))
        effects = {e['name']: e for e in result['factorial']['cold']}
        self.assertEqual(effects['local_layout_saving_seconds_A_minus_B']['by_repetition'], [8,8])
        self.assertEqual(effects['iotdb_layout_saving_seconds_C_minus_D']['by_repetition'], [10,10])
        self.assertEqual(effects['interaction_seconds_C_minus_D_minus_A_plus_B']['mean'], 2)
        # Mean of paired ratios differs from ratio of grand means.
        self.assertAlmostEqual(effects['local_layout_speedup_A_over_B']['mean'], (12/4+14/6)/2)
        self.assertEqual(len(result['auxiliary']['cold']), 13)

    def test_nested_timers_are_reported_without_summing(self):
        result = summarize(observations(), repetitions=2)
        group = next(g for g in result['groups'] if
                     (g['layout'],g['mode'],g['temperature']) == ('original','typed','cold'))
        self.assertEqual(group['metrics']['server_seconds.server_total']['mean'], 19)
        self.assertEqual(group['metrics']['preparation_seconds']['mean'], 1)
        self.assertEqual(group['metrics']['wall_seconds']['mean'], 20)
        self.assertNotIn('total_overhead_seconds', group['metrics'])

    def test_rejects_missing_duplicate_failed_or_bad_passes(self):
        rows = observations()
        for bad in [rows[:-1], rows+[copy.deepcopy(rows[0])]]:
            with self.assertRaises(ValueError): summarize(bad, repetitions=2)
        for key, value in [('passed', False), ('temperature','warm'), ('repetition', 2),
                           ('pass_index', -1), ('wall_seconds', float('nan'))]:
            bad = copy.deepcopy(rows); bad[0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                summarize(bad, repetitions=2)

    def test_rejects_nonwarm_cache_and_repeated_preparation(self):
        rows = observations()
        for mode, section, field in [('local','metrics','read_bytes'),
                                     ('iotdb','metrics','batch_decodes'),
                                     ('typed','runtime_delta','decode_count')]:
            bad = copy.deepcopy(rows)
            item = next(r for r in bad if r['mode']==mode and r['pass_index']==1)
            item[section][field] = 1
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, 'warm pass'):
                summarize(bad, repetitions=2)
        bad = copy.deepcopy(rows)
        item = next(r for r in bad if r['mode']=='typed' and r['pass_index']==1)
        item['preparation_seconds'] = item['metrics']['preparation_seconds'] = .1
        with self.assertRaisesRegex(ValueError, 'repeated preparation'):
            summarize(bad, repetitions=2)

    def test_preparation_profiles_join_by_id_and_remain_separate(self):
        rows = observations()
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            for row in rows:
                if row['mode'] != 'typed':
                    continue
                row['tag'] = f"r{row['repetition']}-{row['layout']}-typed"
                identity = row['tag'] + '-prepared'
                row['metrics']['preparation_timings'] = ([dict(prepared_id=identity)]
                                                        if row['pass_index'] == 0 else [])
                record = dict(protocol='iotdbfs-query-profile/1', pid=10, sequence=1,
                    steady_start_ns=0, steady_end_ns=900000000, server_total_ns=900000000,
                    ok=True, operation='window_prepare', prepared_id=identity,
                    stages_ns=dict(coverage_index_prepare=400000000, module_prepare=300000000,
                                   coverage_span_build=350000000), runtime_delta={})
                (directory/(row['tag']+'-profile.jsonl')).write_text(json.dumps(record)+'\n')
            enriched = enrich_preparation_profiles(rows, directory)
            self.assertNotIn('preparation_server_seconds', rows[0])
            cold = next(r for r in enriched if r['mode']=='typed' and r['pass_index']==0)
            warm = next(r for r in enriched if r['mode']=='typed' and r['pass_index']==1)
            self.assertEqual(cold['preparation_server_seconds']['server_total'], .9)
            self.assertEqual(cold['preparation_server_seconds']['prepare_total'], .7)
            self.assertEqual(cold['preparation_server_seconds']['coverage_span_build'], .35)
            self.assertTrue(all(v == 0 for v in warm['preparation_server_seconds'].values()))
            result = summarize(enriched, repetitions=2)
            group = next(g for g in result['groups'] if
                         (g['layout'],g['mode'],g['temperature']) == ('original','typed','cold'))
            self.assertEqual(group['metrics']['preparation_server_seconds.server_total']['mean'], .9)
            self.assertEqual(group['metrics']['server_seconds.server_total']['mean'], 19)
            cold = next(r for r in rows if r['mode']=='typed' and r['pass_index']==0)
            cold['metrics']['preparation_timings'][0]['prepared_id'] = 'wrong'
            with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
                enrich_preparation_profiles(rows, directory)
            cold['metrics']['preparation_timings'] = []
            with self.assertRaisesRegex(ValueError, 'exactly one cold'):
                enrich_preparation_profiles(rows, directory)

    def test_requires_exact_trace_and_consistent_timer_boundary(self):
        for field, value in [('request_seconds', [1]*50), ('raw_window_seconds', 100.)]:
            bad = observations(); bad[0][field] = value
            with self.assertRaises(ValueError): summarize(bad, repetitions=2)
        bad = observations()
        item = next(r for r in bad if r['mode']=='typed')
        item['metrics']['samples'] = 101
        with self.assertRaisesRegex(ValueError, 'request/sample count'):
            summarize(bad, repetitions=2)


if __name__ == '__main__':
    unittest.main()
