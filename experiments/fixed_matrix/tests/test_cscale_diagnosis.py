"""Offline scientific protocol checks; no network, service or mount actions."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
import unittest
import tempfile
from unittest.mock import patch

from experiments.fixed_matrix import cscale_diagnosis as m
from experiments.fixed_matrix.tests.test_shared_candidates import resource

REPO = Path(__file__).resolve().parents[3]
PROBE_SHA = 'b'*64
EXPECTED = [dict(binding=dict(path='/one', version=11), unit='dataset:/data/u1'),
            dict(binding=dict(path='/two', version=22), unit='dataset:/data/u2')]
LOCATORS = [dict(path='/one', version=11, dataset='/data/u1'), dict(path='/two', version=22, dataset='/data/u2')]


def row(c, campaign, cfg, repeat):
    d = 100 if c == 128 else 420
    diagnostic = cfg == 'B'
    gid = f'FM1-hdf5-N512-V1-C{c}-S0-base-latest'
    counters = {'hot_query.domain_hits': 1, 'hot_query.index_lookups': 1, 'hot_query.proof_hits': 2,
                'dataset_catalog_selected_sources': c, 'dataset_catalog_candidate_datasets': 4*c}
    stages = {}
    if diagnostic:
        counters.update(m.FORMAL_COUNTERS)
        counters.update({'cdiag.scope_paths': c, 'cdiag.selector.validated_paths': 2*c})
        counters.update({'cdiag.calls.'+k: v for k, v in m.FORMAL_CALLS.items()})
        counters.update({k: 1 for k in m.FORMAL_POSITIVE})
        stages = {prefix+k: 0 for k in m.FORMAL_CALLS for prefix in ('cdiag.self.', 'cdiag.total.')}
        stages.update({'cdiag.total.serve': 80, 'cdiag.self.serve': 80})
    return dict(group_id=gid, C=c, N=512, campaign=campaign, configuration=cfg, repeat=repeat,
        pair_position=m.scope_order(campaign, repeat).index(c), arm='IoTDB-FS',
        phase='warmup' if repeat < m.WARMUPS else 'measurement',
        corpus_id='same-corpus', case_id='case-'+str(c), oracle_sha256='oracle-'+str(c),
        run_id=f'{c}-{campaign}-{cfg}', product_sha256=PROBE_SHA if diagnostic else m.BASELINE_SHA,
        resource_id='resource', ok=True, discovery_ns=d,
        client_components=dict(request_build_ns=10, call_ns=d-20, row_materialize_ns=5, scope_validation_ns=5) if diagnostic else None,
        oracle_verified=True, value_queries_executed=0, selected_units=2,
        discovered_rows=deepcopy(LOCATORS), discovery_response=dict(ok=True, datasets=[
            dict(path=r['path'], version=r['version'], dataset_path=r['dataset']) for r in LOCATORS]),
        discovery_call=dict(server_profile=dict(ok=True, sequence=repeat+1, counters=counters, stages_ns=stages,
            server_total_ns=90, iotdb_delta={k: 0 for k in ('query_calls', 'version_store_query_calls',
                'metadata_query_calls', 'payload_calls', 'layout_calls')})),
        resource_before=resource(1), resource_after=resource(2))


def science():
    bindings = {}
    rows = []
    for g in m.groups(REPO):
        c = g['C_paths']
        bindings[g['group_id']] = dict(scope=['/one', '/two'], expected_matches=EXPECTED,
            corpus_id='same-corpus', case_id='case-'+str(c), oracle_sha256='oracle-'+str(c),
            run_ids={str(campaign): {cfg: f'{c}-{campaign}-{cfg}' for cfg in m.CONFIGURATIONS} for campaign in m.CAMPAIGNS})
    for campaign in m.CAMPAIGNS:
        for cfg in m.runner.configuration_order(campaign):
            for repeat in range(m.WARMUPS+m.MEASURED):
                for c in m.scope_order(campaign, repeat):
                    rows.append(row(c, campaign, cfg, repeat))
    return rows, bindings


class CScaleDiagnosisTests(unittest.TestCase):
    def analyze(self, mutate=None):
        rows, bindings = science()
        if mutate:
            mutate(rows)
        return m.analyze_rows(rows, bindings, m.WARMUPS, m.MEASURED, m.BASELINE_SHA, PROBE_SHA, 'resource')

    def test_sealed_cases_same_n_and_fixed_counts(self):
        groups = m.groups(REPO)
        self.assertEqual([(g['N'], g['C_paths'], g['V_stored'], g['D'], g['K_units'], g['selection']) for g in groups],
                         [(512, 128, 1, 4, 2, 'latest'), (512, 512, 1, 4, 2, 'latest')])
        self.assertEqual(m.runner.expected_rows(SimpleNamespace()), 560)
        rows, _ = science()
        self.assertEqual((len(rows), sum(r['phase'] == 'warmup' for r in rows)), (560, 80))
        with self.assertRaises(RuntimeError):
            m.runner.groups(REPO, 'n512')

    def test_products_and_scopes_balanced_without_extra_mounts(self):
        self.assertEqual([m.runner.configuration_order(c) for c in m.CAMPAIGNS],
                         [('A', 'B'), ('B', 'A'), ('B', 'A'), ('A', 'B')])
        self.assertEqual([m.scope_order(c, 0) for c in m.CAMPAIGNS],
                         [(128, 512), (512, 128), (512, 128), (128, 512)])
        for campaign in m.CAMPAIGNS:
            positions = [m.scope_order(campaign, repeat) for repeat in range(5, 35)]
            self.assertEqual(positions.count((128, 512)), 15)
            self.assertEqual(positions.count((512, 128)), 15)

    def test_pairwise_growth_and_normalization_not_phase1_product_effect(self):
        summary = self.analyze()
        self.assertEqual(summary['query_rows'], 560)
        for cfg in summary['configurations']:
            self.assertAlmostEqual(cfg['growth'], 4.2)
            self.assertAlmostEqual(cfg['normalized_growth'], 1.05)
            self.assertEqual(len(cfg['blocks']), 4)
        self.assertEqual(summary['configurations'][1]['role'], 'diagnostic attribution only')
        self.assertNotIn('effects', summary)

    def test_warmup_never_enters_measurement_and_slow_blocks_kept(self):
        def warm(rows):
            for r in rows:
                if r['phase'] == 'warmup':
                    r['discovery_ns'] *= 100
                    if r['client_components'] is not None:
                        r['client_components'] = {k: v*100 for k, v in r['client_components'].items()}
        self.assertAlmostEqual(self.analyze(warm)['configurations'][0]['growth'], 4.2)
        def slow(rows):
            for r in rows:
                if r['campaign'] == 3 and r['C'] == 512 and r['configuration'] == 'A':
                    r['discovery_ns'] *= 100
        summary = self.analyze(slow)
        self.assertAlmostEqual(summary['configurations'][0]['blocks'][3]['growth'], 420)

    def test_rows_cannot_be_missing_duplicated_or_rescheduled(self):
        for mutate in (lambda r: r.pop(), lambda r: r.append(deepcopy(r[0])),
                       lambda r: r[0].update(pair_position=1), lambda r: r[0].update(phase='measurement'),
                       lambda r: r[0].update(N=128), lambda r: r[0].update(resource_id='other')):
            with self.assertRaises(RuntimeError):
                self.analyze(mutate)

    def test_no_probe_clocks_on_clean_or_missing_probe_clocks(self):
        for mutate in (lambda r: r[0].update(client_components=dict(request_build_ns=10, call_ns=80,
                                                                row_materialize_ns=5, scope_validation_ns=5)),
                       lambda r: next(v for v in r if v['configuration'] == 'B').update(client_components=None)):
            with self.assertRaises(RuntimeError):
                self.analyze(mutate)

    def test_oracle_raw_response_and_no_values_are_required(self):
        def raw(rows):
            rows[0]['discovery_response']['datasets'][0]['dataset_path'] = '/bad'
        def locator(rows):
            rows[0]['discovered_rows'][0]['version'] += 1
        for mutate in (raw, locator, lambda r: r[0].update(value_queries_executed=1),
                       lambda r: r[0].update(oracle_verified=False), lambda r: r[0].update(product_sha256='other')):
            with self.assertRaises(RuntimeError):
                self.analyze(mutate)

    def test_discovery_clean_has_exact_two_outer_clocks(self):
        response = dict(ok=True, datasets=[dict(path=r['path'], version=r['version'], dataset_path=r['dataset']) for r in LOCATORS])
        stat = dict(control_ns=90, ipc_bytes=0, response_path=None)
        with patch.object(m.client, 'call', return_value=(response, [], stat)), \
             patch.object(m.client, 'profiles', return_value=[dict(ok=True, sequence=1)]), \
             patch.object(m.time, 'perf_counter_ns', side_effect=[100, 200]) as clock:
            value = m.discovery_query({}, ['/one', '/two'], {'kind': 'latest'}, EXPECTED)
        self.assertEqual(clock.call_count, 2)
        self.assertEqual(value['discovery_ns'], 100)
        self.assertIsNone(value['client_components'])
        m.validate_discovery_row(value, ['/one', '/two'], EXPECTED)

    def test_discovery_probe_components_cover_exact_outer_clock(self):
        response = dict(ok=True, datasets=[dict(path=r['path'], version=r['version'], dataset_path=r['dataset']) for r in LOCATORS])
        with patch.object(m.client, 'call', return_value=(response, [], dict(control_ns=90, ipc_bytes=0, response_path=None))), \
             patch.object(m.client, 'profiles', return_value=[dict(ok=True, sequence=1)]), \
             patch.object(m.time, 'perf_counter_ns', side_effect=[100, 110, 190, 195, 200]):
            value = m.discovery_query({}, ['/one', '/two'], {'kind': 'latest'}, EXPECTED, diagnostic=True)
        self.assertEqual(value['client_components'], dict(request_build_ns=10, call_ns=80,
                                                        row_materialize_ns=5, scope_validation_ns=5))
        m.validate_discovery_row(value, ['/one', '/two'], EXPECTED)

    def test_execution_closure_includes_nested_launcher_and_new_sources(self):
        with patch.object(m.run, 'closure', return_value={'base.py': 'sha'}):
            closure = m.runner.closure(REPO)
        self.assertIn('experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py', closure)
        self.assertIn('experiments/cross_version/cscale_diagnosis/README.md', closure)
        self.assertIn('experiments/cross_version/cscale_diagnosis/run.sh', closure)
        self.assertIn('experiments/fixed_matrix/tests/test_cscale_diagnosis.py', closure)

    def test_absent_profile_metric_is_not_zero_or_fully_observed(self):
        rows = [row(128, 0, 'B', 5), row(128, 0, 'B', 6)]
        rows[0]['discovery_call']['server_profile']['stages_ns'] = {'diagnostic.test': 12}
        summary = m.profile_summary(rows)
        self.assertEqual(summary['stages_ns']['diagnostic.test'],
                         dict(observed_rows=1, total_rows=2, median=12, minimum=12, maximum=12))
        self.assertNotIn('missing', summary['stages_ns'])

    def test_explicit_same_baseline_instrumentation_source_gate(self):
        sources_a = {name: 'old' for name in m.PRODUCT_DIFF}
        overlay = dict(protocol=m.PROTOCOL, baseline_sha256=m.BASELINE_SHA,
                       instrumentation_only=True, performance_claim=False, changed_product_files=sorted(m.PRODUCT_DIFF))
        a = dict(source_files=sources_a, original_receipt='/original-receipt',
                 original_receipt_sha256=m.ORIGINAL_RECEIPT_SHA, metadata_augmentation_only=True,
                 binary_sha256=m.BASELINE_SHA, unit_binary_sha256='unit',
                 focused_tests='focused', all_unit_tests='all', focused_tests_sha256='focused', all_unit_tests_sha256='all')
        b = dict(source_files={n: 'instrumented' for n in m.PRODUCT_DIFF}, diagnostic_overlay=overlay,
                 focused_tests='focused', all_unit_tests='all')
        args = SimpleNamespace(baseline_binary=Path('/A'), binary=Path('/B'),
                               baseline_build_binding=Path('/binding-A'), build_binding=Path('/binding-B'))
        def sha(path):
            if path == Path('/A'):
                return m.BASELINE_SHA
            return m.ORIGINAL_RECEIPT_SHA if path == Path('/original-receipt') else PROBE_SHA
        def read(path):
            return a if path in (Path('/binding-A'), Path('/original-receipt')) else b
        xml = m.ET.ElementTree(m.ET.fromstring('<testsuites tests="1"><testcase status="run" result="completed" /></testsuites>'))
        with patch.object(m, 'verify_build_binding', return_value={'complete': True}), \
             patch.object(m, 'compare_build_environment', return_value={'same': True}), \
             patch.object(m, 'file_sha', side_effect=sha), patch.object(m, 'read_json', side_effect=read), \
             patch.object(m.ET, 'parse', return_value=xml):
            proof = m.runner.verify_candidate(args)
            self.assertEqual(proof['changed_product_files'], sorted(m.PRODUCT_DIFF))
            for key, value in (('baseline_sha256', 'other'), ('instrumentation_only', False),
                               ('performance_claim', True), ('changed_product_files', ['include/a.h'])):
                old = b['diagnostic_overlay'][key]
                b['diagnostic_overlay'][key] = value
                with self.assertRaises(RuntimeError):
                    m.runner.verify_candidate(args)
                b['diagnostic_overlay'][key] = old
            b['source_files']['src/new.cpp'] = 'new'
            b['diagnostic_overlay']['changed_product_files'].append('src/new.cpp')
            with self.assertRaises(RuntimeError):
                m.runner.verify_candidate(args)

    def test_same_namespace_native_binding_and_nested_scope_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = SimpleNamespace(work=root/'new-run', source_collection=root, repo=REPO)
            entry = dict(corpus_id='same', corpus_key='same', source_root='same', head='same',
                         journal='same', journal_digest='same', namespace='same', native_root='same')
            cases = [dict(entry, group_id='small'), dict(entry, group_id='large')]
            proof = dict(cases=cases)
            scopes = {name: dict(scope=[str(i) for i in range(count)], physical_versions={'kind': 'latest'})
                      for name, count in (('small', 128), ('large', 512))}
            with patch.object(m.NativeABRunner, 'source_proof', return_value=proof), \
                 patch.object(m, 'read_json', return_value=scopes):
                self.assertTrue(m.runner.source_proof(args)['same_mount_for_scopes'])
                cases[1]['namespace'] = 'different'
                with self.assertRaises(RuntimeError):
                    m.runner.source_proof(args)
                cases[1]['namespace'] = 'same'
                scopes['small']['scope'][0] = 'absent'
                with self.assertRaises(RuntimeError):
                    m.runner.source_proof(args)

    def test_preflight_and_verify_are_inherited_zero_query_methods(self):
        self.assertIs(m.CScaleRunner.prepare_only, m.NativeABRunner.prepare_only)
        self.assertIs(m.CScaleRunner.verify_prepared, m.NativeABRunner.verify_prepared)
        self.assertIs(m.CScaleRunner.supervise, m.NativeABRunner.supervise)

    def test_fee_capture_uses_exact_real_batches_and_event_references(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'fees.json'
            value = dict(protocol='iotdbfs-cscale-real-fees/2', candidate_paths=128, identity_encoding='latin1-bytes',
                prelude_fees=[['actual/head', 140]], prelude_consumed_bytes=140,
                batches=[dict(id=i, phase=phase, fees=[[f'{phase}/one', 12], [f'{phase}/two', 24]])
                         for i, phase in enumerate(('fee_selection', 'fee_manifest', 'fee_catalog', 'proof_replay'))],
                events=[dict(batch_id=i, phase=phase) for i, phase in enumerate(('fee_selection', 'fee_manifest',
                         'fee_catalog', 'proof_replay'))]+[dict(batch_id=3, phase='proof_replay')])
            path.write_text(json.dumps(value))
            receipt = m.verify_fee_trace(path, 128)
            self.assertTrue(receipt['complete'])
            self.assertEqual(receipt['batches'][0]['entries'], 2)
            self.assertEqual(receipt['batches'][0]['sum_fee_values_raw'], 36)
            for change in (lambda v: v.update(candidate_paths=512),
                           lambda v: v['events'][0].update(batch_id=8),
                           lambda v: v['batches'][0]['fees'].append(['fee_selection/one', 12]),
                           lambda v: v['batches'].pop()):
                malformed = deepcopy(value)
                change(malformed)
                path.write_text(json.dumps(malformed))
                with self.assertRaises(RuntimeError):
                    m.verify_fee_trace(path, 128)

    def test_formal_probe_counters_are_explicit_and_exact(self):
        sample = row(128, 0, 'B', 5)
        m.validate_formal_profile(sample, 128, 'B')
        for key in (*m.FORMAL_COUNTERS, *m.FORMAL_POSITIVE, 'cdiag.calls.selector_validate',
                    'cdiag.scope_paths', 'cdiag.selector.validated_paths'):
            changed = deepcopy(sample)
            changed['discovery_call']['server_profile']['counters'].pop(key)
            with self.assertRaises(RuntimeError):
                m.validate_formal_profile(changed, 128, 'B')
        for key, bad in (('cdiag.trace_exports', 1), ('cdiag.capture_events', 4),
                         ('cdiag.fee.active_collector', 1), ('cdiag.fee.memo_hits', 2),
                         ('cdiag.budget.post_first_node_delta', 1), ('cdiag.budget.post_first_charge_delta', 1),
                         ('cdiag.prelude_capture_errors', 1), ('cdiag.fee.ledger_nodes_before_first', 0),
                         ('cdiag.fee.consumed_before_first', 0)):
            changed = deepcopy(sample)
            changed['discovery_call']['server_profile']['counters'][key] = bad
            with self.assertRaises(RuntimeError):
                m.validate_formal_profile(changed, 128, 'B')

    def test_formal_profiles_require_hot_hits_sql_zero_and_exclusive_closure(self):
        sample = row(512, 0, 'B', 5)
        changes = [lambda p: p['stages_ns'].update({'cdiag.self.serve': 79}),
                   lambda p: p.update(server_total_ns=79),
                   lambda p: p['stages_ns'].pop('cdiag.total.fee_ledger'),
                   lambda p: p['counters'].update({'hot_query.proof_hits': 1}),
                   lambda p: p['iotdb_delta'].update(query_calls=1),
                   lambda p: p['iotdb_delta'].pop('metadata_query_calls')]
        for change in changes:
            changed = deepcopy(sample)
            change(changed['discovery_call']['server_profile'])
            with self.assertRaises(RuntimeError):
                m.validate_formal_profile(changed, 512, 'B')
        clean = row(512, 0, 'A', 5)
        m.validate_formal_profile(clean, 512, 'A')
        clean['discovery_call']['server_profile']['iotdb_delta']['layout_calls'] = 1
        with self.assertRaises(RuntimeError):
            m.validate_formal_profile(clean, 512, 'A')

    def test_real_fee_union_uses_max_identity_and_excludes_nonbyte_domains(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'fees.json'
            batches = [dict(id=i, phase=p, fees=[['same', 10+i]]) for i, p in enumerate(
                ('fee_selection', 'fee_manifest', 'fee_catalog', 'proof_replay'))]
            batches[0]['fees'] += [['v04-payload-bytes/file', 1000], ['v04-extents/file', 2], ['zero', 0]]
            value = dict(protocol='iotdbfs-cscale-real-fees/2', candidate_paths=128, identity_encoding='latin1-bytes', batches=batches,
                prelude_fees=[['actual/head', 140]], prelude_consumed_bytes=140,
                events=[dict(batch_id=i, phase=b['phase']) for i, b in enumerate(batches)]+[dict(batch_id=3, phase='proof_replay')])
            path.write_text(json.dumps(value))
            actual = m.verify_fee_trace(path, 128)['expected_formal_fee_counters']
            self.assertEqual(actual, {'cdiag.fee.visited_entries': 7, 'cdiag.fee.inserted_nodes': 3,
                                     'cdiag.fee.ledger_nodes_final': 4, 'cdiag.fee.consumed_bytes_final': 153,
                                     'cdiag.fee.ledger_nodes_before_first': 1, 'cdiag.fee.consumed_before_first': 140})

    def test_fee_identity_high_bytes_and_nul_are_lossless_latin1(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'fees.json'
            key = b'file/\x80\xff\x00/source'.decode('latin1')
            batches = [dict(id=i, phase=p, fees=[[key, 10+i]]) for i, p in enumerate(
                ('fee_selection', 'fee_manifest', 'fee_catalog', 'proof_replay'))]
            value = dict(protocol='iotdbfs-cscale-real-fees/2', candidate_paths=512,
                prelude_fees=[['actual/head', 140]], prelude_consumed_bytes=140,
                identity_encoding='latin1-bytes', batches=batches,
                events=[dict(batch_id=i, phase=b['phase']) for i, b in enumerate(batches)]+[dict(batch_id=3, phase='proof_replay')])
            path.write_text(json.dumps(value, ensure_ascii=True))
            self.assertIn('\\u0080\\u00ff\\u0000', path.read_text())
            receipt = m.verify_fee_trace(path, 512)
            self.assertEqual([b['raw_identity_bytes'] for b in receipt['batches']], [len(key)]*4)
            self.assertEqual(receipt['expected_formal_fee_counters']['cdiag.fee.ledger_nodes_final'], 2)
            self.assertEqual(receipt['expected_formal_fee_counters']['cdiag.fee.consumed_bytes_final'], 153)
            for change in (lambda v: v.pop('identity_encoding'),
                           lambda v: v.update(identity_encoding='utf8'),
                           lambda v: v['batches'][0]['fees'][0].__setitem__(0, '\u0100'),
                           lambda v: v['batches'][0]['fees'][0].__setitem__(0, '\ud800')):
                invalid = deepcopy(value)
                change(invalid)
                path.write_text(json.dumps(invalid, ensure_ascii=True))
                with self.assertRaises(RuntimeError):
                    m.verify_fee_trace(path, 512)

    def test_real_prelude_is_replayed_first_not_invented_or_reinserted(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'fees.json'
            head = b'owner.view/\x80\x00'.decode('latin1')
            batches = [dict(id=i, phase=p, fees=[[head, 140], ['new', 10+i]]) for i,p in enumerate(
                ('fee_selection', 'fee_manifest', 'fee_catalog', 'proof_replay'))]
            value = dict(protocol='iotdbfs-cscale-real-fees/2', candidate_paths=128,
                identity_encoding='latin1-bytes', prelude_fees=[[head,140]], prelude_consumed_bytes=140,
                batches=batches, events=[dict(batch_id=i, phase=b['phase']) for i,b in enumerate(batches)]+
                    [dict(batch_id=3, phase='proof_replay')])
            path.write_text(json.dumps(value,ensure_ascii=True))
            receipt = m.verify_fee_trace(path,128)
            self.assertEqual(receipt['expected_formal_fee_counters'], {
                'cdiag.fee.ledger_nodes_before_first':1, 'cdiag.fee.consumed_before_first':140,
                'cdiag.fee.visited_entries':8, 'cdiag.fee.inserted_nodes':1,
                'cdiag.fee.ledger_nodes_final':2, 'cdiag.fee.consumed_bytes_final':153})
            self.assertEqual(receipt['prelude']['raw_identity_bytes'],len(head))
            changes = [lambda v:v.pop('prelude_fees'), lambda v:v.update(prelude_consumed_bytes=0),
                       lambda v:v.update(prelude_consumed_bytes=141),
                       lambda v:v['prelude_fees'][0].__setitem__(1,0),
                       lambda v:v['prelude_fees'].append([head,140]),
                       lambda v:v['prelude_fees'][0].__setitem__(0,'v04-payload-bytes/file'),
                       lambda v:v['prelude_fees'][0].__setitem__(0,'v04-extents/file'),
                       lambda v:v['prelude_fees'][0].__setitem__(0,'\u0100'),
                       lambda v:v.update(protocol='iotdbfs-cscale-real-fees/1')]
            for change in changes:
                invalid = deepcopy(value)
                change(invalid)
                path.write_text(json.dumps(invalid,ensure_ascii=True))
                with self.assertRaises(RuntimeError):
                    m.verify_fee_trace(path,128)


if __name__ == '__main__':
    unittest.main()
