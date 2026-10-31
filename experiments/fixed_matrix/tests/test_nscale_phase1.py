"""Offline protocol fixtures: no server, SSH, database, mount, or cgroups."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from experiments.fixed_matrix import nscale_phase1 as definition
from experiments.fixed_matrix import hdf5_native_ab as engine
from experiments.fixed_matrix.tests.test_shared_candidates import resource, row as native_row

m = definition.runner
REPO = Path(__file__).resolve().parents[3]
CANDIDATE = hashlib.sha256(b'offline nscale candidate').hexdigest()


def row(group):
    value = native_row(group, 'A')
    if group['format'] == 'hdf5':
        value['discovery_call'] = dict(server_profile=dict(counters={'registration_attempts': 1}))
        value['value_calls'][0]['server_profile']['counters'].update(registration_attempts=3, typed_batch_items=2)
    return value


def science(tier):
    selected = m.groups(REPO, tier)
    bindings, rows = {}, []
    for group in selected:
        gid = group['group_id']
        bound = dict(corpus_id='corpus-'+gid, case_id='case-'+gid, oracle_sha256='oracle-'+gid, run_ids={})
        bindings[gid] = bound
        for campaign in engine.CAMPAIGNS:
            for config in engine.CONFIGS:
                bound['run_ids'].setdefault(str(campaign), {})[config] = {a: gid+str(campaign)+config+a for a in engine.ARMS}
                for arm in engine.ARMS:
                    for repeat in range(engine.REPEATS):
                        value = row(group) if arm == 'IoTDB-FS' else dict(oracle_verified=True, mode_verified=True, selected_units=2)
                        d, q = (40, 60) if arm == 'FS-index' else ((200, 200) if config == 'A' else (100, 200))
                        value.update(group_id=gid, corpus_id=bound['corpus_id'], case_id=bound['case_id'], oracle_sha256=bound['oracle_sha256'],
                            campaign=campaign, configuration=config, arm=arm, repeat=repeat,
                            run_id=bound['run_ids'][str(campaign)][config][arm], phase='warmup' if repeat < 5 else 'measurement',
                            ok=True, resource_id='resource', owned_output_bytes=group['owned_output_bytes'],
                            product_sha256=definition.BASELINE_SHA if config == 'A' else CANDIDATE,
                            discovery_ns=d, value_ns=q, total_ns=d+q, resource_before=resource(1), resource_after=resource(2))
                        rows.append(value)
    return selected, bindings, rows


class NScalePhase1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def analyze(self, tier, change=None):
        groups, bound, rows = science(tier)
        if change:
            change(rows)
        path = self.root/'rows.jsonl'
        path.write_text(''.join(json.dumps(v)+'\n' for v in rows))
        return m.analyze(path, bound, groups, 'resource', CANDIDATE)

    def test_exact_six_existing_independent_cases_and_1440_rows(self):
        total = warm = measured = 0
        for tier, n in (('n8', 8), ('n128', 128), ('n512', 512)):
            selected, _, rows = science(tier)
            self.assertEqual([g['format'] for g in selected], ['grib', 'hdf5'])
            for g in selected:
                self.assertEqual((g['N'], g['C_paths'], g['V_stored'], g['D'], g['K_units'], g['size_profile'], g['selection']),
                                 (n, n, 1, 4, 2, 'S0', 'latest'))
                self.assertEqual(m.campaigns(g), (0, 1, 2, 3))
            self.assertEqual(m.expected_rows(SimpleNamespace(repo=REPO, tier=tier)), 480)
            total += len(rows)
            warm += sum(r['phase'] == 'warmup' for r in rows)
            measured += sum(r['phase'] == 'measurement' for r in rows)
        self.assertEqual((total, warm, measured), (1440, 480, 960))
        self.assertEqual([m.configuration_order(c) for c in engine.CAMPAIGNS],
                         [('A', 'B'), ('B', 'A'), ('B', 'A'), ('A', 'B')])

    def test_protocol_is_immutable_and_old_entries_unchanged(self):
        from experiments.fixed_matrix import batch_parse, hdf5_native_cpu, shared_candidates
        before = [(v.PROTOCOL, v.PRODUCT_DIFF.copy()) for v in (batch_parse, hdf5_native_cpu, shared_candidates)]
        with self.assertRaises(FrozenInstanceError):
            definition.SPEC.protocol = 'changed'
        other = definition.NScaleRunner(replace(definition.SPEC, protocol='offline-independent/1'))
        self.assertEqual(other.spec.protocol, 'offline-independent/1')
        self.assertEqual(m.spec.protocol, 'iotdbfs-fixed-nscale-phase1/1')
        self.assertEqual(before, [(v.PROTOCOL, v.PRODUCT_DIFF) for v in (batch_parse, hdf5_native_cpu, shared_candidates)])

    def test_execution_closure_pins_nested_native_filesystem_launcher(self):
        relative = 'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py'
        with patch.object(definition.run, 'closure', return_value={'base.py': 'sha'}):
            closure = m.closure(REPO)
        self.assertEqual(closure[relative], file_sha(REPO/relative))
        self.assertEqual(closure['base.py'], 'sha')
        self.assertIn('experiments/cross_version/nscale_phase1/run.sh', closure)
        self.assertIn('experiments/cross_version/nscale_phase1/README.md', closure)

    def test_exact_four_campaign_statistics_for_both_formats_all_tiers(self):
        for tier in definition.TIERS:
            result = self.analyze(tier)
            self.assertEqual((result['query_rows'], len(result['effects'])), (480, 2))
            for effect in result['effects']:
                self.assertEqual(len(effect['blocks']), 4)
                self.assertAlmostEqual(effect['R_B_over_A'], .75)
                self.assertAlmostEqual(effect['raw_IoTDB_T_B_over_A'], .75)
                self.assertAlmostEqual(effect['Rd_B_over_A'], .5)
                self.assertAlmostEqual(effect['Rq_B_over_A'], 1)

    def test_warmups_excluded_and_slow_measurements_retained(self):
        def warm(rows):
            for r in rows:
                if r['repeat'] < 5:
                    for key in ('discovery_ns', 'value_ns', 'total_ns'):
                        r[key] *= 10000
        for effect in self.analyze('n512', warm)['effects']:
            self.assertAlmostEqual(effect['R_B_over_A'], .75)
        def slow(rows):
            for r in rows:
                if r['campaign'] == 3 and r['configuration'] == 'B' and r['arm'] == 'IoTDB-FS':
                    for key in ('discovery_ns', 'value_ns', 'total_ns'):
                        r[key] *= 100
        result = self.analyze('n512', slow)
        for effect in result['effects']:
            self.assertGreater(effect['blocks'][-1]['R_B_over_A'], 1)

    def test_complete_science_clock_resource_and_dynamic_candidate_are_required(self):
        changes = [lambda r: r.pop(), lambda r: r.append(deepcopy(r[0])),
            lambda r: r[0].update(oracle_sha256='other'), lambda r: r[0].update(product_sha256='other'),
            lambda r: r[0].update(total_ns=1), lambda r: r[0].update(resource_id='other'),
            lambda r: r[0].update(phase='measurement'),
            lambda r: r[0]['resource_after']['cgroups']['parent']['files'].update({'cpuset.cpus.partition': 'isolated'})]
        for change in changes:
            with self.assertRaises(RuntimeError):
                self.analyze('n512', change)

    def test_modes_are_equal_for_both_products_without_new_mechanism_counters(self):
        for group in m.groups(REPO, 'n8'):
            for config in engine.CONFIGS:
                m.validate_row(row(group), group, config)
            value = row(group)
            value['execution_mode'] = 'other'
            with self.assertRaises(RuntimeError):
                m.validate_row(value, group, 'B')
        group = m.groups(REPO, 'n8')[1]
        for key, val in (('registration_attempts', 1), ('typed_batch_items', 1)):
            value = row(group)
            value['value_calls'][0]['server_profile']['counters'][key] = val
            with self.assertRaises(RuntimeError):
                m.validate_row(value, group, 'B')

    def gate(self, tier):
        return dict(protocol=definition.PROTOCOL, tier=tier, query_records=480, product_sha256=CANDIDATE,
            baseline_sha256=definition.BASELINE_SHA,
            **{k: True for k in ('complete', 'scientific_complete', 'ready_for_analysis', 'original_service_restored',
                                'resources_restored', 'controller_restoration_verified')})

    def test_each_tier_gate_requires_complete_same_ab_and_restoration(self):
        for tier in ('n8', 'n128'):
            gate = self.gate(tier)
            m.validate_tier_gate(gate, tier, definition.BASELINE_SHA, CANDIDATE)
            for key, val in (('tier', 'n512'), ('query_records', 240), ('product_sha256', 'other'),
                             ('baseline_sha256', 'other'), ('complete', False), ('resources_restored', False),
                             ('controller_restoration_verified', False), ('protocol', 'other')):
                broken = dict(gate, **{key: val})
                with self.assertRaises(RuntimeError):
                    m.validate_tier_gate(broken, tier, definition.BASELINE_SHA, CANDIDATE)

    def gate_fixture(self, tier):
        prior = self.root/'prior'
        prior.mkdir(exist_ok=True)
        proof = dict(source_collection='sealed-source', source_pins={'report': 'sha'}, current_collection='native-source',
            current_location_files={'locations': 'sha'}, build_gate={'candidate_sha': CANDIDATE}, resource_identity={'cpus': [2, 3]},
            product={'path': '/build/B', 'sha256': CANDIDATE}, baseline={'path': '/build/A', 'sha256': definition.BASELINE_SHA})
        closure = {'runner.py': 'source-sha'}
        (prior/'prepared.json').write_text(json.dumps(dict(sources=closure, product=proof['product'], baseline=proof['baseline'])))
        (prior/'reuse-source.json').write_text(json.dumps(proof))
        gate = self.gate('n8' if tier == 'n128' else 'n128')
        for name, key in (('queries.jsonl', 'queries_sha256'), ('bindings.json', 'bindings_sha256'), ('analysis.json', 'analysis_sha256')):
            (prior/name).write_text('original '+name)
            gate[key] = file_sha(prior/name)
        (prior/'report.json').write_text(json.dumps(gate))
        args = SimpleNamespace(tier=tier, gate_report=prior/'report.json', repo=REPO)
        return args, proof, closure

    def test_scale_gate_checks_preceding_source_execution_closure_and_artifact_hashes(self):
        for tier in ('n128', 'n512'):
            args, proof, closure = self.gate_fixture(tier)
            with patch.object(m, 'closure', return_value=closure):
                m.validate_gate_artifacts(args, proof)
                for key in ('source_pins', 'current_location_files', 'build_gate', 'resource_identity', 'product'):
                    broken = deepcopy(proof)
                    broken[key]['other'] = 'other'
                    with self.assertRaises(RuntimeError):
                        m.validate_gate_artifacts(args, broken)
                for artifact in ('queries.jsonl', 'bindings.json', 'analysis.json'):
                    path = args.gate_report.parent/artifact
                    original = path.read_text()
                    path.write_text('tampered')
                    with self.assertRaises(RuntimeError):
                        m.validate_gate_artifacts(args, proof)
                    path.write_text(original)
            with patch.object(m, 'closure', return_value={'runner.py': 'changed'}):
                with self.assertRaises(RuntimeError):
                    m.validate_gate_artifacts(args, proof)

    def test_n512_cannot_skip_n128_and_n8_cannot_reuse_a_gate(self):
        args, proof, closure = self.gate_fixture('n512')
        path = args.gate_report
        gate = json.loads(path.read_text())
        gate['tier'] = 'n8'
        path.write_text(json.dumps(gate))
        with patch.object(m, 'closure', return_value=closure):
            with self.assertRaises(RuntimeError):
                m.validate_gate_artifacts(args, proof)
        with self.assertRaises(RuntimeError):
            m.validate_gate_artifacts(SimpleNamespace(tier='n8', gate_report=path), proof)
        with self.assertRaises(RuntimeError):
            m.validate_gate_artifacts(SimpleNamespace(tier='n512', gate_report=None), proof)

    def test_disk_preflight_keeps_one_percent_before_scientific_or_service_work(self):
        args = SimpleNamespace(work=self.root/'new-work')
        with patch.object(definition.shutil, 'disk_usage', return_value=SimpleNamespace(total=1000, free=9)):
            with patch.object(engine.NativeABRunner, 'source_proof') as parent:
                with self.assertRaisesRegex(RuntimeError, '1%'):
                    m.source_proof(args)
                parent.assert_not_called()

    def test_grib_launcher_and_hdf5_native_launcher_keep_original_settings(self):
        grib, hdf5 = m.groups(REPO, 'n8')
        with patch.object(definition.run.infrastructure, 'launcher', return_value={'grib': True}) as grib_launch:
            self.assertEqual(m.launcher('path', 'binary', 'sha', 'namespace', 'journal', {'group': grib}), {'grib': True})
            grib_launch.assert_called_once_with('path', 'binary', 'sha', 'namespace', 'journal')
        with patch.object(engine.NativeABRunner, 'launcher', return_value={'native': True}) as native_launch:
            self.assertEqual(m.launcher('path', 'binary', 'sha', 'namespace', 'journal', {'group': hdf5}), {'native': True})
            native_launch.assert_called_once()

    def build_fixture(self):
        a, b = self.root/'A', self.root/'B'
        a.write_bytes(b'accepted baseline')
        b.write_bytes(b'actual new candidate')
        paths = {key: self.root/(key+'.xml') for key in ('focused_tests', 'all_unit_tests')}
        for path in paths.values():
            root = ET.Element('testsuites', tests=str(len(definition.REQUIRED_REGRESSION_TESTS)))
            suite = ET.SubElement(root, 'testsuite', tests=str(len(definition.REQUIRED_REGRESSION_TESTS)))
            for name, case in definition.REQUIRED_REGRESSION_TESTS:
                ET.SubElement(suite, 'testcase', classname=name, name=case, status='run', result='completed')
            ET.ElementTree(root).write(path)
        old_xml = self.root/'baseline.xml'
        old_xml.write_text('<testsuites tests="1"><testsuite tests="1"><testcase classname="Old" name="Accepted" status="run" result="completed"/></testsuite></testsuites>')
        sources = {name: 'old source' for name in definition.PRODUCT_DIFF}
        sources['src/unchanged.cpp'] = 'unchanged'
        changed = {name: 'new source' if name in definition.PRODUCT_DIFF else value for name, value in sources.items()}
        receipt_a, receipt_b = self.root/'A.json', self.root/'B.json'
        receipt_a.write_text(json.dumps(dict(source_files=sources, focused_tests=str(old_xml), all_unit_tests=str(old_xml))))
        receipt_b.write_text(json.dumps(dict(source_files=changed, **{key: str(path) for key, path in paths.items()})))
        args = SimpleNamespace(baseline_binary=a, binary=b, baseline_build_binding=receipt_a, build_binding=receipt_b)
        return args, paths

    def test_standard_binding_verifies_both_products_and_candidate_sha_is_dynamic(self):
        args, _ = self.build_fixture()
        original_sha = engine.file_sha
        def sha(path):
            return definition.BASELINE_SHA if Path(path) == args.baseline_binary else original_sha(path)
        with patch.object(engine, 'file_sha', side_effect=sha), \
             patch.object(engine, 'verify_build_binding', return_value={'complete': True}) as binding, \
             patch.object(engine, 'compare_build_environment', return_value={'same': True}) as environment:
            proof = m.verify_candidate(args)
            self.assertEqual(proof['candidate_sha256'], original_sha(args.binary))
            self.assertEqual(proof['changed_product_files'], sorted(definition.PRODUCT_DIFF))
            self.assertEqual(binding.call_count, 2)
            self.assertEqual([c.args[0].binary for c in binding.call_args_list], [args.baseline_binary, args.binary])
            environment.assert_called_once_with(args.baseline_binary, args.binary)

    def test_candidate_requires_only_approved_product_diff_and_all_unique_completed_regressions(self):
        args, paths = self.build_fixture()
        original_sha = engine.file_sha
        def sha(path):
            return definition.BASELINE_SHA if Path(path) == args.baseline_binary else original_sha(path)
        with patch.object(engine, 'file_sha', side_effect=sha), \
             patch.object(engine, 'verify_build_binding', return_value={'complete': True}), \
             patch.object(engine, 'compare_build_environment', return_value={'same': True}):
            original = args.build_binding.read_text()
            source_a = json.loads(args.baseline_build_binding.read_text())['source_files']
            for name in definition.PRODUCT_DIFF:
                receipt = json.loads(original)
                receipt['source_files'][name] = source_a[name]
                args.build_binding.write_text(json.dumps(receipt))
                with self.assertRaisesRegex(RuntimeError, 'outside approved'):
                    m.verify_candidate(args)
            receipt = json.loads(original)
            receipt['source_files']['src/unchanged.cpp'] = 'unapproved'
            args.build_binding.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(RuntimeError, 'outside approved'):
                m.verify_candidate(args)
            args.build_binding.write_text(original)
            for path in paths.values():
                xml = path.read_text()
                for kind in ('missing', 'duplicate', 'skipped', 'incomplete'):
                    tree = ET.fromstring(xml)
                    suite = tree.find('testsuite')
                    first = suite.find('testcase')
                    if kind == 'missing':
                        suite.remove(first)
                        tree.set('tests', str(len(definition.REQUIRED_REGRESSION_TESTS)-1))
                        suite.set('tests', tree.get('tests'))
                    elif kind == 'duplicate':
                        suite.append(deepcopy(first))
                        tree.set('tests', str(len(definition.REQUIRED_REGRESSION_TESTS)+1))
                        suite.set('tests', tree.get('tests'))
                    elif kind == 'skipped':
                        ET.SubElement(first, 'skipped')
                    else:
                        first.set('result', 'suppressed')
                    ET.ElementTree(tree).write(path)
                    with self.assertRaises(RuntimeError):
                        m.verify_candidate(args)
                path.write_text(xml)

    def test_prepare_and_verify_preserve_zero_query_roundtrip_gate(self):
        work = self.root/'preflight'
        args = SimpleNamespace(repo=REPO, work=work, tier='n8')
        proof, closure = {'protocol': definition.PROTOCOL, 'complete': True}, {'runner.py': 'sha'}
        def bootstrap(given):
            (given.work/'bootstrap.json').write_text(json.dumps(dict(complete=True, native_loaded=True,
                service_modified=False, query_executed=False)))
        with patch.object(definition.run, 'bootstrap', side_effect=bootstrap), patch.object(m, 'closure', return_value=closure):
            m.prepare_only(args, proof)
            m.verify_prepared(args, proof)
            with self.assertRaises(RuntimeError):
                m.verify_prepared(args, dict(proof, changed=True))
        with patch.object(m, 'closure', return_value={'runner.py': 'changed'}):
            with self.assertRaises(RuntimeError):
                m.verify_prepared(args, proof)


# SHA helper intentionally uses the same artifact identity implementation as production.
from experiments.identity.registry import file_sha

if __name__ == '__main__':
    unittest.main()
