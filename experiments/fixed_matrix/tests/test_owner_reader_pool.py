"""Offline protocol tests: no database, mounts, SSH, service or cgroups."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from experiments.fixed_matrix import owner_reader_pool as m
from experiments.fixed_matrix import hdf5_native_ab as engine
from experiments.fixed_matrix.tests.test_shared_candidates import resource, row as original_row
from experiments.identity.registry import file_sha

REPO = Path(__file__).resolve().parents[3]
OLD = hashlib.sha256(b'original single reader').hexdigest()
CANDIDATE = hashlib.sha256(b'new reader pool candidate').hexdigest()


def runner():
    return m.OwnerReaderPoolRunner(engine.ProtocolSpec(m.PROTOCOL, 'experiments.fixed_matrix.owner_reader_pool',
        'offline', OLD, frozenset({'src/storage/versioning/IoTDBVersionStore.cpp'}), m.REQUIRED_TESTS, ()))


def science(groups, *, qualification=False):
    bindings = {}; rows = []
    for g in groups:
        gid = g['group_id']
        b = dict(corpus_id='corpus-'+gid, case_id='case-'+gid, oracle_sha256='oracle-'+gid, run_ids={})
        bindings[gid] = b
        for campaign in ((0,) if qualification else m.CAMPAIGNS):
            for config in m.CONFIGS:
                b['run_ids'].setdefault(str(campaign), {})[config] = {a: gid+str(campaign)+config+a for a in m.ARMS}
                for arm in m.ARMS:
                    for repeat in range(1 if qualification else m.REPEATS):
                        row = original_row(g, 'B') if arm == 'IoTDB-FS' else dict(oracle_verified=True,
                            mode_verified=True, selected_units=2)
                        if arm == 'IoTDB-FS':
                            if g['format'] == 'hdf5':
                                row['discovery_call'] = dict(server_profile=dict(operation='dataset_catalog'))
                            else:
                                for call in row['value_calls']:
                                    call['server_profile']['operation'] = 'typed'
                        d, q = (40, 60) if arm == 'FS-index' else ({'OLD': 200, 'P1': 150, 'P2': 100, 'P4': 50}[config], 200)
                        row.update(group_id=gid, corpus_id=b['corpus_id'], case_id=b['case_id'], oracle_sha256=b['oracle_sha256'],
                            campaign=campaign, configuration=config, reader_pool_size=m.POOLS[config], arm=arm, repeat=repeat,
                            run_id=b['run_ids'][str(campaign)][config][arm],
                            phase='qualification' if qualification else 'warmup' if repeat < 2 else 'measurement',
                            ok=True, resource_id='resource', owned_output_bytes=g['owned_output_bytes'],
                            product_sha256=OLD if config == 'OLD' else CANDIDATE,
                            discovery_ns=d, value_ns=q, total_ns=d+q, resource_before=resource(1), resource_after=resource(2))
                        rows.append(row)
    return bindings, rows


class OwnerReaderPoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.runner = runner()

    def write_rows(self, path, rows):
        path.write_text(''.join(json.dumps(row)+'\n' for row in rows))

    def analysis(self, mutation=None, qualification_mutation=None):
        selected = self.runner.groups(REPO, 'n128'); bindings, rows = science(selected)
        qgroups = m.selected_groups(REPO, 8); qbindings, qualification = science(qgroups, qualification=True)
        bindings.update(qbindings)
        if mutation:
            mutation(rows)
        if qualification_mutation:
            qualification_mutation(qualification)
        self.write_rows(self.root/'queries.jsonl', rows)
        self.write_rows(self.root/'qualification.jsonl', qualification)
        (self.root/'qualification-report.json').write_text(json.dumps(dict(complete=True,
            query_records=16, groups=qgroups, queries_sha256=file_sha(self.root/'qualification.jsonl'))))
        with patch.object(m.run, 'validate_coverage', return_value={'complete': True}) as coverage:
            result = self.runner.analyze(self.root/'queries.jsonl', bindings, selected, 'resource', CANDIDATE)
            self.assertEqual(len(coverage.call_args.args[1]), 16)
        return result

    def test_exact_groups_576_formal_and_16_qualification_rows(self):
        groups = self.runner.groups(REPO, 'n128')
        self.assertEqual([g['format'] for g in groups], ['grib', 'hdf5'])
        self.assertEqual(self.runner.expected_rows(SimpleNamespace(repo=REPO, tier='n128')), 576)
        _, rows = science(groups)
        self.assertEqual((len(rows), sum(r['phase'] == 'warmup' for r in rows),
                          sum(r['phase'] == 'measurement' for r in rows)), (576, 96, 480))
        _, qualification = science(m.selected_groups(REPO, 8), qualification=True)
        self.assertEqual(len(qualification), 16)
        self.assertTrue(all(row['phase'] == 'qualification' for row in qualification))
        for tier in ('n8', 'n512'):
            with self.assertRaises(RuntimeError):
                self.runner.groups(REPO, tier)
        self.assertEqual([m.configuration_order(c) for c in m.CAMPAIGNS],
            [('OLD', 'P1', 'P2', 'P4'), ('P1', 'P2', 'P4', 'OLD'), ('P4', 'P2', 'P1', 'OLD')])

    def test_reuses_real_supervisor_without_changing_older_protocol(self):
        self.assertIs(m.OwnerReaderPoolRunner.supervise, engine.NativeABRunner.supervise)
        self.assertIs(m.OwnerReaderPoolRunner.prepare_only, engine.NativeABRunner.prepare_only)
        self.assertIs(m.OwnerReaderPoolRunner.verify_prepared, engine.NativeABRunner.verify_prepared)
        self.assertEqual(engine.CONFIGS, ('A', 'B'))
        self.assertEqual(engine.CAMPAIGNS, (0, 1, 2, 3))
        self.assertEqual(engine.WARMUPS, 5)

    def test_launcher_changes_only_candidate_pool_and_retains_hdf_native_cache(self):
        binary = self.root/'iotdb_fs'; binary.write_bytes(b'fake executable')
        native = self.root/'native'; native.mkdir()
        for fmt in ('grib', 'hdf5'):
            plan = dict(group=dict(format=fmt), native_root=native)
            receipts = {}
            for config in m.CONFIGS:
                receipt = self.runner.launcher(self.root/(fmt+config), binary, file_sha(binary),
                    'vd_offline', self.root/'owner', plan, config)
                self.assertEqual(file_sha(Path(receipt['path'])), receipt['sha256'])
                receipts[config] = receipt
                if config == 'OLD':
                    self.assertNotIn(m.POOL_KEY, receipt['settings'])
                else:
                    self.assertEqual(receipt['settings'][m.POOL_KEY], str(m.POOLS[config]))
                if fmt == 'hdf5':
                    self.assertEqual(receipt['settings']['query.hdf5.mode'], 'native-hdf5')
                    self.assertEqual(receipt['settings']['query.hdf5.native_cache'], 'true')
                without_pool = dict(receipt['settings']); without_pool.pop(m.POOL_KEY, None)
                self.assertEqual(without_pool, receipts['OLD']['settings'])

    def test_full_query_calls_keep_original_discovery_and_serial_batch_boundaries(self):
        for group in self.runner.groups(REPO, 'n128'):
            _, rows = science([group])
            row = deepcopy(next(r for r in rows if r['arm'] == 'IoTDB-FS'))
            self.runner.validate_row(row, group, 'OLD')
            row['discovery_call']['server_profile']['operation'] = 'format_catalog_by_selector'
            with self.assertRaises(RuntimeError):
                self.runner.validate_row(row, group, 'P4')
            _, rows = science([group]); row = deepcopy(rows[0])
            row['value_calls'][0]['server_profile']['operation'] = 'query_template_execute'
            with self.assertRaises(RuntimeError):
                self.runner.validate_row(row, group, 'P1')

    def test_campaign_medians_and_comparisons_against_old_and_p1_are_separate(self):
        result = self.analysis()
        self.assertEqual((result['query_rows'], result['qualification_rows'], len(result['cells'])), (576, 16, 8))
        self.assertEqual(len(result['effects']), 12)
        for g in self.runner.groups(REPO, 'n128'):
            effect = next(e for e in result['effects'] if e['group_id'] == g['group_id'] and
                e['configuration'] == 'P2' and e['reference'] == 'OLD')
            self.assertEqual(len(effect['blocks']), 3)
            self.assertAlmostEqual(effect['R_over_reference'], .75)
            self.assertAlmostEqual(effect['IoTDB-FS_T_over_reference'], .75)
            self.assertAlmostEqual(effect['Rd_over_reference'], .5)
            self.assertAlmostEqual(effect['Rq_over_reference'], 1)
            own = next(e for e in result['effects'] if e['group_id'] == g['group_id'] and
                e['configuration'] == 'P2' and e['reference'] == 'P1')
            self.assertAlmostEqual(own['IoTDB-FS_T_over_reference'], 300/350)

    def test_warmups_are_excluded_but_all_slow_formal_values_remain(self):
        def warm(rows):
            for row in rows:
                if row['phase'] == 'warmup':
                    for key in ('total_ns', 'discovery_ns', 'value_ns'):
                        row[key] *= 10000
        result = self.analysis(warm)
        self.assertAlmostEqual(next(e for e in result['effects'] if e['configuration'] == 'P2' and
            e['reference'] == 'OLD')['R_over_reference'], .75)
        def slow(rows):
            for row in rows:
                if row['campaign'] == 2 and row['configuration'] == 'P2' and row['arm'] == 'IoTDB-FS':
                    for key in ('total_ns', 'discovery_ns', 'value_ns'):
                        row[key] *= 100
        result = self.analysis(slow)
        effect = next(e for e in result['effects'] if e['configuration'] == 'P2' and e['reference'] == 'OLD')
        self.assertAlmostEqual(effect['blocks'][-1]['R_over_reference'], 75)

    def test_missing_duplicate_wrong_identity_output_clock_budget_or_resource_is_rejected(self):
        changes = [lambda r: r.pop(), lambda r: r.append(deepcopy(r[0])),
            lambda r: r[0].update(oracle_sha256='other'), lambda r: r[0].update(product_sha256=CANDIDATE),
            lambda r: r[0].update(reader_pool_size=4), lambda r: r[0].update(owned_output_bytes=1),
            lambda r: r[0].update(total_ns=1), lambda r: r[0].update(phase='measurement'),
            lambda r: r[0].update(resource_id='other'),
            lambda r: r[0]['resource_after']['cgroups']['query']['files'].update({'memory.max': 'max'}),
            lambda r: r[0]['resource_after']['cgroups']['query']['files'].update({'memory.events': 'oom 1\noom_kill 0'})]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.analysis(change)
        with self.assertRaises(RuntimeError):
            self.analysis(qualification_mutation=lambda r: r.pop())

    def test_missing_or_changed_qualification_gate_cannot_authorize_formal_analysis(self):
        groups = self.runner.groups(REPO, 'n128'); bindings, rows = science(groups)
        self.write_rows(self.root/'queries.jsonl', rows)
        with self.assertRaises(FileNotFoundError):
            self.runner.analyze(self.root/'queries.jsonl', bindings, groups, 'resource', CANDIDATE)
        self.analysis()
        with (self.root/'qualification.jsonl').open('a') as out:
            out.write('\n')
        with self.assertRaises(RuntimeError):
            self.runner.analyze(self.root/'queries.jsonl', bindings, groups, 'resource', CANDIDATE)

    def test_worker_finishes_all_n8_checks_before_formal_collection_and_stops_on_failure(self):
        for fail_qualification in (False, True):
            with self.subTest(fail_qualification=fail_qualification):
                work = self.root/('failure' if fail_qualification else 'success'); work.mkdir()
                binary = work/'binary'; binary.write_bytes(b'new reader pool candidate')
                args = SimpleNamespace(repo=REPO, work=work, tier='n128', binary=binary)
                formal_bindings, formal = science(self.runner.groups(REPO, 'n128'))
                qbindings, qualified_rows = science(m.selected_groups(REPO, 8), qualification=True)
                bindings = dict(formal_bindings, **qbindings)
                (work/'bindings.json').write_text(json.dumps(bindings))
                frozen = dict(sources={'runner.py': 'sha'}, resource_id='resource', product={'sha256': CANDIDATE})
                (work/'prepared.json').write_text(json.dumps(frozen))
                (work/'reuse-source.json').write_text('{}')
                calls = []
                def collect(args, execution, bindings, registry, frozen, report, env, paired, *, qualification=False):
                    calls.append('qualification' if qualification else 'formal')
                    if not qualification:
                        gate = json.loads((work/'qualification-report.json').read_text())
                        self.assertTrue(gate['complete']); self.assertEqual(gate['query_records'], 16)
                    rows = qualified_rows if qualification else formal
                    self.write_rows(work/('qualification.jsonl' if qualification else 'queries.jsonl'), rows)
                    return 15 if qualification and fail_qualification else len(rows)
                paired = SimpleNamespace(sf=object())
                startup = SimpleNamespace(adjust_mount=lambda *args: None)
                with patch.object(self.runner, 'source_proof', return_value={}), \
                     patch.object(self.runner, 'closure', return_value=frozen['sources']), \
                     patch.object(self.runner, 'plans', return_value=([], bindings)), \
                     patch.object(self.runner, 'collect', side_effect=collect), \
                     patch.object(m.run, 'dependencies', return_value=(paired, None, {}, startup)), \
                     patch.object(m.signal, 'signal'):
                    if fail_qualification:
                        with self.assertRaisesRegex(RuntimeError, 'N8 qualification row count'):
                            self.runner.worker(args)
                        self.assertEqual(calls, ['qualification'])
                        self.assertFalse((work/'queries.jsonl').exists())
                    else:
                        self.runner.worker(args)
                        self.assertEqual(calls, ['qualification', 'formal'])
                        self.assertTrue(json.loads((work/'report.json').read_text())['complete'])

    def binding_fixture(self):
        repo = self.root/'repo'; stage = self.root/'frozen'; source = stage/'source'; oldsource = stage/'oldsource'
        (repo/m.WORK).mkdir(parents=True); source.mkdir(parents=True); oldsource.mkdir()
        for name in m.PRODUCT_DIFF:
            product = source/name; product.parent.mkdir(parents=True, exist_ok=True); product.write_text('new source '+name+'\n')
            if name != 'include/iotdbfs/storage/versioning/detail/BoundedReaderPool.h':
                oldfile = oldsource/name; oldfile.parent.mkdir(parents=True, exist_ok=True); oldfile.write_text('old source '+name+'\n')
        runtime_source = source/'tests/unit/storage/versioning/test_ReaderPoolRuntime.cpp'
        runtime_source.parent.mkdir(parents=True); runtime_source.write_text('frozen runtime regression source\n')
        product = source/'src/storage/versioning/IoTDBVersionStore.cpp'
        sources = {str(p.relative_to(source)): file_sha(p) for p in source.rglob('*') if p.is_file()}
        old_sources = {str(p.relative_to(oldsource)): file_sha(p) for p in oldsource.rglob('*') if p.is_file()}
        manifest = stage/'source.json'; manifest.write_text(json.dumps(sources))
        old = stage/'old-build/bin/iotdb_fs'; old.parent.mkdir(parents=True); old.write_bytes(b'original single reader')
        candidate = stage/'candidate-build/bin/iotdb_fs'; candidate.parent.mkdir(parents=True); candidate.write_bytes(b'new reader pool candidate')
        compiler = stage/'compiler'; compiler.write_bytes(b'same compiler')
        for name in ('arrow', 'hdf5', 'sdk', 'prefix'):
            (stage/name).mkdir()
        cache_text = ('CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_CXX_COMPILER:FILEPATH='+str(compiler)+
            '\nCMAKE_CXX_FLAGS:STRING=\nCMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG\n'+
            'Arrow_DIR:PATH='+str(stage/'arrow')+'\nHDF5_DIR:PATH='+str(stage/'hdf5')+'\n'+
            'CMAKE_PREFIX_PATH:PATH='+str(stage/'prefix')+'\nIOTDB_ROOT:PATH='+str(stage/'sdk')+'\n')
        for binary, root in ((old, oldsource), (candidate, source)):
            (binary.parents[1]/'CMakeCache.txt').write_text(cache_text+'CMAKE_HOME_DIRECTORY:INTERNAL='+str(root)+'\n')
            target = binary.parents[1]/'CMakeFiles/iotdb_fs.dir'; target.mkdir(parents=True)
            (target/'flags.make').write_text('CXX_DEFINES = -DEXAMPLE\nCXX_INCLUDES = -I'+str(root/'include')+'\nCXX_FLAGS = -O3 -DNDEBUG -std=c++17\n')
            (target/'link.txt').write_text(str(compiler)+' -O3 -DNDEBUG CMakeFiles/iotdb_fs.dir/src/main.cpp.o -o bin/iotdb_fs libiotdb_fs_core.a -lfuse\n')
        root = ET.Element('testsuites', tests='1923', failures='0', errors='0', disabled='0', skipped='0')
        suite = ET.SubElement(root, 'testsuite', tests='1923')
        names = list(m.REQUIRED_TESTS)+[('Other', 'Test'+str(i)) for i in range(1923-len(m.REQUIRED_TESTS))]
        for classname, name in names:
            ET.SubElement(suite, 'testcase', classname=classname, name=name, status='run', result='completed')
        unit = stage/'unit.xml'; ET.ElementTree(root).write(unit)
        oldunit = stage/'old-unit.xml'; oldunit.write_bytes(unit.read_bytes())
        oldunit_binary = old.parent/'unit_tests'; oldunit_binary.write_bytes(b'old test executable')
        unit_binary = candidate.parent/'unit_tests'; unit_binary.write_bytes(b'new test executable')
        oldreceipt = stage/'old-build-binding.json'
        oldreceipt.write_text(json.dumps(dict(complete=True, full_clean_build=True, binary=str(old), binary_sha256=file_sha(old),
            source_root=str(oldsource), source_files=old_sources, focused_tests=str(oldunit), all_unit_tests=str(oldunit),
            focused_tests_sha256=file_sha(oldunit), all_unit_tests_sha256=file_sha(oldunit),
            unit_binary=str(oldunit_binary), unit_binary_sha256=file_sha(oldunit_binary))))
        binding = dict(complete=True, baseline_binary=str(old), baseline_sha256=file_sha(old),
            candidate_binary=str(candidate), candidate_sha256=file_sha(candidate), candidate_source_root=str(source),
            source_manifest_path=str(manifest), unit_tests_path=str(unit), unit_tests_sha256=file_sha(unit), source_files=sources,
            baseline_build_receipt=str(oldreceipt), baseline_build_receipt_sha256=file_sha(oldreceipt),
            baseline_source_root=str(oldsource), baseline_unit_tests=str(oldunit), baseline_unit_tests_sha256=file_sha(oldunit),
            candidate_unit_binary=str(unit_binary), candidate_unit_binary_sha256=file_sha(unit_binary),
            candidate_build_cache_sha256=file_sha(candidate.parents[1]/'CMakeCache.txt'), product_diff=sorted(m.PRODUCT_DIFF))
        supplemental = stage/'supplemental-runtime'; supplemental.mkdir()
        testcopy = supplemental/runtime_source.name; testcopy.write_bytes(runtime_source.read_bytes())
        core = candidate.parents[1]/'libiotdb_fs_core.a'; core.write_bytes(b'frozen candidate core archive')
        runtime_binary = supplemental/'runtime_pool_tests'; runtime_binary.write_bytes(b'supplemental linux test executable')
        runtime_xml = supplemental/'runtime-pool-tests.xml'
        rtroot = ET.Element('testsuites', tests='3', failures='0', errors='0', skipped='0')
        rtsuite = ET.SubElement(rtroot, 'testsuite', tests='3')
        for classname, name in m.RUNTIME_TESTS:
            ET.SubElement(rtsuite, 'testcase', classname=classname, name=name, status='run', result='completed')
        ET.ElementTree(rtroot).write(runtime_xml)
        command = [str(compiler), '-std=c++17', '-O3', '-DNDEBUG', '-pthread', '-I'+str(source/'include'),
                   str(testcopy), str(core), '-lgtest_main', '-lgtest', '-lcrypto', '-o', str(runtime_binary)]
        command_path = supplemental/'compile-command.json'; command_path.write_text(json.dumps(command))
        logs = {}
        for name in ('compile.stdout', 'compile.stderr', 'run.stdout', 'run.stderr'):
            (supplemental/name).write_text('preserved log'); logs[name] = file_sha(supplemental/name)
        runtime_receipt = dict(complete=True, platform='Linux', product_unchanged=True, tests=3,
            candidate_sha256=file_sha(candidate), candidate_source_root=str(source), source_manifest_sha256=file_sha(manifest),
            source=str(testcopy), source_sha256=file_sha(testcopy), frozen_test_source=str(runtime_source),
            frozen_test_source_sha256=file_sha(runtime_source), core_archive=str(core), core_archive_sha256=file_sha(core),
            compiler=str(compiler), compiler_sha256=file_sha(compiler), binary=str(runtime_binary), binary_sha256=file_sha(runtime_binary),
            xml=str(runtime_xml), xml_sha256=file_sha(runtime_xml), compile_command=command, compile_command_path=str(command_path),
            compile_command_sha256=file_sha(command_path), logs=logs,
            protected_sha256={str(candidate): file_sha(candidate), str(unit_binary): file_sha(unit_binary), str(core): file_sha(core)})
        runtime_receipt_path = supplemental/'receipt.json'; runtime_receipt_path.write_text(json.dumps(runtime_receipt))
        binding.update(supplemental_runtime_receipt=str(runtime_receipt_path), supplemental_runtime_receipt_sha256=file_sha(runtime_receipt_path))
        binding_path = repo/m.WORK/'protocol-binding.json'; binding_path.write_text(json.dumps(binding))
        (repo/m.WORK/'base-product-audit.json').write_text(json.dumps(dict(complete=True,
            binary_sha256=dict(baseline_binary=file_sha(old)))))
        args = SimpleNamespace(repo=repo, baseline_binary=old, binary=candidate)
        # The dependency repo deliberately differs from the frozen source.
        (repo/'src').mkdir(); (repo/'src'/'pool.cpp').write_text('unrelated checkout\n')
        library_patch = patch.object(m, 'resolved_libraries', return_value={'libexample.so.1': dict(path='/same/library', sha256='fixed')})
        library_patch.start(); self.addCleanup(library_patch.stop)
        return args, binding, binding_path, product, unit

    def test_build_gate_verifies_independent_candidate_source_and_all_1923_tests(self):
        args, binding, path, product, unit = self.binding_fixture()
        result = self.runner.verify_candidate(args)
        self.assertEqual(result['unit_tests'], 1923)
        self.assertEqual(result['candidate_sha256'], CANDIDATE)
        cache = product.parent/'__pycache__'; cache.mkdir(); (cache/'ignored.pyc').write_bytes(b'cache')
        self.assertEqual(self.runner.verify_candidate(args)['unit_tests'], 1923)
        product.write_text('changed source\n')
        with self.assertRaisesRegex(RuntimeError, 'candidate source changed'):
            self.runner.verify_candidate(args)

    def test_build_gate_rejects_skips_missing_regressions_extra_sources_and_binary_drift(self):
        args, binding, path, product, unit = self.binding_fixture()
        original = unit.read_bytes(); tree = ET.parse(unit); root = tree.getroot()
        root.set('skipped', '1'); tree.write(unit)
        with self.assertRaisesRegex(RuntimeError, 'unit XML'):
            self.runner.verify_candidate(args)
        unit.write_bytes(original); tree = ET.parse(unit)
        next(tree.getroot().iter('testcase')).set('name', 'missing'); tree.write(unit)
        binding['unit_tests_sha256'] = file_sha(unit); path.write_text(json.dumps(binding))
        with self.assertRaisesRegex(RuntimeError, 'regression'):
            self.runner.verify_candidate(args)
        unit.write_bytes(original); binding['unit_tests_sha256'] = file_sha(unit); path.write_text(json.dumps(binding))
        extra = product.parent/'extra.cpp'; extra.write_text('extra')
        with self.assertRaisesRegex(RuntimeError, 'exact frozen'):
            self.runner.verify_candidate(args)
        extra.unlink(); args.binary.write_bytes(b'changed binary')
        with self.assertRaisesRegex(RuntimeError, 'candidate_binary'):
            self.runner.verify_candidate(args)

    def test_old_product_audit_must_be_complete_and_bound_to_exact_old_binary(self):
        args, binding, path, product, unit = self.binding_fixture()
        audit = args.repo/m.WORK/'base-product-audit.json'
        for value in (dict(complete=False, binary_sha256={'baseline_binary': OLD}),
                      dict(complete=True, binary_sha256={'baseline_binary': CANDIDATE})):
            audit.write_text(json.dumps(value))
            with self.assertRaisesRegex(RuntimeError, 'product audit'):
                self.runner.verify_candidate(args)

    def test_old_source_receipt_and_xml_drift_are_rejected(self):
        for changed in ('source', 'receipt', 'xml', 'unit_binary'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                self.root = Path(temp).resolve()
                args, binding, _, _, _ = self.binding_fixture()
                receipt_path = Path(binding['baseline_build_receipt'])
                receipt = json.loads(receipt_path.read_text())
                if changed == 'source':
                    (Path(receipt['source_root'])/'src/main.cpp').write_text('unapproved old source')
                elif changed == 'receipt':
                    receipt_path.write_text(receipt_path.read_text()+'\n')
                elif changed == 'xml':
                    Path(receipt['all_unit_tests']).write_text('<testsuites tests="1"/>')
                else:
                    Path(receipt['unit_binary']).write_bytes(b'changed old tests')
                with self.assertRaises(RuntimeError):
                    self.runner.verify_candidate(args)

    def test_old_skip_is_rejected_even_when_all_artifact_digests_match(self):
        args, binding, path, _, _ = self.binding_fixture()
        oldreceipt_path = Path(binding['baseline_build_receipt'])
        oldreceipt = json.loads(oldreceipt_path.read_text()); oldunit = Path(oldreceipt['all_unit_tests'])
        tree = ET.parse(oldunit); tree.getroot().set('skipped', '1'); tree.write(oldunit)
        for key in ('focused_tests_sha256', 'all_unit_tests_sha256'):
            oldreceipt[key] = file_sha(oldunit)
        oldreceipt_path.write_text(json.dumps(oldreceipt))
        binding.update(baseline_build_receipt_sha256=file_sha(oldreceipt_path), baseline_unit_tests_sha256=file_sha(oldunit))
        path.write_text(json.dumps(binding))
        with self.assertRaisesRegex(RuntimeError, 'OLD focused/full gate'):
            self.runner.verify_candidate(args)

    def test_extra_product_changes_cannot_be_authorized_by_self_consistent_binding(self):
        args, binding, path, _, _ = self.binding_fixture()
        source = Path(binding['candidate_source_root']); extra = source/'src/unapproved.cpp'; extra.write_text('extra change')
        binding['source_files']['src/unapproved.cpp'] = file_sha(extra)
        Path(binding['source_manifest_path']).write_text(json.dumps(binding['source_files']))
        binding['product_diff'].append('src/unapproved.cpp'); path.write_text(json.dumps(binding))
        with self.assertRaisesRegex(RuntimeError, 'approved seven pool files'):
            self.runner.verify_candidate(args)

    def test_candidate_binary_tests_source_root_and_release_environment_remain_bound(self):
        for changed in ('unit_binary', 'cache', 'flags', 'source_root'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                self.root = Path(temp).resolve()
                args, binding, path, _, _ = self.binding_fixture()
                cache = args.binary.parents[1]/'CMakeCache.txt'
                if changed == 'unit_binary':
                    Path(binding['candidate_unit_binary']).write_bytes(b'changed candidate test executable')
                elif changed == 'cache':
                    cache.write_text(cache.read_text()+'# unrelated cache mutation\n')
                elif changed == 'flags':
                    cache.write_text(cache.read_text().replace('-O3 -DNDEBUG', '-O0 -g'))
                    binding['candidate_build_cache_sha256'] = file_sha(cache)
                else:
                    cache.write_text(cache.read_text().replace('CMAKE_HOME_DIRECTORY:INTERNAL='+binding['candidate_source_root'],
                        'CMAKE_HOME_DIRECTORY:INTERNAL='+binding['baseline_source_root']))
                    binding['candidate_build_cache_sha256'] = file_sha(cache)
                path.write_text(json.dumps(binding))
                with self.assertRaises(RuntimeError):
                    self.runner.verify_candidate(args)

    def test_search_prefix_metadata_difference_requires_actual_target_and_library_equality(self):
        args, binding, path, _, _ = self.binding_fixture()
        cache = args.binary.parents[1]/'CMakeCache.txt'
        cache.write_text('\n'.join(line for line in cache.read_text().splitlines()
                                  if not line.startswith('CMAKE_PREFIX_PATH:'))+'\n')
        binding['candidate_build_cache_sha256'] = file_sha(cache); path.write_text(json.dumps(binding))
        result = self.runner.verify_candidate(args)['build_environment']
        self.assertTrue(result['actual_target_and_libraries_equal'])
        self.assertIsNone(result['metadata_differences']['search_prefix']['candidate'])
        self.assertTrue(result['metadata_differences']['search_prefix']['OLD'])
        target = args.binary.parents[1]/'CMakeFiles/iotdb_fs.dir'
        flags = target/'flags.make'; original_flags = flags.read_text()
        flags.write_text(original_flags.replace('-O3', '-O0'))
        with self.assertRaisesRegex(RuntimeError, 'actual iotdb_fs target'):
            self.runner.verify_candidate(args)
        flags.write_text(original_flags)
        link = target/'link.txt'; link.write_text(link.read_text().replace('-lfuse', '-lfuse -lother'))
        with self.assertRaisesRegex(RuntimeError, 'actual iotdb_fs target'):
            self.runner.verify_candidate(args)

    def test_ninja_shell_wrapper_and_make_link_normalize_without_building(self):
        args, binding, _, _, _ = self.binding_fixture()
        build = args.baseline_binary.parents[1]; target = build/'CMakeFiles/iotdb_fs.dir'
        compiler = self.root/'frozen/compiler'; source = Path(binding['baseline_source_root'])
        (build/'compile_commands.json').write_text(json.dumps([dict(file=str(source/'src/main.cpp'),
            arguments=[str(compiler), '-DEXAMPLE', '-I'+str(source/'include'), '-O3', '-DNDEBUG', '-std=c++17',
                '-o', 'CMakeFiles/iotdb_fs.dir/src/main.cpp.o', '-c', str(source/'src/main.cpp')])]))
        link = (target/'link.txt').read_text().strip(); (target/'link.txt').unlink()
        (build/'build.ninja').write_text('# frozen generated instruction\n')
        with patch.object(m.subprocess, 'run', return_value=SimpleNamespace(stdout=': && '+link+' && :\n')) as command:
            result = self.runner.verify_candidate(args)
            self.assertTrue(result['build_environment']['actual_target_and_libraries_equal'])
            self.assertEqual(command.call_args.args[0], ['ninja', '-C', str(build), '-t', 'commands', 'bin/iotdb_fs'])

    def test_actual_runtime_library_path_or_digest_difference_is_rejected(self):
        args, _, _, _, _ = self.binding_fixture()
        old = {'libexample.so.1': dict(path='/same/library', sha256='fixed')}
        for new in ({'libexample.so.1': dict(path='/other/library', sha256='fixed')},
                    {'libexample.so.1': dict(path='/same/library', sha256='changed')}):
            with patch.object(m, 'resolved_libraries', side_effect=[old, new]), \
                 self.assertRaisesRegex(RuntimeError, 'actual runtime library'):
                self.runner.verify_candidate(args)

    def test_linux_supplemental_runtime_gate_is_separate_from_the_1923_test_gate(self):
        args, _, _, _, _ = self.binding_fixture()
        result = self.runner.verify_candidate(args)
        self.assertEqual(result['unit_tests'], 1923)
        self.assertEqual(result['supplemental_runtime']['tests'], 3)
        self.assertEqual(result['supplemental_runtime']['platform'], 'Linux')

    def test_supplemental_runtime_requires_frozen_sources_core_build_and_all_regressions(self):
        for changed in ('platform', 'product', 'core', 'source', 'log', 'xml_skip', 'missing_test', 'command'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                self.root = Path(temp).resolve()
                args, binding, path, _, _ = self.binding_fixture()
                receipt_path = Path(binding['supplemental_runtime_receipt'])
                receipt = json.loads(receipt_path.read_text())
                if changed == 'platform': receipt['platform'] = 'Darwin'
                elif changed == 'product': receipt['candidate_sha256'] = OLD
                elif changed in ('core', 'source'):
                    Path(receipt['core_archive' if changed == 'core' else 'source']).write_bytes(b'changed input')
                elif changed == 'log': (receipt_path.parent/'run.stdout').write_text('changed log')
                elif changed in ('xml_skip', 'missing_test'):
                    tree = ET.parse(receipt['xml'])
                    if changed == 'xml_skip': tree.getroot().set('skipped', '1')
                    else: next(tree.getroot().iter('testcase')).set('name', 'unrelated')
                    tree.write(receipt['xml']); receipt['xml_sha256'] = file_sha(Path(receipt['xml']))
                else:
                    receipt['compile_command'][2] = '-O0'
                    command_path = Path(receipt['compile_command_path']); command_path.write_text(json.dumps(receipt['compile_command']))
                    receipt['compile_command_sha256'] = file_sha(command_path)
                receipt_path.write_text(json.dumps(receipt)); binding['supplemental_runtime_receipt_sha256'] = file_sha(receipt_path)
                path.write_text(json.dumps(binding))
                with self.assertRaises(RuntimeError):
                    self.runner.verify_candidate(args)


class ResolvedLibraryTests(unittest.TestCase):
    def test_ldd_resolves_and_hashes_all_dependencies_and_rejects_missing_libraries(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); lines = ['linux-vdso.so.1 (0x1234)']
            for name in ('libarrow.so.2100', 'libhdf5.so.310', 'libiotdb_session.so.2'):
                path = root/name; path.write_bytes(name.encode()); lines.append(name+' => '+str(path)+' (0x1234)')
            loader = root/'ld.so'; loader.write_bytes(b'loader'); lines.append(str(loader)+' (0x1234)')
            with patch.object(m.subprocess, 'run', return_value=SimpleNamespace(stdout='\n'.join(lines))):
                result = m.resolved_libraries(root/'iotdb_fs')
            self.assertEqual(len(result), 4)
            self.assertEqual(result['interpreter'], dict(path=str(loader), sha256=file_sha(loader)))
            for bad in ('libarrow.so.2100 => not found', 'statically linked', 'unexpected output'):
                with patch.object(m.subprocess, 'run', return_value=SimpleNamespace(stdout=bad)), self.assertRaises(RuntimeError):
                    m.resolved_libraries(root/'iotdb_fs')


if __name__ == '__main__':
    unittest.main()
