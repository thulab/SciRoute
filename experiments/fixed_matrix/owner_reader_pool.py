"""FM1 N128: frozen single-reader OLD versus one candidate with pools 1/2/4.

Query-only reuse of four sealed cases. Sixteen N8 functional queries precede
the 576 N128 rows; qualification never enters performance statistics.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
import json
import os
import re
import shlex
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
from types import SimpleNamespace
import xml.etree.ElementTree as ET

from . import client, grib_catalog_client, hdf5_batch_client, run, telemetry
from .grib_catalog import require_json_roundtrip, validate_row as validate_grib
from .hdf5_cache_batch import validate_row as validate_hdf5
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, PATH_ARGS
from fs_index.native import NativeContext
from .replay import SOURCE_PINS
from .shared_candidates import native_binding
from .hdf5_cache_batch import verify_build_binding
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-owner-reader-pool-n128/1'
WORK = 'experiments/cross_version/work/owner-reader-pool-n128-20261024'
CONFIGS = ('OLD', 'P1', 'P2', 'P4')
POOLS = {'OLD': 1, 'P1': 1, 'P2': 2, 'P4': 4}
CAMPAIGNS = (0, 1, 2)
ARMS = ('IoTDB-FS', 'FS-index')
WARMUPS, MEASURED, REPEATS = 2, 10, 12
POOL_KEY = 'storage.version_directory.reader_pool_size'
PRODUCT_DIFF = frozenset(('CMakeLists.txt', 'include/iotdbfs/IoTDBStructuredBlobBackend.h',
    'include/iotdbfs/storage/versioning/IoTDBVersionStore.h',
    'include/iotdbfs/storage/versioning/detail/BoundedReaderPool.h',
    'src/IoTDBStructuredBlobBackend.cpp', 'src/main.cpp',
    'src/storage/versioning/IoTDBVersionStore.cpp'))
RUNTIME_TESTS = (
    ('ReaderPoolRuntime', 'CancellingLastWaiterInterruptsProducerInsideSaturatedPool'),
    ('ReaderPoolRuntime', 'CancellingInitiatorPreservesJoinedProducerWaitingForReader'),
    ('ReaderPoolRuntime', 'StorageBudgetFailureStaysStickyAndDoesNotPoisonNextProducer'),
)
REQUIRED_TESTS = tuple(('BoundedReaderPool', name) for name in (
    'ReusesOnlyExplicitlyValidatedReaders', 'ConcurrentLeasesHaveIndependentReadersAndRespectCapacity',
    'SaturatedPoolTimesOutWithoutOpeningAnotherSession', 'WaitingCancellationDoesNotDisturbAnActiveReader',
    'CancelledRequestDoesNotConsumeAnIdleReader', 'FailedConnectionReleasesItsCapacityWithoutRetrying',
    'MovedLeaseReleasesAndClosesExactlyOnce', 'CancellationAfterConnectionOpenDiscardsTheNewReader',
    'AnOutstandingLeaseRetainsItsStateAfterPoolDestruction', 'InvalidLimitsAreRejectedBeforeOpeningConnections'))
need = run.need


def cmake_cache(path):
    return {line.split(':', 1)[0]: line.split('=', 1)[1] for line in path.read_text().splitlines()
            if line and not line.startswith(('#', '//')) and ':' in line and '=' in line}


def normalize_target_tokens(tokens, source_root, build_root):
    normalized = []
    for token in tokens:
        for root, label in ((source_root, '<SOURCE>'), (build_root, '<BUILD>')):
            token = token.replace(str(root)+'/', label+'/')
            if token == str(root): token = label
        normalized.append(token)
    return normalized


def target_commands(binary, values):
    """Read generated compile/link instructions without invoking a build."""
    build = binary.parents[1]; source = Path(values['CMAKE_HOME_DIRECTORY']).resolve(strict=True)
    compile_path = build/'compile_commands.json'; flags_path = build/'CMakeFiles/iotdb_fs.dir/flags.make'
    if compile_path.exists():
        entries = [v for v in read_json(compile_path) if Path(v['file']).resolve() == source/'src/main.cpp']
        need(len(entries) == 1, 'iotdb_fs main compile command missing or ambiguous')
        entry = entries[0]; command = entry.get('arguments') or shlex.split(entry['command'])
        compiler = Path(command[0]).resolve(strict=True); compile_tokens = []
        index = 1
        while index < len(command):
            token = command[index]
            if token in ('-o', '-c'):
                need(index+1 < len(command), 'incomplete main compile command')
                index += 2
            else:
                compile_tokens.append(token); index += 1
        compile_artifact = compile_path
    else:
        assignments = {line.split('=', 1)[0].strip(): line.split('=', 1)[1].strip()
                       for line in flags_path.read_text().splitlines() if line.startswith('CXX_') and '=' in line}
        need(set(assignments) == {'CXX_DEFINES', 'CXX_INCLUDES', 'CXX_FLAGS'}, 'iotdb_fs target flags incomplete')
        compile_tokens = sum((shlex.split(assignments[k]) for k in ('CXX_DEFINES', 'CXX_INCLUDES', 'CXX_FLAGS')), [])
        compiler = Path(values['CMAKE_CXX_COMPILER']).resolve(strict=True); compile_artifact = flags_path
    need(compiler == Path(values['CMAKE_CXX_COMPILER']).resolve(strict=True), 'actual target compiler differs from cache')
    link_path = build/'CMakeFiles/iotdb_fs.dir/link.txt'
    if link_path.exists():
        link_text = link_path.read_text().strip(); link_artifact = dict(path=str(link_path), sha256=file_sha(link_path))
    else:
        need((build/'build.ninja').is_file(), 'generated iotdb_fs link instruction missing')
        result = subprocess.run(['ninja', '-C', str(build), '-t', 'commands', 'bin/iotdb_fs'],
            check=True, capture_output=True, text=True, timeout=30)
        commands = result.stdout.splitlines(); need(bool(commands), 'ninja target command list empty')
        link_text = commands[-1]; link_artifact = dict(path=str(build/'build.ninja'), sha256=file_sha(build/'build.ninja'),
            command_sha256=digest(link_text), extraction='ninja -t commands; read only')
    link_tokens = shlex.split(link_text)
    if link_tokens[:2] == [':', '&&'] and link_tokens[-2:] == ['&&', ':']:
        link_tokens = link_tokens[2:-2]
    need(link_tokens and not any(t in ('&&', ';', '|', '||') for t in link_tokens), 'unexpected shell operation in link command')
    need(Path(link_tokens[0]).resolve(strict=True) == compiler, 'actual linker driver differs from compiler')
    return dict(compile_tokens=normalize_target_tokens(compile_tokens, source, build),
        link_tokens=normalize_target_tokens(link_tokens, source, build), compiler_sha256=file_sha(compiler),
        compile_artifact=dict(path=str(compile_artifact), sha256=file_sha(compile_artifact)), link_artifact=link_artifact)


def resolved_libraries(binary):
    result = subprocess.run(['ldd', str(binary)], check=True, capture_output=True, text=True, timeout=30)
    libraries = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith('linux-vdso.so.'): continue
        match = re.fullmatch(r'(\S+) => (/\S+) \(0x[0-9a-fA-F]+\)', line)
        if match: name, filename = match.groups()
        else:
            match = re.fullmatch(r'(/\S+) \(0x[0-9a-fA-F]+\)', line)
            need(match is not None, 'unresolved or unexpected ldd dependency: '+line)
            name, filename = 'interpreter', match.group(1)
        library = Path(filename).resolve(strict=True)
        need(name not in libraries and library.is_file(), 'ambiguous runtime dependency')
        libraries[name] = dict(path=str(library), sha256=file_sha(library))
    need(all(any(n.startswith(prefix) for n in libraries) for prefix in
             ('libarrow.so.', 'libhdf5.so.', 'libiotdb_session.so.')), 'required runtime libraries missing')
    return libraries


def compare_owner_build_environment(baseline, binary):
    """Permit search-prefix metadata differences only after actual target/library equality."""
    keys = ('CMAKE_BUILD_TYPE', 'CMAKE_CXX_COMPILER', 'CMAKE_CXX_FLAGS', 'CMAKE_CXX_FLAGS_RELEASE',
            'Arrow_DIR', 'HDF5_DIR', 'IOTDB_ROOT')
    records = {}
    for label, path in (('OLD', baseline), ('candidate', binary)):
        cache_path = path.parents[1]/'CMakeCache.txt'; values = cmake_cache(cache_path)
        need(all(values.get(k) for k in ('CMAKE_BUILD_TYPE', 'CMAKE_CXX_COMPILER', 'Arrow_DIR', 'HDF5_DIR', 'IOTDB_ROOT')),
             'build cache lacks compiler/dependency configuration')
        selected = {k: values.get(k) for k in keys}
        need(selected['CMAKE_BUILD_TYPE'] == 'Release', 'OLD/candidate must use Release builds')
        for k in ('CMAKE_CXX_COMPILER', 'Arrow_DIR', 'HDF5_DIR', 'IOTDB_ROOT'):
            selected[k] = str(Path(selected[k]).resolve(strict=True))
        records[label] = dict(cache_sha256=file_sha(cache_path), configuration=selected,
            search_prefix=values.get('CMAKE_PREFIX_PATH'), generator=values.get('CMAKE_GENERATOR'),
            target=target_commands(path, values), runtime_libraries=resolved_libraries(path))
    old, new = records['OLD'], records['candidate']
    need(old['configuration'] == new['configuration'], 'OLD/candidate compiler, Release flags or resolved dependencies differ')
    need(all(old['target'][key] == new['target'][key] for key in ('compile_tokens', 'link_tokens', 'compiler_sha256')),
         'actual iotdb_fs target compile/link instructions differ')
    need(old['runtime_libraries'] == new['runtime_libraries'], 'OLD/candidate actual runtime library paths or bytes differ')
    return dict(records=records, actual_target_and_libraries_equal=True, metadata_differences={
        key: dict(OLD=old[key], candidate=new[key]) for key in ('search_prefix', 'generator') if old[key] != new[key]})


def configuration_order(campaign):
    need(campaign in CAMPAIGNS, 'unknown fixed campaign')
    return (CONFIGS, ('P1', 'P2', 'P4', 'OLD'), ('P4', 'P2', 'P1', 'OLD'))[campaign]


def selected_groups(repo, n):
    need(n in (8, 128), 'only sealed N8/N128 cases are allowed')
    by = {g['group_id']: g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    groups = [by[f'FM1-{fmt}-N{n}-V1-C{n}-S0-base-latest'] for fmt in ('grib', 'hdf5')]
    for g in groups:
        need((g['N'], g['C_paths'], g['C_versions'], g['V_stored'], g['D'], g['K_units'],
              g['size_profile'], g['selection']) == (n, n, n, 1, 4, 2, 'S0', 'latest'),
             'sealed fixed group parameters differ')
    return groups


def protocol_spec(repo):
    binding = read_json(repo/WORK/'protocol-binding.json')
    return ProtocolSpec(PROTOCOL, 'experiments.fixed_matrix.owner_reader_pool', __doc__,
        binding['baseline_sha256'],
        frozenset(binding.get('product_diff', ['src/storage/versioning/IoTDBVersionStore.cpp'])),
        REQUIRED_TESTS, ('experiments/fixed_matrix/tests/test_owner_reader_pool.py',
            WORK+'/protocol-binding.json', WORK+'/experiment-plan.json', WORK+'/base-product-audit.json',
            'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py'))


class OwnerReaderPoolRunner(NativeABRunner):
    def groups(self, repo, tier):
        need(tier == 'n128', 'formal run is the two sealed N128 cases only')
        return selected_groups(repo, 128)

    def campaigns(self, group):
        return CAMPAIGNS

    def configuration_order(self, campaign):
        return configuration_order(campaign)

    def expected_rows(self, args):
        return len(self.groups(args.repo, args.tier))*len(CAMPAIGNS)*len(CONFIGS)*len(ARMS)*REPEATS

    def verify_supplemental_runtime(self, args, binding, sources, build_environment):
        path = Path(binding['supplemental_runtime_receipt']).resolve(strict=True)
        need(file_sha(path) == binding['supplemental_runtime_receipt_sha256'], 'supplemental runtime receipt changed')
        receipt = read_json(path); source_root = Path(binding['candidate_source_root']).resolve(strict=True)
        test_name = 'tests/unit/storage/versioning/test_ReaderPoolRuntime.cpp'
        need(receipt.get('complete') is True and receipt.get('platform') == 'Linux' and
             receipt.get('product_unchanged') is True and receipt.get('tests') == 3 and
             receipt.get('candidate_sha256') == binding['candidate_sha256'] and
             Path(receipt['candidate_source_root']).resolve(strict=True) == source_root and
             receipt['source_manifest_sha256'] == file_sha(Path(binding['source_manifest_path'])),
             'supplemental runtime gate is incomplete or bound to another platform/product/source')
        for artifact in ('source', 'frozen_test_source', 'core_archive', 'compiler', 'binary', 'xml', 'compile_command'):
            name = 'compile_command_path' if artifact == 'compile_command' else artifact
            need(file_sha(Path(receipt[name])) == receipt[artifact+'_sha256'], 'supplemental runtime artifact changed: '+artifact)
        need(Path(receipt['frozen_test_source']).resolve(strict=True) == source_root/test_name and
             receipt['source_sha256'] == receipt['frozen_test_source_sha256'] == sources[test_name],
             'supplemental runtime source differs from frozen candidate')
        core = args.binary.parents[1]/'libiotdb_fs_core.a'
        values = cmake_cache(args.binary.parents[1]/'CMakeCache.txt')
        need(Path(receipt['core_archive']).resolve(strict=True) == core.resolve(strict=True) and
             receipt['compiler_sha256'] == build_environment['records']['candidate']['target']['compiler_sha256'],
             'supplemental runtime core/compiler differs from candidate build')
        expected_command = [values['CMAKE_CXX_COMPILER'], '-std=c++17', '-O3', '-DNDEBUG', '-pthread',
            '-I'+str(source_root/'include'), receipt['source'], str(core), '-lgtest_main', '-lgtest', '-lcrypto', '-o', receipt['binary']]
        need(receipt['compile_command'] == expected_command == read_json(Path(receipt['compile_command_path'])),
             'supplemental runtime was not compiled against frozen core with approved flags')
        protected = {str(args.binary): binding['candidate_sha256'],
            str(Path(binding['candidate_unit_binary'])): binding['candidate_unit_binary_sha256'], str(core): receipt['core_archive_sha256']}
        need(receipt['protected_sha256'] == protected and all(file_sha(Path(p)) == sha for p, sha in protected.items()),
             'supplemental validation modified production artifacts')
        need(set(receipt['logs']) == {'compile.stdout', 'compile.stderr', 'run.stdout', 'run.stderr'} and
             all(file_sha(path.parent/name) == sha for name, sha in receipt['logs'].items()), 'supplemental runtime logs changed')
        root = ET.parse(receipt['xml']).getroot(); cases = list(root.iter('testcase'))
        need(int(root.attrib.get('tests', 0)) == len(cases) == 3 and
             all(int(n.attrib.get(k, 0)) == 0 for n in root.iter() if n.tag in ('testsuite', 'testsuites')
                 for k in ('failures', 'errors', 'disabled', 'skipped')) and
             not any(list(root.iter(k)) for k in ('failure', 'error', 'skipped')) and
             all(c.get('status') == 'run' and c.get('result') == 'completed' for c in cases) and
             all(sum(c.get('classname') == suite and c.get('name') == name for c in cases) == 1 for suite, name in RUNTIME_TESTS),
             'supplemental runtime must contain all three completed regressions without skips')
        return dict(complete=True, platform='Linux', tests=3, receipt_sha256=file_sha(path),
            xml_sha256=receipt['xml_sha256'], binary_sha256=receipt['binary_sha256'], core_archive_sha256=receipt['core_archive_sha256'])

    def verify_candidate(self, args):
        path = args.repo/WORK/'protocol-binding.json'
        binding = read_json(path)
        need(binding.get('complete') is True, 'protocol binding is incomplete')
        for key, actual, sha in (('baseline_binary', args.baseline_binary, 'baseline_sha256'),
                                ('candidate_binary', args.binary, 'candidate_sha256')):
            need(Path(binding[key]).resolve(strict=True) == actual and file_sha(actual) == binding[sha],
                 key+' path or digest changed')
        need(binding['baseline_sha256'] == self.spec.baseline_sha and
             binding['candidate_sha256'] != self.spec.baseline_sha, 'OLD/candidate identity differs')
        old_receipt_path = Path(binding['baseline_build_receipt']).resolve(strict=True)
        need(file_sha(old_receipt_path) == binding['baseline_build_receipt_sha256'],
             'OLD build receipt changed')
        old_gate = verify_build_binding(SimpleNamespace(binary=args.baseline_binary, build_binding=old_receipt_path))
        old = read_json(old_receipt_path)
        need(old.get('full_clean_build') is True and
             Path(old['source_root']).resolve(strict=True) == Path(binding['baseline_source_root']).resolve(strict=True) and
             Path(old['all_unit_tests']).resolve(strict=True) == Path(binding['baseline_unit_tests']).resolve(strict=True) and
             file_sha(Path(old['all_unit_tests'])) == binding['baseline_unit_tests_sha256'],
             'OLD source/full clean build or unit gate binding differs')
        for label in ('focused_tests', 'all_unit_tests'):
            gate_root = ET.parse(old[label]).getroot(); gate_cases = list(gate_root.iter('testcase'))
            need(len(gate_cases) == int(gate_root.attrib.get('tests', 0)) and gate_cases and
                 all(int(n.attrib.get(k, 0)) == 0 for n in gate_root.iter()
                     if n.tag in ('testsuite', 'testsuites') for k in ('failures', 'errors', 'disabled', 'skipped')) and
                 not any(list(gate_root.iter(k)) for k in ('failure', 'error', 'skipped')) and
                 all(c.get('status') == 'run' and c.get('result') == 'completed' for c in gate_cases),
                 'OLD focused/full gate contains skipped or incomplete cases')
        audit_path = args.repo/WORK/'base-product-audit.json'; audit = read_json(audit_path)
        need(audit.get('complete') is True and
             audit.get('binary_sha256', {}).get('baseline_binary') == binding['baseline_sha256'],
             'original single-reader product audit is incomplete or bound to another binary')
        source_root = Path(binding['candidate_source_root']).resolve(strict=True)
        manifest_path = Path(binding['source_manifest_path']).resolve(strict=True)
        sources = read_json(manifest_path)
        need(isinstance(sources, dict) and sources and sources == binding['source_files'],
             'frozen candidate source manifest differs')
        for name, sha in sources.items():
            relative = Path(name)
            source = source_root/relative
            need(not relative.is_absolute() and '..' not in relative.parts and not source.is_symlink() and
                 source.resolve(strict=True).is_relative_to(source_root) and file_sha(source) == sha,
                 'candidate source changed: '+name)
        need({str(p.relative_to(source_root)) for p in source_root.rglob('*')
              if p.is_file() and '__pycache__' not in p.relative_to(source_root).parts} == set(sources),
             'candidate source manifest must enumerate exact frozen source closure')
        product = lambda name: name.startswith(('src/', 'include/', 'cmake/')) or name == 'CMakeLists.txt'
        names = {name for name in old['source_files'] if product(name)} | {name for name in sources if product(name)}
        changed = {name for name in names if old['source_files'].get(name) != sources.get(name)}
        need(changed == PRODUCT_DIFF and set(binding['product_diff']) == PRODUCT_DIFF,
             'OLD/candidate product differs outside the approved seven pool files')
        cache = args.binary.parents[1]/'CMakeCache.txt'
        need(file_sha(cache) == binding['candidate_build_cache_sha256'], 'candidate build cache changed')
        home = [line.split('=', 1)[1] for line in cache.read_text().splitlines()
                if line.startswith('CMAKE_HOME_DIRECTORY:INTERNAL=')]
        need(len(home) == 1 and Path(home[0]).resolve(strict=True) == source_root,
             'candidate build cache is bound to another source root')
        build_environment = compare_owner_build_environment(args.baseline_binary, args.binary)
        unit_path = Path(binding['unit_tests_path']).resolve(strict=True)
        need(file_sha(unit_path) == binding['unit_tests_sha256'], 'candidate unit XML changed')
        unit_binary = Path(binding['candidate_unit_binary']).resolve(strict=True)
        need(unit_binary.parent == args.binary.parent and
             file_sha(unit_binary) == binding['candidate_unit_binary_sha256'],
             'candidate unit binary changed or not from the same build')
        root = ET.parse(unit_path).getroot(); cases = list(root.iter('testcase'))
        need(root.tag in ('testsuites', 'testsuite') and int(root.attrib.get('tests', 0)) == 1923 and
             len(cases) == 1923 and all(int(node.attrib.get(key, 0)) == 0
                for node in root.iter() if node.tag in ('testsuites', 'testsuite')
                for key in ('failures', 'errors', 'disabled', 'skipped')) and
             not any(list(root.iter(key)) for key in ('failure', 'error', 'skipped')) and
             all(c.get('status') == 'run' and c.get('result') == 'completed' for c in cases),
             'candidate requires exactly 1923 completed unit tests without skips')
        for suite, name in REQUIRED_TESTS:
            need(sum(c.get('classname') == suite and c.get('name') == name for c in cases) == 1,
                 'required reader runtime regression is missing: '+name)
        supplemental = self.verify_supplemental_runtime(args, binding, sources, build_environment)
        return dict(complete=True, candidate_sha256=binding['candidate_sha256'],
            baseline_sha256=binding['baseline_sha256'], protocol_binding_sha256=file_sha(path),
            base_product_audit_sha256=file_sha(audit_path),
            baseline_build_gate=old_gate, changed_product_files=sorted(changed),
            build_environment=build_environment, candidate_unit_binary_sha256=file_sha(unit_binary),
            supplemental_runtime=supplemental,
            candidate_source_root=str(source_root), source_manifest_sha256=file_sha(manifest_path),
            source_files=len(sources), unit_tests=1923, unit_tests_sha256=file_sha(unit_path))

    def source_proof(self, args):
        need(args.tier == 'n128' and args.gate_report is None, 'qualification is in-run N8, not an unrelated gate')
        source = args.source_collection
        need(source.name == 'collection-r2', 'sealed FM1 collection-r2 required')
        for name, sha in SOURCE_PINS.items():
            need(file_sha(source/name) == sha, 'scientific source changed: '+name)
        need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json') == run.CATALOG_SHA,
             'fixed catalog changed')
        need(file_sha(args.library) == run.NATIVE_SHA and
             file_sha(args.inspector) == run.infrastructure.INSPECTOR_SHA, 'native helper or inspector changed')
        build = self.verify_candidate(args)
        registry = Registry(source/'registry'); bindings = read_json(source/'bindings.json')
        original = read_json(source/'report.json'); entries = []
        for group in selected_groups(args.repo, 8)+self.groups(args.repo, args.tier):
            gid = group['group_id']; bound = bindings[gid]
            corpus = registry.get_corpus(bound['corpus_id']); case = registry.get_case(bound['case_id'])
            need(case['corpus_id'] == corpus['corpus_id'] and case['expected_matches'] == bound['expected_matches'] and
                 digest(case['expected_matches']) == bound['oracle_sha256'], 'case/oracle identity changed')
            matched = [v for v in original['corpora'] if v['corpus_id'] == corpus['corpus_id']]
            need(len(matched) == 1, 'independent corpus is not uniquely bound')
            item = matched[0]; base = source/'corpora'/item['corpus_key']
            prep = read_json(base/'preparation.json'); manifest = read_json(base/'input/manifest.json')
            need(prep['complete'] and file_sha(base/'index.sqlite') == item['index_sha256'] == prep['index_sha256'] and
                 file_sha(base/'input/manifest.json') == item['raw_manifest_sha256'] == corpus['source_evidence']['manifest_sha256'] and
                 file_sha(base/'publication.json') == prep['publication_receipt_sha256'] == corpus['publication_evidence']['receipt_sha256'],
                 'sealed index/manifest/publication changed')
            need(read_json(base/'publication.json')['head'] == item['head'] and
                 run.infrastructure.journal_snapshot(base/'owner') == item['journal_digest'],
                 'archived head/journal changed')
            entry = dict(group_id=gid, format=group['format'], qualification=group['N'] == 8,
                corpus_id=corpus['corpus_id'], case_id=case['case_id'], corpus_key=item['corpus_key'],
                source_root=str(base), head=item['head'], journal=str(base/'owner'), namespace=item['namespace'],
                journal_digest=item['journal_digest'], artifacts={n: file_sha(base/n) for n in
                    ('input/manifest.json', 'catalog.json', 'index.sqlite', 'publication.json', 'preparation.json')},
                raw_validation=run.verify_raw(manifest, base/'input', base/'fs'))
            if group['format'] == 'hdf5':
                entry.update(native_binding(args, corpus['corpus_id'], manifest))
            entries.append(entry)
        return dict(protocol=PROTOCOL, complete=True, tier=args.tier, cases=entries, build_gate=build,
            source_collection=str(source), source_pins=SOURCE_PINS, current_collection=str(args.current_collection),
            current_location_files={n: file_sha(args.current_collection/n) for n in ('current-locations.json', 'report.json')},
            expected_queries=576, qualification_queries=16, configurations=list(CONFIGS), pools=POOLS,
            configuration_order=[list(configuration_order(c)) for c in CAMPAIGNS], arms=list(ARMS),
            campaigns=list(CAMPAIGNS), warmup_per_arm=WARMUPS, measured_per_arm=MEASURED,
            product=dict(path=str(args.binary), sha256=build['candidate_sha256']),
            baseline=dict(path=str(args.baseline_binary), sha256=self.spec.baseline_sha),
            resource_identity=run.infrastructure.resource_identity([2, 3]), scientific_publication=False,
            index_rebuilt=False, new_cases=False, location_bindings_created=False)

    def plans(self, args, registry, report, proof):
        original = read_json(args.source_collection/'report.json'); oldbind = read_json(args.source_collection/'bindings.json')
        source_registry = Registry(args.source_collection/'registry')
        selected = {g['group_id']: g for g in selected_groups(args.repo, 8)+self.groups(args.repo, args.tier)}
        result = []; bindings = {}
        for entry in proof['cases']:
            gid = entry['group_id']; cid = entry['corpus_id']; base = Path(entry['source_root'])
            corpus = registry.register_corpus(source_registry.get_corpus(cid))
            registry.register_case(source_registry.get_case(entry['case_id']))
            bindings[gid] = deepcopy(oldbind[gid]); bindings[gid]['run_ids'] = {}
            execution = args.work/'corpora'/entry['corpus_key']; execution.mkdir(parents=True, mode=0o700)
            item = deepcopy(next(v for v in original['corpora'] if v['corpus_id'] == cid))
            item.update(complete=False, mounts=[], source_root=str(base), head=entry['head'],
                        journal_digest=entry['journal_digest'], qualification=entry['qualification'])
            report['corpora'].append(item)
            result.append(dict(root=base, execution_root=execution, corpus=corpus, group=selected[gid], item=item,
                journal=Path(entry['journal']), journal_digest=entry['journal_digest'], scientific_head=entry['head'],
                native_root=Path(entry['native_root']) if entry['format'] == 'hdf5' else None,
                prep=read_json(base/'preparation.json'), manifest=read_json(base/'input/manifest.json'),
                cfg=dict(catalog_path=str(base/'catalog.json'), index_path=str(base/'index.sqlite'),
                    cache_files=8, cache_datasets=8, metadata_cache_bytes=2097152, raw_chunk_cache_bytes=262144)))
        return result, bindings

    def launcher(self, path, binary, sha, namespace, journal, plan, configuration):
        need(configuration in CONFIGS, 'unknown reader configuration')
        receipt = (super().launcher(path, binary, sha, namespace, journal, plan)
            if plan['group']['format'] == 'hdf5' else run.infrastructure.launcher(path, binary, sha, namespace, journal))
        need(POOL_KEY not in receipt['settings'], 'base launcher unexpectedly sets reader pool')
        if configuration != 'OLD':
            old = 'SETTINGS='+repr(receipt['settings'])+'\n'; source = path.read_text()
            need(source.count(old) == 1, 'launcher settings declaration differs')
            settings = dict(receipt['settings'], **{POOL_KEY: str(POOLS[configuration])})
            path.write_text(source.replace(old, 'SETTINGS='+repr(settings)+'\n', 1))
            receipt.update(settings=settings, sha256=file_sha(path))
        return receipt

    def validate_row(self, row, group, configuration):
        need(configuration in CONFIGS, 'unknown reader configuration')
        if group['format'] == 'hdf5':
            validate_hdf5(row, group['K_units'], 'C1B1')
            d = row['discovery_call']['server_profile']; q = row['value_calls'][0]['server_profile']
            need(d.get('operation') == 'dataset_catalog' and q['counters'].get('typed_batch_items') == 2,
                 'original HDF5 discovery/batch protocol differs')
        else:
            validate_grib(row, 'C', group['K_units'])
            need(all(v['server_profile'].get('operation') == 'typed' for v in row['value_calls']),
                 'GRIB must retain two serial ordinary typed value calls')
        return row

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired, *, qualification=False):
        paths = read_json(args.work/'resource-paths.json'); count = 0
        filename = 'qualification.jsonl' if qualification else 'queries.jsonl'
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        for campaign in ((0,) if qualification else CAMPAIGNS):
            plans = sorted((p for p in execution if (p['group']['N'] == 8) == qualification),
                key=lambda p: ('grib', 'hdf5').index(p['group']['format']), reverse=campaign % 2 == 1)
            for ordinal, plan in enumerate(plans):
                group = plan['group']; gid = group['group_id']; b = bindings[gid]
                for configuration in configuration_order(campaign):
                    # Retain the supervisor's exact campaign-* cleanup layout
                    # for functional mounts as well as formal mounts.
                    block = plan['execution_root']/('campaign-qualification' if qualification else 'campaign-'+str(campaign))/configuration
                    block.mkdir(parents=True)
                    journal = block/'owner'; shutil.copytree(plan['journal'], journal)
                    need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'journal copy differs')
                    binary = args.baseline_binary if configuration == 'OLD' else args.binary; sha = file_sha(binary)
                    launch = self.launcher(block/'launcher', binary, sha, plan['item']['namespace'], journal, plan, configuration)
                    atomic_json(block/'launcher.receipt.json', launch)
                    before, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-before')
                    need(before == plan['scientific_head'], 'head changed before mount')
                    print(json.dumps(dict(event='owner_reader_pool_mount', qualification=qualification,
                        campaign=campaign, group=gid, configuration=configuration)), flush=True)
                    with paired.sf.private_mount(Path(launch['path']), block/'mount', env, writer, profile=True) as context, ExitStack() as stack:
                        need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha, 'running binary differs')
                        actual = read_json(context['root']/'profile.launch.json')
                        need(actual['settings'] == launch['settings'], 'actual reader settings differ')
                        native = stack.enter_context(NativeContext(args.library, dict(plan['cfg'], build_index=False)))
                        environment = telemetry.environment(args.repo, artifact_paths=[args.library, binary],
                            process_roles=dict(client=os.getpid(), daemon=context['pid'],
                                datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())), data_path=plan['root'])
                        atomic_json(block/'environment.json', environment)
                        mount = dict(campaign=campaign, configuration=configuration, qualification=qualification,
                            daemon_pid=context['pid'], startup_ns=context['startup_ns'], product_sha256=sha,
                            reader_pool_size=POOLS[configuration], settings=launch['settings'],
                            environment_sha256=file_sha(block/'environment.json'))
                        plan['item']['mounts'].append(mount)
                        atomic_json(block/'query-plan.json', native.explain('index', b['native_selection'], client.PREDICATE, b['scope']))
                        for arm in ARMS:
                            model = build_run(registry.get_case(b['case_id']), execution=str(block), arm=arm,
                                implementation=dict(binary_sha256=sha if arm == 'IoTDB-FS' else run.NATIVE_SHA,
                                    runner_sha256=digest(frozen['sources']), engine='SQLite index + native owned values' if arm == 'FS-index'
                                        else 'native-hdf5 cache+batch' if group['format'] == 'hdf5' else 'format_catalog + two serial typed values'),
                                clock=dict(kind='client-owned-full-query', d='entry through discovery delivery',
                                    q='discovery through owned Float32/mask', profile_oracle='outside'),
                                cache=dict(protocol=PROTOCOL, configuration=configuration, reader_pool_size=POOLS[configuration],
                                    native_helper=plan['cfg'] if arm == 'FS-index' else None,
                                    daemon_settings=launch['settings'] if arm == 'IoTDB-FS' else None),
                                environment=dict(resource_id=frozen['resource_id'], process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign, qualification=qualification, warmup=0 if qualification else WARMUPS,
                                    measured=0 if qualification else MEASURED, functional_queries=1 if qualification else 0,
                                    schedule='fixed rotating configuration order; paired alternating arms'))
                            bound = registry.register_run(model)
                            b['run_ids'].setdefault(str(campaign), {}).setdefault(configuration, {})[arm] = bound['run_id']
                        atomic_json(args.work/'bindings.json', bindings)
                        for repeat in range(1 if qualification else REPEATS):
                            order = ARMS if (repeat+campaign+ordinal+CONFIGS.index(configuration)) % 2 == 0 else ARMS[::-1]
                            for arm in order:
                                rb = telemetry.sample(paths); run.validate_sample(rb)
                                if arm == 'IoTDB-FS':
                                    query = hdf5_batch_client.query if group['format'] == 'hdf5' else grib_catalog_client.query
                                    row = query(context, group, b['scope'], b['physical_versions'], b['expected_matches'],
                                        **({'batch': True} if group['format'] == 'hdf5' else {'lightweight': True}))
                                    self.validate_row(row, group, configuration)
                                else:
                                    row = client.query_native(native, arm, group, b['scope'], b['native_selection'], b['expected_matches'])
                                ra = telemetry.sample(paths); run.validate_sample(ra)
                                row.update(group_id=gid, corpus_id=b['corpus_id'], case_id=b['case_id'], oracle_sha256=b['oracle_sha256'],
                                    run_id=b['run_ids'][str(campaign)][configuration][arm], campaign=campaign, configuration=configuration,
                                    reader_pool_size=POOLS[configuration], arm=arm, repeat=repeat,
                                    phase='qualification' if qualification else 'warmup' if repeat < WARMUPS else 'measurement', ok=True,
                                    resource_id=frozen['resource_id'], resource_before=rb, resource_after=ra,
                                    environment_sha256=mount['environment_sha256'], product_sha256=sha)
                                run.append(args.work/filename, row); count += 1
                                report['qualification_records' if qualification else 'query_records'] = count
                            atomic_json(args.work/'report.json', report)
                    after, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-after')
                    need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'],
                         'query changed head/journal')
                    need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'], 'query changed FS-index')
                    mount.update(complete=True, scientific_head_unchanged=True, journal_unchanged=True)
                    atomic_json(args.work/'report.json', report)
        return count

    def validate_rows(self, rows, bindings, selected, resource_id, candidate_sha, *, qualification=False):
        wanted = {(g['group_id'], c, config, a, r) for g in selected
            for c in ((0,) if qualification else CAMPAIGNS) for config in CONFIGS for a in ARMS
            for r in range(1 if qualification else REPEATS)}
        keys = {(r['group_id'], r['campaign'], r['configuration'], r['arm'], r['repeat']) for r in rows}
        need(len(rows) == len(wanted) and keys == wanted, 'all fixed rows are required without duplication or exclusion')
        gm = {g['group_id']: g for g in selected}
        for row in rows:
            b = bindings[row['group_id']]; g = gm[row['group_id']]; config = row['configuration']
            need(all(row[k] == b[k] for k in ('corpus_id', 'case_id', 'oracle_sha256')) and
                 row['run_id'] == b['run_ids'][str(row['campaign'])][config][row['arm']], 'scientific/run identity differs')
            need(row['product_sha256'] == (self.spec.baseline_sha if config == 'OLD' else candidate_sha) and
                 row['reader_pool_size'] == POOLS[config] and row['resource_id'] == resource_id and bool(resource_id),
                 'product/pool/resource identity differs')
            phase = 'qualification' if qualification else 'warmup' if row['repeat'] < WARMUPS else 'measurement'
            need(row['phase'] == phase and all(row.get(k) is True for k in ('ok', 'oracle_verified', 'mode_verified')) and
                 row['selected_units'] == g['K_units'] and row['owned_output_bytes'] == g['owned_output_bytes'],
                 'phase or verified owned output differs')
            need(all(type(row[k]) is int and row[k] > 0 for k in ('total_ns', 'discovery_ns', 'value_ns')) and
                 row['total_ns'] == row['discovery_ns']+row['value_ns'], 'full T=d+q clock differs')
            for side in ('resource_before', 'resource_after'):
                run.validate_sample(row[side])
            need(row['resource_after']['monotonic_ns'] > row['resource_before']['monotonic_ns'], 'resource clock reversed')
            if row['arm'] == 'IoTDB-FS':
                self.validate_row(row, g, config)

    def analyze(self, path, bindings, selected, resource_id, candidate_sha):
        rows = [json.loads(s) for s in Path(path).read_text().splitlines()]
        self.validate_rows(rows, bindings, selected, resource_id, candidate_sha)
        work = Path(path).parent; gate = read_json(work/'qualification-report.json')
        need(gate.get('complete') is True and gate.get('query_records') == 16 and
             gate.get('queries_sha256') == file_sha(work/'qualification.jsonl'), 'N8 functional qualification incomplete or changed')
        qualification = [json.loads(s) for s in (work/'qualification.jsonl').read_text().splitlines()]
        # Group identities and output shapes were frozen in the qualification
        # report; the same full row validator is applied again after restoration.
        self.validate_rows(qualification, bindings, gate['groups'], resource_id, candidate_sha, qualification=True)
        coverage = run.validate_coverage(work, qualification)
        cells = []; effects = []
        for group in selected:
            gid = group['group_id']; by_config = {}
            for config in CONFIGS:
                blocks = []
                for campaign in CAMPAIGNS:
                    arms = {a: {k: statistics.median(r[k]/1e6 for r in rows if r['group_id'] == gid and
                        r['configuration'] == config and r['campaign'] == campaign and r['arm'] == a and r['phase'] == 'measurement')
                        for k in ('total_ns', 'discovery_ns', 'value_ns')} for a in ARMS}
                    blocks.append(dict(campaign=campaign, warm_median_ms=arms, **{name: arms['IoTDB-FS'][k]/arms['FS-index'][k]
                        for name, k in (('R', 'total_ns'), ('Rd', 'discovery_ns'), ('Rq', 'value_ns'))}))
                cell = dict(group_id=gid, configuration=config, corpus_id=bindings[gid]['corpus_id'],
                    case_id=bindings[gid]['case_id'], blocks=blocks,
                    median_ms={a: {k: statistics.median(b['warm_median_ms'][a][k] for b in blocks)
                        for k in ('total_ns', 'discovery_ns', 'value_ns')} for a in ARMS},
                    **{k: statistics.median(b[k] for b in blocks) for k in ('R', 'Rd', 'Rq')})
                cells.append(cell); by_config[config] = cell
            for reference in ('OLD', 'P1'):
                for config in CONFIGS:
                    if config == reference:
                        continue
                    blocks = []
                    for current, base in zip(by_config[config]['blocks'], by_config[reference]['blocks']):
                        blocks.append(dict(campaign=current['campaign'],
                            **{k+'_over_reference': current[k]/base[k] for k in ('R', 'Rd', 'Rq')},
                            **{a+'_'+label+'_over_reference': current['warm_median_ms'][a][key]/base['warm_median_ms'][a][key]
                                for a in ARMS for label, key in (('T', 'total_ns'), ('d', 'discovery_ns'), ('q', 'value_ns'))}))
                    effects.append(dict(group_id=gid, configuration=config, reference=reference, blocks=blocks,
                        **{key: statistics.median(b[key] for b in blocks) for key in blocks[0] if key != 'campaign'}))
        return dict(protocol=PROTOCOL, complete=True, query_rows=len(rows), qualification_rows=len(qualification),
            qualification_resource_coverage=coverage, cells=cells, effects=effects,
            statistic='Per-campaign arm medians, then paired ratios and median across three campaigns; all formal rows retained.',
            limitation='Sequential retained-process warm full queries; no cold-disk or query-concurrency claim.')

    def worker(self, args):
        def stop(sig, frame):
            raise RuntimeError('owner reader pool worker cancelled; preserve evidence')
        for sig in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, stop)
        proof = self.source_proof(args); frozen = read_json(args.work/'prepared.json')
        need(proof == read_json(args.work/'reuse-source.json') and self.closure(args.repo) == frozen['sources'], 'preflight changed')
        paired, _, env, startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf, 1800, args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL, complete=False, tier=args.tier, query_records=0, qualification_records=0,
            expected_queries=self.expected_rows(args), corpora=[], product_sha256=file_sha(args.binary),
            baseline_sha256=self.spec.baseline_sha, resource_id=frozen['resource_id'], scientific_publication=False,
            index_rebuilt=False, new_cases=False, location_bindings_created=False, credentials_recorded=False)
        atomic_json(args.work/'report.json', report)
        try:
            registry = Registry(args.work/'registry'); execution, bindings = self.plans(args, registry, report, proof)
            atomic_json(args.work/'execution-order.json', dict(configurations=list(CONFIGS), arms=list(ARMS),
                qualification_first=True, qualification_queries=16, fresh_mount_and_FS_context=True,
                formal=[dict(campaign=c, formats=['grib', 'hdf5'] if c % 2 == 0 else ['hdf5', 'grib'],
                    configurations=list(configuration_order(c))) for c in CAMPAIGNS]))
            count = self.collect(args, execution, bindings, registry, frozen, report, env, paired, qualification=True)
            need(count == 16, 'N8 qualification row count differs')
            qrows = [json.loads(s) for s in (args.work/'qualification.jsonl').read_text().splitlines()]
            qgroups = selected_groups(args.repo, 8)
            self.validate_rows(qrows, bindings, qgroups, frozen['resource_id'], frozen['product']['sha256'], qualification=True)
            atomic_json(args.work/'qualification-report.json', dict(complete=True, query_records=16, groups=qgroups,
                queries_sha256=file_sha(args.work/'qualification.jsonl'), performance_statistics=False))
            report['qualification_complete'] = True
            atomic_json(args.work/'report.json', report)
            count = self.collect(args, execution, bindings, registry, frozen, report, env, paired)
            need(count == self.expected_rows(args), 'formal collection incomplete')
            need(self.source_proof(args) == proof and self.closure(args.repo) == frozen['sources'], 'frozen inputs changed')
            for plan in execution:
                plan['item'].update(complete=True, final_raw_validation=run.verify_raw(plan['manifest'], plan['root']/'input', plan['root']/'fs'))
            report.update(complete=True, queries_sha256=file_sha(args.work/'queries.jsonl'),
                bindings_sha256=file_sha(args.work/'bindings.json'), qualification_sha256=file_sha(args.work/'qualification.jsonl'),
                qualification_report_sha256=file_sha(args.work/'qualification-report.json'))
        except BaseException as error:
            report['failure'] = dict(type=type(error).__name__, message=str(error)[:500])
            raise
        finally:
            atomic_json(args.work/'report.json', report)

    def main(self):
        parser = argparse.ArgumentParser(description=__doc__)
        for key in PATH_ARGS:
            parser.add_argument('--'+key.replace('_', '-'), type=Path,
                required=key not in ('binary', 'baseline_binary', 'build_binding', 'baseline_build_binding'))
        parser.add_argument('--tier', choices=('n128',), default='n128')
        parser.add_argument('--gate-report', type=Path)
        modes = parser.add_mutually_exclusive_group()
        modes.add_argument('--worker', action='store_true'); modes.add_argument('--prepare-only', action='store_true')
        modes.add_argument('--verify-prepared', action='store_true')
        args = parser.parse_args(); args.repo = args.repo.resolve(strict=True)
        binding_path = args.repo/WORK/'protocol-binding.json'; binding = read_json(binding_path)
        for key, default in (('binary', binding['candidate_binary']), ('baseline_binary', binding['baseline_binary']),
                             ('build_binding', binding_path), ('baseline_build_binding', binding_path)):
            if getattr(args, key) is None:
                setattr(args, key, Path(default))
        for key in PATH_ARGS:
            setattr(args, key, getattr(args, key).resolve(strict=key != 'work'))
        for source in (args.source_collection, args.current_collection):
            need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work), 'run/source paths overlap')
        if args.worker:
            self.worker(args)
        else:
            proof = self.source_proof(args); require_json_roundtrip(proof, 'preflight proof')
            if args.prepare_only:
                self.prepare_only(args, proof)
            elif args.verify_prepared:
                self.verify_prepared(args, proof)
            else:
                self.supervise(args, proof)


if __name__ == '__main__':
    OwnerReaderPoolRunner(protocol_spec(Path(__file__).resolve().parents[2])).main()
