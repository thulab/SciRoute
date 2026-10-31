"""Existing N512/B4/C128 HDF scene; no publication or index construction.

Preparation hashes the full 512-source backing corpus, including predicate
misses. Query execution reuses the frozen owned-array clock and independent
oracle validation. All Python/native setup and integrity checks are outside it.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import importlib
import json
from pathlib import Path
import sys
import time

CORPUS_ID = '69afe940d4184232679e46266e4f56e6089b363b6b8d19fe19f00bc119c5cfae'
CASE_ID = 'e3bbf388a538a86f11e6b66245172c9ac20df3fc24b9914eb832d1776f88bd98'
PREPARED = 'experiments/cross_version/work/hdf5-fee-batch-opt-20261020/final/prepared.json'
PREPARED_SHA = '4777fa6bee053eb00bfc0541ab3e37b0e191f0c275b6effb015c790dd369f991'
BUCKETS = ['b00', 'b01', 'b05', 'b12']
_MODULES = {}


def _need(ok, message):
    if not ok:
        raise RuntimeError(message)


def _sha(path):
    out = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            out.update(block)
    return out.hexdigest()


def _load(path):
    return json.loads(Path(path).read_text())


def _registry_documents(repo, registry):
    from experiments.identity.registry import Registry
    registry = Registry(registry) if not hasattr(registry, 'get_corpus') else registry
    source = Registry(Path(repo)/'experiments/cross_version/work/experiment-identity-20261020/registry')
    corpus, case = source.get_corpus(CORPUS_ID), source.get_case(CASE_ID)
    registry.register_corpus(corpus)
    registry.register_case(case)
    return corpus, case


def prepare_hdf(repo, registry):
    """Return a JSON-only scene after a complete read-only physical preflight."""
    repo = Path(repo).resolve(strict=True)
    corpus, case = _registry_documents(repo, registry)
    prepared_path = repo/PREPARED
    _need(_sha(prepared_path) == PREPARED_SHA, 'accepted C128 preparation changed')
    prepared = _load(prepared_path)
    _need(prepared.get('complete') is True, 'C128 preparation incomplete')
    identity = prepared['identity']
    _need(case['corpus_id'] == CORPUS_ID and corpus['parameters']['N'] == 512 and
          case['parameters']['C_paths'] == case['parameters']['C_versions'] == 128 and
          case['parameters']['scope']['buckets'] == BUCKETS and
          case['parameters']['selection'] == {'kind':'latest'}, 'strict HDF case changed')
    stage = Path(identity['source_stage'])
    root = stage/'cases/n512r1'
    report = _load(root/'report.json')
    phase = next(p for p in report['phases'] if p['V'] == 1)
    scene = dict(protocol='iotdbfs-resource-limited-hdf-scene/1', scene_id='hdf5-c128',
        corpus_id=CORPUS_ID, case_id=CASE_ID, repo=str(repo), N=512, backing_N=512,
        B=4, C_paths=128, C_versions=128, D=4, V_stored=1, selection='latest',
        format='hdf5', K=2, owned_output_bytes=40960, origin=[0,0,0,0], shape=[1,1,64,64],
        prefix=report['prefix'], logical_prefix=report['logical_prefix'], head=phase['head'],
        original_owner=str(root/'owner'), journal_digest=identity['journal_digest'],
        raw_root=str(root/'raw'), case_root=str(root), source_stage=str(stage),
        runner=identity['runner'], runner_manifest_sha256=identity['runner_manifest_sha256'],
        native_library=identity['native_library'], native_sha256=identity['native_sha256'],
        native_artifacts=deepcopy(prepared['native_artifacts']),
        frozen_inputs=deepcopy(identity['frozen_inputs']), selector=deepcopy(identity['selector']),
        expected_matches=deepcopy(case['expected_matches']), inventory=deepcopy(corpus['inventory']),
        raw_files=deepcopy(corpus['source_evidence']['raw_files']),
        preparation_evidence={'path':str(prepared_path),'sha256':PREPARED_SHA},
        limitation='four historical known-hit complete buckets within N512; not an independent N128 corpus; source hashing warms OS pages')
    scene['preflight'] = inspect_hdf(scene, registry)
    return scene


def inspect_hdf(scene, registry=None):
    """Rehash all physical sources and immutable execution evidence, never SQL."""
    started = time.perf_counter_ns()
    _need(scene['corpus_id'] == CORPUS_ID and scene['case_id'] == CASE_ID, 'HDF identity differs')
    if registry is not None:
        from experiments.identity.registry import Registry
        registry = Registry(registry) if not hasattr(registry, 'get_corpus') else registry
        case = registry.get_case(CASE_ID)
        corpus = registry.get_corpus(CORPUS_ID)
        _need(case['corpus_id'] == CORPUS_ID and
              scene['inventory'] == corpus['inventory'] and
              scene['raw_files'] == corpus['source_evidence']['raw_files'] and
              scene['expected_matches'] == case['expected_matches'], 'case/corpus registry differs')
    artifacts = {**scene['frozen_inputs'], **scene['native_artifacts'],
                 scene['native_library']:scene['native_sha256'],
                 scene['preparation_evidence']['path']:scene['preparation_evidence']['sha256']}
    for path, expected in artifacts.items():
        _need(Path(path).is_file() and not Path(path).is_symlink() and _sha(path) == expected,
              'frozen HDF artifact changed: '+path)
    manifest = _load(Path(scene['raw_root'])/'manifest.json')
    _need(manifest['N'] == 512 and manifest['rounds'] == 1 and len(manifest['entries']) == 512,
          'full N512 backing inventory required')
    inventory = {(e['logical_file'],e['ordinal']):e for e in scene['inventory']}
    raw_map = {(e['logical_file'],e['ordinal']):e['file'] for e in scene['raw_files']}
    seen = set()
    for entry in manifest['entries']:
        key = (entry['logical_file'],entry['ordinal'])
        _need(key not in seen and key in inventory and key in raw_map, 'raw inventory key differs')
        seen.add(key); record = inventory[key]
        _need(entry['sha256'] == record['sha256'] and entry['bytes'] == record['bytes'] and
              raw_map[key] == entry['file'], 'registered raw fingerprint differs')
        path = Path(scene['raw_root'])/entry['file']
        _need(path.is_file() and not path.is_symlink() and path.stat().st_nlink == 1 and
              path.stat().st_size == entry['bytes'] and _sha(path) == entry['sha256'],
              'physical raw HDF source changed: '+str(path))
    _need(len(seen) == len(inventory) == len(raw_map) == 512, 'incomplete registered backing corpus')
    catalog_path = next(p for p in scene['native_artifacts'] if p.endswith('/catalog.json'))
    catalog = _load(catalog_path)
    selected = [e for e in manifest['entries'] if Path(e['logical_file']).parent.as_posix() in BUCKETS]
    _need(len(selected) == 128 and len(catalog['sources']) == 128, 'C128 catalog cardinality differs')
    expected = {scene['logical_prefix']+'/'+e['logical_file']:e for e in selected}
    binding_rows = [json.loads(line) for line in
                    (Path(scene['case_root'])/'bindings.jsonl').read_text().splitlines()]
    bindings = {b['path']:b for b in binding_rows}
    _need(len(bindings) == len(binding_rows) == 512, 'publication bindings incomplete')
    for record in inventory.values():
        binding = bindings.get(record['binding']['path'])
        _need(binding is not None and binding['version'] == record['binding']['version'] and
              binding['sha256'] == record['sha256'] and binding['bytes'] == record['bytes'],
              'strict publication binding differs')
    got = set()
    for source in catalog['sources']:
        logical = source['logical_path']
        _need(logical in expected and logical not in got, 'native catalog scope differs')
        got.add(logical)
        binding = bindings[logical]
        _need(source['version'] == binding['version'], 'native exact version differs')
        # The generic native catalog supplies physical path/stat rather than oracle answers.
        physical = source.get('path', source.get('local_path'))
        _need(physical == binding['source'], 'native catalog physical source differs')
        stat = Path(physical).stat()
        _need(source['size'] == stat.st_size and source['mtime_ns'] == stat.st_mtime_ns,
              'native physical stat validation differs')
    _need(got == set(expected), 'native catalog omits selected sources')
    journal = Path(scene['original_owner'])
    _need(journal.is_dir() and not journal.is_symlink(), 'owner journal directory invalid')
    members = list(journal.iterdir())
    _need({p.name for p in members} == {'state','owner.lock'} and
          all(p.is_file() and not p.is_symlink() for p in members), 'unexpected owner journal member')
    actual_journal = {p.name:_sha(p) for p in members}
    _need(actual_journal == scene['journal_digest'], 'scientific owner journal changed')
    return dict(complete=True, raw_sources=512, candidate_sources=128,
        checked_artifacts=artifacts, build_index=False, database_read=False,
        elapsed_ns=time.perf_counter_ns()-started)


def _modules(scene, paired=None):
    key = (scene['repo'], scene['runner'])
    if key not in _MODULES:
        runner = Path(scene['runner'])
        manifest_path = runner.parent/'runner-manifest.json'
        _need(_sha(manifest_path) == scene['runner_manifest_sha256'], 'frozen runner manifest changed')
        manifest = _load(manifest_path)
        for name in ('route3_selector_ab.py','route3_global_hot_ab.py'):
            _need(_sha(runner/name) == manifest[name], 'frozen runner helper changed')
        sys.path.insert(0,str(runner))
        shared = importlib.import_module('route3_global_hot_ab')
        _need(Path(shared.__file__).resolve() == (runner/'route3_global_hot_ab.py').resolve(), 'wrong HDF timed helper import')
        modules = shared.ab.frozen_modules(Path(scene['source_stage']),Path(scene['repo']))
        _MODULES[key] = (shared,modules)
    shared,modules = _MODULES[key]
    remote = modules['route3_iotdb']
    # Explicitly retain the frozen paired IPC implementation. Do not let an
    # import of the maintained strict small_runner silently change this clock.
    chosen = paired if paired is not None else modules['paired']
    if not hasattr(remote,'_resource_original_dependencies'):
        remote._resource_original_dependencies = remote._dependencies
    _,publisher = remote._resource_original_dependencies()
    remote._dependencies = lambda:(chosen,publisher)
    return shared,modules


def _case(scene, modules):
    root = Path(scene['case_root']); report = _load(root/'report.json')
    bindings = [json.loads(line) for line in (root/'bindings.jsonl').read_text().splitlines()]
    expected = {}
    for item in scene['expected_matches']:
        _need(item['unit'].startswith('dataset:'), 'unexpected HDF oracle unit')
        expected[item['logical_file'],item['ordinal'],item['unit'][8:]] = item['native_oracle']
    return dict(report=report, bindings={(b['path'],b['version']):b for b in bindings},
        expected={('latest',(0,0,0,0)):expected})


class _Native:
    def __init__(self, scene, shared, modules):
        self.scene,self.shared,self.modules = scene,shared,modules
        paths = scene['native_artifacts']
        config = dict(catalog_path=next(p for p in paths if p.endswith('/catalog.json')),
            index_path=next(p for p in paths if p.endswith('/catalog.sqlite')), build_index=False,
            cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)
        self.native = modules['route3_native'].NativeContext(scene['native_library'],config)
        self.case = _case(scene,modules)
    def close(self):
        self.native.close()
    def __enter__(self): return self
    def __exit__(self,*args): self.close()


def open_hdf_native(scene, paired=None, env=None, modules=None):
    if modules is None:
        shared,modules = _modules(scene,paired)
    else:
        shared = modules['shared']
    return _Native(scene,shared,modules)


def _validated(row,scene):
    _need(row.get('oracle_verified') is True and row.get('owned_output_bytes') == 40960,
          'owned HDF output/oracle validation failed')
    row['actual_matches'] = deepcopy(scene['expected_matches'])
    row['candidate_sources'] = 128
    row['backing_N'] = 512
    return row


def query_hdf_native(native, scene):
    return _validated(native.shared.timed_query('FS-index',native.native,None,native.case,
                      scene['selector'],native.modules),scene)


def query_hdf_iotdb(paired, context, scene, offset):
    shared,modules = _modules(scene,paired)
    context['route3_profile_offset'] = offset
    row = shared.timed_query('typed',None,context,_case(scene,modules),scene['selector'],modules)
    return _validated(row,scene),context['route3_profile_offset']
