"""Filesystem-native HDF5 validation on two existing, sealed FM1 cases.

No corpus generation, scientific publication, index rebuild, or new case. The
supervisor owns the original database maintenance/resource window. This worker
adds only exact-version native-file locators, then runs the unchanged FM1 loop.
One block is descriptive validation, not replacement of the three-block FM1.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import time

from experiments.fixed_matrix import run
from experiments.fixed_matrix.replay import SOURCE_PINS
from experiments.identity.model import digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

GROUPS = ('FM1-hdf5-N8-V1-C8-S0-base-latest',
          'FM1-hdf5-N128-V1-C128-S0-base-latest')
PROTOCOL = 'iotdbfs-fixed-native-filesystem-probe/1'
EXECUTION_LOOP_SHA = '77381ae7ccf87c33e085714c51742957d110e7e10315f247b63076d4841ec695'
need = run.need


def source_proof(args, *, group_ids=None, campaigns=(0,)):
    """Offline checks only; never import the server's connection environment."""
    source = args.source_collection
    need(source.name == 'collection-r2', 'probe requires the sealed r2 collection')
    for relative, expected in SOURCE_PINS.items():
        need(file_sha(source / relative) == expected, 'sealed source changed: ' + relative)
    need(file_sha(args.repo / 'experiments/fixed_matrix/catalog.json') == run.CATALOG_SHA,
         'fixed experiment catalog changed')
    old = read_json(source / 'prepared.json')
    need(old['product']['sha256'] == '36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9'
         and old['native_library_sha256'] == run.NATIVE_SHA, 'source implementation binding differs')
    # Use the accepted campaign-2 factored collection loop. The archived r2
    # controller remains unchanged; only its orchestration was later factored.
    frozen = ('experiments/fixed_matrix/client.py', 'experiments/fixed_matrix/native.py',
              'experiments/fixed_matrix/run.py', 'experiments/fixed_matrix/catalog.json')
    for relative in frozen:
        expected = old['sources'][relative]
        need(file_sha(source / 'source' / relative) == expected, 'archived runner changed: ' + relative)
        actual = EXECUTION_LOOP_SHA if relative == 'experiments/fixed_matrix/run.py' else expected
        need(file_sha(args.repo / ('fs_index/native.py' if relative == 'experiments/fixed_matrix/native.py' else relative)) == actual, 'scientific runner changed: ' + relative)
    build = read_json(args.build_binding)
    product_sha = file_sha(args.binary)
    need(build.get('complete') is True and build.get('binary_sha256') == product_sha,
         'new product build is not accepted')
    if 'binary' in build:
        need(Path(build['binary']).resolve() == args.binary, 'build binary path differs')
    need(file_sha(args.library) == run.NATIVE_SHA, 'frozen FS helper changed')
    need(file_sha(args.inspector) == run.infrastructure.INSPECTOR_SHA, 'head inspector changed')
    bindings = read_json(source / 'bindings.json')
    report = read_json(source / 'report.json')
    source_registry = Registry(source / 'registry')
    catalog = read_json(args.repo / 'experiments/fixed_matrix/catalog.json')
    groups = {g['group_id']: g for g in catalog['groups']}
    entries = []
    selected = GROUPS if group_ids is None else tuple(group_ids)
    need(selected and len(selected) == len(set(selected)), 'fixed group selection differs')
    for gid in selected:
        group = groups[gid]
        need(group['format'] == 'hdf5' and group['collection_tier'] == 'primary'
             and (selected != GROUPS or (group['V_stored'] == 1 and group['size_profile'] == 'S0'
                  and group['selection'] == 'latest' and group['C_paths'] == group['N'])),
             'selected fixed case differs')
        bound = bindings[gid]
        corpus = source_registry.get_corpus(bound['corpus_id'])
        case = source_registry.get_case(bound['case_id'])
        need(case['corpus_id'] == corpus['corpus_id']
             and case['expected_matches'] == bound['expected_matches']
             and digest(case['expected_matches']) == bound['oracle_sha256'], 'fixed case/oracle changed')
        matched = [c for c in report['corpora'] if c['corpus_id'] == corpus['corpus_id']]
        need(len(matched) == 1, 'independent corpus not uniquely bound')
        item = matched[0]
        base = source / 'corpora' / item['corpus_key']
        prep = read_json(base / 'preparation.json')
        need(prep['complete'] and file_sha(base / 'index.sqlite') == item['index_sha256'] == prep['index_sha256'],
             'frozen FS index changed')
        need(file_sha(base / 'input/manifest.json') == item['raw_manifest_sha256']
             == corpus['source_evidence']['manifest_sha256'], 'raw manifest binding changed')
        need(file_sha(base / 'publication.json') == prep['publication_receipt_sha256']
             == corpus['publication_evidence']['receipt_sha256'], 'scientific publication receipt changed')
        need(read_json(base / 'publication.json')['head'] == item['head'], 'original published head differs')
        journal = run.infrastructure.journal_snapshot(base / 'owner')
        need(journal == item['journal_digest'], 'archived owner journal changed')
        manifest = read_json(base / 'input/manifest.json')
        validation = run.verify_raw(manifest, base / 'input', base / 'fs')
        artifacts = {p: file_sha(base / p) for p in
                     ('input/manifest.json', 'catalog.json', 'index.sqlite', 'publication.json', 'preparation.json')}
        entries.append(dict(group_id=gid, corpus_key=item['corpus_key'], corpus_id=corpus['corpus_id'],
            case_id=case['case_id'], oracle_sha256=bound['oracle_sha256'], source_root=str(base),
            archived_owner_journal=str(base / 'owner'), archived_journal_digest=journal,
            original_head=item['head'], source_artifacts=artifacts, raw_validation=validation))
    return dict(protocol=PROTOCOL, complete=True, source_collection=str(source), source_pins=SOURCE_PINS,
        product=dict(path=str(args.binary), sha256=product_sha, binding=str(args.build_binding),
                     binding_sha256=file_sha(args.build_binding)),
        native_library_sha256=run.NATIVE_SHA, inspector=str(args.inspector), cases=entries,
        expected_queries=len(selected) * len(campaigns) * 36, campaigns=list(campaigns), warmup_per_arm=2, measured_per_arm=10,
        scientific_publication=False, index_rebuilt=False, new_cases=False,
        mutation='exact-version native-file location bindings only; original journals stay archived')


def filesystem_launcher(path, binary, prefix, journal, initialize, *, native_root, expected_sha):
    """Named, receipted settings change; the measured FM1 loop is unchanged."""
    need(not initialize, 'probe must never initialize a new namespace')
    receipt = run.infrastructure.launcher(path, binary, expected_sha, prefix, journal)
    old = 'SETTINGS=' + repr(receipt['settings']) + '\n'
    settings = dict(receipt['settings'], **{'query.hdf5.mode': 'native-hdf5',
                                           'storage.native_file_root': str(native_root)})
    source = path.read_text()
    need(source.count(old) == 1, 'launcher settings layout changed')
    path.write_text(source.replace(old, 'SETTINGS=' + repr(settings) + '\n', 1))
    receipt.update(settings=settings, sha256=file_sha(path), payload_backend='filesystem',
                   data_driver='bounded local-pread VFD; no database payload fallback')
    atomic_json(path.with_name(path.name + '.receipt.json'), receipt)
    return receipt


def build_plans(args, registry, report, proof):
    source = args.source_collection
    original = read_json(source / 'report.json')
    old_items = {item['corpus_id']: item for item in original['corpora']}
    original_bindings = read_json(source / 'bindings.json')
    old_registry = Registry(source / 'registry')
    catalog = {g['group_id']: g for g in read_json(args.repo / 'experiments/fixed_matrix/catalog.json')['groups']}
    plans, bindings = [], {}
    for entry in proof['cases']:
        gid = entry['group_id']
        original_item = old_items[entry['corpus_id']]
        base = Path(entry['source_root'])
        execution = args.work / 'corpora' / entry['corpus_key']
        execution.mkdir(parents=True, mode=0o700, exist_ok=False)
        active = execution / 'owner-active'
        shutil.copytree(base / 'owner', active)
        need(run.infrastructure.journal_snapshot(active) == entry['archived_journal_digest'],
             'copied active journal differs before binding')
        corpus = registry.register_corpus(old_registry.get_corpus(entry['corpus_id']))
        registry.register_case(old_registry.get_case(entry['case_id']))
        bound = deepcopy(original_bindings[gid]); bound['run_ids'] = {}; bindings[gid] = bound
        item = deepcopy(original_item)
        item.update(complete=False, mounts=[], source_root=str(base),
                    archived_owner_journal=str(base / 'owner'), active_owner_journal=str(active))
        report['corpora'].append(item)
        plans.append(dict(spec=corpus['parameters']['generation']['specification'], root=base,
            execution_root=execution, manifest=read_json(base / 'input/manifest.json'), fsroot=base / 'fs',
            journal=active, item=item, scientific_head=entry['original_head'],
            journal_digest=entry['archived_journal_digest'], corpus=corpus, groups=[catalog[gid]],
            cfg=dict(catalog_path=str(base / 'catalog.json'), index_path=str(base / 'index.sqlite'),
                     cache_files=8, cache_datasets=8, metadata_cache_bytes=2097152, raw_chunk_cache_bytes=262144),
            prep=read_json(base / 'preparation.json')))
    return plans, bindings


def bind_files(args, plan, env, launch):
    """Explicit preparation, outside every scientific query clock."""
    root = plan['execution_root'] / 'file-binding'; root.mkdir(mode=0o700)
    before, before_receipt = run.head(args.inspector, plan['item']['namespace'], env, root / 'head-before')
    need(before == plan['scientific_head'], 'namespace head differs before explicit file binding')
    config = root / 'binding.properties'; config.write_text(''); config.chmod(0o600)
    current = dict(env, IOTDB_CONFIG_FILE=str(config))
    for name in ('IOTDBFS_QUERY_PROFILE_JSONL', 'IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL',
                 'IOTDBFS_PUBLICATION_PROFILE_JSONL'):
        current.pop(name, None)
    logical_root = '/fm1/' + plan['spec']['corpus_key']
    started = time.perf_counter_ns()
    with (root / 'stdout.jsonl').open('x') as out, (root / 'private.stderr').open('x') as err:
        out_path, err_path = root / 'stdout.jsonl', root / 'private.stderr'
        out_path.chmod(0o600); err_path.chmod(0o600)
        completed = subprocess.run([launch['path'], 'native-file-bind', '--subtree', logical_root],
            cwd=args.repo, env=current, stdin=subprocess.DEVNULL, stdout=out, stderr=err, timeout=1800)
    elapsed = time.perf_counter_ns() - started
    need(completed.returncode == 0, 'native-file binding failed; no automatic retry')
    receipts = [json.loads(line) for line in out_path.read_text().splitlines() if line.startswith('{')]
    expected = {(e['binding']['path'], e['binding']['version']) for e in plan['corpus']['inventory']}
    sizes = {(e['binding']['path'], e['binding']['version']): e['bytes'] for e in plan['corpus']['inventory']}
    actual = {(r['path'], r['version']) for r in receipts}
    need(actual == expected and len(receipts) == len(expected), 'native-file binding inventory differs')
    for receipt in receipts:
        need(receipt.get('protocol') == 'iotdbfs-native-file-bind/1' and receipt.get('complete') is True
             and receipt.get('scientific_publication') is False
             and receipt.get('same_scientific_identity') is True
             and receipt.get('source_identity_before') == receipt.get('source_identity_after')
             and isinstance(receipt.get('source_identity_before'), str)
             and len(receipt['source_identity_before']) == 64
             and all(c in '0123456789abcdef' for c in receipt['source_identity_before'])
             and receipt.get('bytes') == sizes[(receipt['path'], receipt['version'])],
             'scientific descriptor/seal identity changed')
    after, after_receipt = run.head(args.inspector, plan['item']['namespace'], env, root / 'head-after')
    need(before['namespace'] == after['namespace'] and before['incarnation'] == after['incarnation']
         and after['seq'] >= before['seq'], 'binding changed namespace identity')
    journal_digest = run.infrastructure.journal_snapshot(plan['journal'])
    proof = dict(complete=True, preparation_ns=elapsed, scientific_publication=False,
        scientific_identities_unchanged=True, versions=len(receipts), source_identity_receipts=receipts,
        head_before=before, head_after=after, head_before_receipt=before_receipt, head_after_receipt=after_receipt,
        stdout_sha256=file_sha(out_path), active_owner_journal=str(plan['journal']),
        active_journal_digest=journal_digest,
        warning='original owner journal is archive only; future owner opens must use this active journal')
    atomic_json(root / 'receipt.json', proof)
    plan['scientific_head'] = after; plan['journal_digest'] = journal_digest
    plan['item'].update(head=after, journal_digest=journal_digest,
                        file_binding_receipt=str(root / 'receipt.json'), file_binding_receipt_sha256=file_sha(root / 'receipt.json'))
    return proof


def validate_filesystem_row(row, *, expected_units=2):
    need(row.get('mode_verified') is True and row.get('oracle_verified') is True
         and row.get('execution_mode') == 'native-hdf5', 'unverified native result')
    need(len(row.get('value_calls', [])) == expected_units, 'fixed query selected-unit count differs')
    for call in row['value_calls']:
        profile = call['server_profile']; mode = profile.get('server_hdf5_execution', {})
        counters = profile['counters']
        need(mode.get('mode') == 'native-hdf5' and mode.get('payload_backend') == 'filesystem',
             'native HDF5 did not use filesystem backend')
        need(counters.get('hdf5.native.filesystem.opens', 0) == 1
             and counters.get('hdf5.native.filesystem.calls', 0) > 0
             and counters.get('hdf5.native.filesystem.calls') == counters.get('hdf5.native.source.calls')
             and counters.get('hdf5.native.filesystem.bytes', 0) > 0
             and counters.get('hdf5.native.filesystem.bytes') == counters.get('hdf5.native.source.bytes'),
             'filesystem source was not exclusively used')
        for name in ('payload_map.block_reads', 'payload_map.data_batches', 'payload_map.hash_bytes',
                     'hdf5.selection.decoder_compiles', 'hdf5.layout.pages.calls'):
            need(not counters.get(name, 0), 'forbidden native-path work: ' + name)
    return row


def summarize(path, bindings):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    need(len(rows) == 72, 'small fixed probe requires all 72 rows')
    keys = {(r['group_id'], r['campaign'], r['arm'], r['repeat']) for r in rows}
    need(len(keys) == len(rows) and keys == {(g, 0, a, n) for g in GROUPS for a in run.ARMS for n in range(12)},
         'probe query coverage differs')
    for row in rows:
        bound = bindings[row['group_id']]
        need(row['case_id'] == bound['case_id'] and row['corpus_id'] == bound['corpus_id']
             and row['oracle_sha256'] == bound['oracle_sha256'] and row.get('ok') is True
             and row.get('oracle_verified') is True and row.get('mode_verified') is True
             and row['phase'] == ('warmup' if row['repeat'] < 2 else 'measurement')
             and all(type(row[k]) is int and row[k] > 0 for k in ('total_ns', 'discovery_ns', 'value_ns'))
             and row['total_ns'] == row['discovery_ns'] + row['value_ns'], 'invalid probe row')
        if row['arm'] == 'IoTDB-FS': validate_filesystem_row(row)
    cells = []
    for gid in GROUPS:
        arms = {}
        for arm in run.ARMS:
            measured = [r for r in rows if r['group_id'] == gid and r['arm'] == arm and r['phase'] == 'measurement']
            need(len(measured) == 10, 'warm sample coverage differs')
            arms[arm] = {stage: statistics.median(r[stage] for r in measured) / 1e6
                         for stage in ('total_ns', 'discovery_ns', 'value_ns')}
        db, fs = arms['IoTDB-FS'], arms['FS-index']
        cells.append(dict(group_id=gid, case_id=bindings[gid]['case_id'], corpus_id=bindings[gid]['corpus_id'],
            warm_median_ms=arms, R=db['total_ns'] / fs['total_ns'],
            Rd=db['discovery_ns'] / fs['discovery_ns'], Rq=db['value_ns'] / fs['value_ns']))
    return dict(complete=True, cells=cells, queries=72, measured_queries=60,
        statistic='one campaign: quotient of each arm\'s ten warm medians; no timing exclusions',
        limitation='single-block path/full-query probe; not the formal three-campaign FM1 or an advantage claim')


def worker(args):
    if args.gate_fd is not None:
        need(os.read(args.gate_fd, 1) == b'1', 'worker not admitted'); os.close(args.gate_fd)
    def cancelled(signum, frame):
        raise RuntimeError('filesystem probe cancelled; preserve evidence and restore in supervisor')
    for sig in (signal.SIGTERM, signal.SIGHUP): signal.signal(sig, cancelled)
    proof = source_proof(args)
    atomic_json(args.work / 'reuse-source.json', proof)
    frozen = read_json(args.work / 'prepared.json')
    need(isinstance(frozen.get('resource_id'), str) and len(frozen['resource_id']) == 64,
         'supervisor resource identity missing')
    sources = run.closure(args.repo)
    sources[str(Path(__file__).resolve().relative_to(args.repo))] = file_sha(__file__)
    supervisor = Path(__file__).with_name('verify.py')
    sources[str(supervisor.resolve().relative_to(args.repo))] = file_sha(supervisor)
    if 'sources' in frozen: need(frozen['sources'] == sources, 'supervisor source closure differs')
    frozen.update(product=proof['product'], native_library=str(args.library), native_library_sha256=run.NATIVE_SHA,
                  inspector=str(args.inspector), sources=sources, payload_backend='filesystem')
    atomic_json(args.work / 'prepared.json', frozen)
    paired, _, env, startup = run.dependencies(args.repo)
    startup.adjust_mount(paired.sf, 1800, args.work / 'startup-receipts.jsonl')
    run.PRODUCT_SHA = proof['product']['sha256']  # Explicit accepted-build override; frozen runner file unchanged.
    report = dict(protocol=PROTOCOL, complete=False, expected_queries=72, query_records=0, corpora=[],
        catalog_sha256=run.CATALOG_SHA, resource_id=frozen['resource_id'], source_collection=str(args.source_collection),
        product_sha256=run.PRODUCT_SHA, payload_backend='filesystem', scientific_publication=False,
        index_rebuilt=False, new_cases=False, credentials_recorded=False)
    atomic_json(args.work / 'report.json', report)
    try:
        registry = Registry(args.work / 'registry')
        plans, bindings = build_plans(args, registry, report, proof)
        by_namespace = {p['item']['namespace']: p for p in plans}
        def launch(path, binary, prefix, journal, initialize):
            return filesystem_launcher(path, binary, prefix, journal, initialize,
                native_root=by_namespace[prefix]['execution_root'] / 'native-files', expected_sha=run.PRODUCT_SHA)
        run.launcher = launch
        for plan in plans:
            root = plan['execution_root']
            bind_launch = launch(root / 'binding-launcher', args.binary, plan['item']['namespace'], plan['journal'], False)
            bind_files(args, plan, env, bind_launch)
        atomic_json(args.work / 'bindings.json', bindings)
        from experiments.fixed_matrix import client
        original_query = client.query_iotdb
        def query_with_filesystem_proof(*positional, **keywords):
            return validate_filesystem_row(original_query(*positional, **keywords))
        client.query_iotdb = query_with_filesystem_proof
        try:
            count = run.collect_campaigns(args, plans, bindings, registry, frozen, report, env, paired, campaigns=[0])
        finally:
            client.query_iotdb = original_query
        need(count == 72, 'probe did not finish all fixed query cells')
        final = source_proof(args)
        need(final == proof, 'sealed source changed during probe')
        need({relative: file_sha(args.repo / relative) for relative in sources} == sources,
             'executed source closure changed')
        for plan in plans:
            plan['item'].update(complete=True,
                final_raw_validation=run.verify_raw(plan['manifest'], plan['root'] / 'input', plan['fsroot']))
        summary = summarize(args.work / 'queries.jsonl', bindings)
        atomic_json(args.work / 'probe-summary.json', summary)
        report.update(complete=True, queries_sha256=file_sha(args.work / 'queries.jsonl'),
            bindings_sha256=file_sha(args.work / 'bindings.json'), summary_sha256=file_sha(args.work / 'probe-summary.json'),
            original_scientific_source_unchanged=True,
            restoration_required=True, note='supervisor restoration receipt required before accepting this probe')
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__, message=str(error)[:500]); raise
    finally:
        atomic_json(args.work / 'report.json', report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('repo', 'work', 'source-collection', 'binary', 'build-binding', 'library', 'inspector'):
        parser.add_argument('--' + key, type=Path, required=True)
    parser.add_argument('--gate-fd', type=int)
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    for key in ('repo', 'source_collection', 'binary', 'build_binding', 'library', 'inspector'):
        setattr(args, key, getattr(args, key).resolve(strict=True))
    args.work = args.work.resolve()
    args.work.mkdir(mode=0o700, parents=True, exist_ok=True)
    need(not args.work.is_relative_to(args.source_collection), 'probe work must not overwrite the sealed collection')
    if args.prepare_only:
        proof = source_proof(args)
        atomic_json(args.work / 'probe-plan.json', proof)
        print(json.dumps(dict(complete=True, scientific_queries=0, service_modified=False,
                             plan=str(args.work / 'probe-plan.json'))))
    else:
        worker(args)


if __name__ == '__main__':
    main()
