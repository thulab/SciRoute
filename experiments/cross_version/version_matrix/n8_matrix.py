#!/usr/bin/env python3
"""Paired N8 version matrix. No production code or old result is modified."""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import time
from urllib.parse import quote

VERSIONS = (1, 2, 4, 8)
SYSTEMS = ('V04', 'V03', 'FS-scan', 'FS-index')
FORMATS = ('hdf5', 'grib')
SCENARIOS = ('identical', 'single_unit')
SELECTIONS = ('latest', 'exact', 'all')
DAEMON_SHAS = {
    'V04': '83b90b15181f3311e5b70bf8dfbd334295b3e5422bf1ded388a9948ce2144305',
    'V03': '5e23a4a6a9b24e91d7d8b0f99227b752668d943d0e5f7c2ebf686130eea2e6f8'}


def validate_spec(spec):
    expected = {'protocol': 'iotdbfs-n8-version-matrix-spec/1', 'N': 8, 'K': 2, 'D': 4, 'depth': 2,
        'version_counts': list(VERSIONS), 'scenarios': list(SCENARIOS), 'formats': list(FORMATS),
        'selections': list(SELECTIONS), 'exact_ordinal': 1, 'systems': list(SYSTEMS),
        'campaigns': 3, 'repeats': 5, 'exclude_repeat': 0}
    require(all(spec.get(key) == value for key, value in expected.items()), 'frozen matrix spec differs')
    return spec


def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ordinals(V, selection):
    require(V in VERSIONS and selection in SELECTIONS, 'invalid matrix selection')
    return list(range(1, V + 1)) if selection == 'all' else [1 if selection == 'exact' else V]


def selected_entries(manifest, scenario, fmt, V, selection):
    selected = set(ordinals(V, selection))
    rows = [e for e in manifest['entries'] if e['scenario'] == scenario and
            e['format'] == fmt and e['ordinal'] in selected]
    require(len(rows) == 8 * len(selected), 'selected corpus lacks N8 versions')
    return rows


def oracle_for(manifest, scenario, fmt, V, selection):
    wanted = manifest['controls']['query']
    result = {}
    for entry in selected_entries(manifest, scenario, fmt, V, selection):
        for unit in entry['units']:
            if all(unit['attributes'][k] == v for k, v in wanted.items()):
                key = (entry['logical_file'], entry['ordinal'], unit['dataset'] if fmt == 'hdf5' else unit['offset'])
                require(key not in result, 'duplicate oracle identity')
                result[key] = unit['oracle']
    require(len(result) == 2 * len(ordinals(V, selection)), 'K2 oracle differs')
    return result


def discovery_requests(paired, template, namespace, fmt, selection):
    prefix = namespace['logical_prefix'] + '/' + fmt
    if selection != 'exact':
        request = paired.discovery_request(prefix, template)
        request['request']['sources']['versions'] = {'kind': selection}
        return [request]
    bindings = sorted((b for b in namespace['bindings'] if b['format'] == fmt and b['ordinal'] == 1),
                      key=lambda b: b['logical_file'])
    require(len(bindings) == 8, 'Exact needs eight path-specific version bindings')
    requests = []
    for b in bindings:
        request = paired.discovery_request(prefix, template)
        request['request']['sources'] = paired.selector(prefix, fmt, b['version'], b['logical_path'])
        requests.append(request)
    return requests


def check_native(row, expected, fmt, V, selection, system):
    actual = {}
    for item in row['units']:
        key = (item['logical_file'], item['ordinal'], item['dataset'] if fmt == 'hdf5' else item['offset'])
        require(key not in actual, 'native duplicated version/unit')
        actual[key] = item
    require(set(actual) == set(expected) and row['selected_units'] == len(expected), 'native version result set differs')
    for key, oracle in expected.items():
        item = actual[key]
        mask = oracle.get('mask_sha256', hashlib.sha256(b'\1' * oracle['elements']).hexdigest())
        require(item['rows'] == oracle['elements'] and item['value_sha256'] == oracle['value_sha256'] and
                item['mask_sha256'] == mask, 'native values or masks differ')
        require(item['path'] == 'v%03d/%s' % (key[1], key[0]), 'native physical/version binding differs')
    if system == 'FS-scan':
        require(row['enumerated_files'] == 8 * len(ordinals(V, selection)) and
                row['candidate_units'] == 32 * len(ordinals(V, selection)), 'native candidate scan differs')


def iotdb_query(paired, context, namespace, manifest, template, fmt, selection, offset):
    import numpy as np
    prefix = namespace['logical_prefix'] + '/' + fmt
    bindings = {(b['logical_path'], b['version']): b for b in namespace['bindings'] if b['format'] == fmt}
    expected = oracle_for(manifest, namespace['scenario'], fmt, namespace['V'], selection)
    calls, rows, value_calls, parts = [], [], [], []
    started = time.perf_counter_ns()
    for request in discovery_requests(paired, template, namespace, fmt, selection):
        response, batches, stats, offset = paired.profile_call(context, request, offset)
        calls.append(stats)
        rows.extend(paired.discovered(response, batches, fmt, prefix))
    ready = time.perf_counter_ns()
    if fmt == 'hdf5':
        requests = [{'kind': 'typed', 'request': {
            'sources': paired.selector(prefix, fmt, r['version'], r['path']),
            'subset': {'projection': ['hdf5.value:' + quote(r['dataset'], safe='')]}}} for r in rows]
    elif selection == 'exact':
        requests = [{'kind': 'typed', 'request': {
            'sources': paired.selector(prefix, fmt, version, path),
            'subset': {'projection': ['grib.data.value'], 'predicate': template['query_template']['predicate']}}}
            for path, version in sorted({(r['path'], r['version']) for r in rows})]
    else:
        sources = paired.selector(prefix, fmt)
        sources['paths'] = [{'kind': 'exact', 'path': path} for path in sorted({r['path'] for r in rows})]
        sources['versions'] = {'kind': selection}
        requests = [{'kind': 'typed', 'request': {'sources': sources, 'subset': {
            'projection': ['grib.data.value'], 'predicate': template['query_template']['predicate']}}}]
    for request in requests:
        _, batches, stats, offset = paired.profile_call(context, request, offset, True)
        value_calls.append(stats)
        parts.extend(batches)
    ended = time.perf_counter_ns()
    # Independent hashes/provenance and oracle comparisons are outside clocks.
    actual, source_keys = {}, {}
    for row in rows:
        binding = bindings.get((row['path'], row['version']))
        require(binding is not None, 'query selected an unrecorded exact version')
        key = (binding['logical_file'], binding['ordinal'], row['dataset'] if fmt == 'hdf5' else row['offset'])
        require(key not in actual and (row['path'], row['version']) not in source_keys, 'query duplicated a version/unit')
        actual[key] = row
        source_keys[(row['path'], row['version'])] = key
    require(set(actual) == set(expected), 'IoTDB discovery/original version oracle differs')
    owned = defaultdict(list)
    for part in parts:
        meta = part['meta']
        require(meta.get(b'iotdbfs.sources') == b'1', 'value provenance must identify one exact source')
        source = (meta[b'iotdbfs.source.0.path'].decode(), int(meta[b'iotdbfs.source.0.version']))
        require(source in source_keys, 'value batch came from an unselected source/version')
        owned[source_keys[source]].append(part)
    require(set(owned) == set(expected), 'value stream omitted a scientific version')
    for key, oracle in expected.items():
        values = np.concatenate([p['array'] for p in owned[key]])
        mask = np.concatenate([p['mask'] for p in owned[key]])
        require(len(values) == oracle['elements'] and hashlib.sha256(values.tobytes()).hexdigest() == oracle['value_sha256'],
                'IoTDB values differ from independent native oracle')
        require(hashlib.sha256(mask.tobytes()).hexdigest() == oracle.get('mask_sha256', hashlib.sha256(b'\1' * len(mask)).hexdigest()),
                'IoTDB validity mask differs')
    return {'total_ns': ended - started - sum(c['instrumentation_ns'] for c in calls + value_calls),
            'discovery_client_ns': ready - started - sum(c['instrumentation_ns'] for c in calls),
            'selected_value_client_ns': ended - ready - sum(c['instrumentation_ns'] for c in value_calls),
            'selected_units': len(rows), 'selected_owned_rows': sum(o['elements'] for o in expected.values()),
            'discovery_requests': len(calls), 'value_requests': len(value_calls),
            'discovery_calls': calls, 'value_calls': value_calls, 'oracle_verified': True}, offset


def prepare_fs(args, manifest, paired, env, report, checkpoint):
    from experiments.cross_version.pilot import corpus
    report['fs'] = []
    inodes = set()
    for scenario in SCENARIOS:
        for V in VERSIONS:
            for fmt in FORMATS:
                root = args.work / 'fs' / scenario / ('V%d' % V) / fmt
                root.mkdir(parents=True)
                files, start = [], time.perf_counter_ns()
                for entry in selected_entries(manifest, scenario, fmt, V, 'all'):
                    source = corpus.resolve(args.input, manifest, entry)
                    destination = root / ('v%03d' % entry['ordinal']) / entry['logical_file']
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with source.open('rb', buffering=0) as raw, destination.open('xb', buffering=0) as target:
                        shutil.copyfileobj(raw, target, 1024 * 1024)
                        os.fsync(target.fileno())
                    stat = destination.stat()
                    require(stat.st_nlink == 1 and (stat.st_dev, stat.st_ino) not in inodes and not destination.is_symlink(),
                            'FS copies must be independent regular files')
                    inodes.add((stat.st_dev, stat.st_ino))
                    require(sha(destination) == entry['sha256'], 'FS copied byte hash differs')
                    files.append({'path': str(destination), 'sha256': entry['sha256'], 'bytes': stat.st_size,
                                  'allocated_bytes': stat.st_blocks * 512, 'inode': stat.st_ino, 'nlink': stat.st_nlink})
                copy_ns = time.perf_counter_ns() - start
                index = root.parent / (fmt + '.sqlite')
                worker = paired.NativeWorker(args.native, env)
                try:
                    build = worker.call({'op': 'build', 'root': str(root), 'format': fmt, 'version_count': V, 'index': str(index)})
                finally:
                    worker.close()
                report['fs'].append({'scenario': scenario, 'V': V, 'format': fmt, 'root': str(root), 'index': str(index),
                                     'files': files, 'copy_fsync_verify_ns': copy_ns, 'index_build': build, 'index_sha256': sha(index)})
                checkpoint()
                print(json.dumps({'event': 'fs_prepared', 'scenario': scenario, 'V': V, 'format': fmt}), flush=True)


def campaign_order(campaign):
    groups = [(s, V) for s in SCENARIOS for V in VERSIONS]
    start = (campaign * 3) % len(groups)
    return groups[start:] + groups[:start]


def summarize(records):
    cells = defaultdict(list)
    for row in records:
        cells[tuple(row[k] for k in ('scenario', 'V', 'format', 'selection', 'system'))].append(row)
    require(len(cells) == 192 and len(records) == 2880, 'incomplete query matrix')
    summaries = []
    for key, rows in sorted(cells.items()):
        require(len(rows) == 15 and {(r['campaign'], r['repeat']) for r in rows} == {(c, r) for c in range(3) for r in range(5)}, 'cell replication differs')
        warm = [r for r in rows if r['repeat'] > 0]
        summaries.append(dict(zip(('scenario', 'V', 'format', 'selection', 'system'), key),
            observations=15, warm_observations=12, warm_p50_ms=statistics.median(r['total_ns'] for r in warm) / 1e6,
            warm_min_ms=min(r['total_ns'] for r in warm) / 1e6, warm_max_ms=max(r['total_ns'] for r in warm) / 1e6,
            campaign_warm_p50_ms=[statistics.median(r['total_ns'] for r in warm if r['campaign'] == c) / 1e6 for c in range(3)],
            first_by_campaign_ms=[next(r['total_ns'] for r in rows if r['campaign'] == c and r['repeat'] == 0) / 1e6 for c in range(3)]))
    lookup = {(r['scenario'], r['V'], r['format'], r['selection'], r['system']): r for r in summaries}
    comparisons = []
    for scenario in SCENARIOS:
        for fmt in FORMATS:
            for selection in SELECTIONS:
                for V in VERSIONS[1:]:
                    for comparator in ('FS-scan', 'FS-index', 'V03'):
                        a, a1 = [lookup[(scenario, v, fmt, selection, 'V04')] for v in (V, 1)]
                        b, b1 = [lookup[(scenario, v, fmt, selection, comparator)] for v in (V, 1)]
                        t, t1, f, f1 = [r['warm_p50_ms'] for r in (a, a1, b, b1)]
                        per_campaign = []
                        for c in range(3):
                            tc, t1c, fc, f1c = [r['campaign_warm_p50_ms'][c] for r in (a, a1, b, b1)]
                            per_campaign.append({'campaign': c, 'V04_total_ms': tc, 'control_total_ms': fc,
                                'V04_increment_ms': tc - t1c, 'control_increment_ms': fc - f1c,
                                'relative_advantage_change': (fc / tc) / (f1c / t1c), 'V04_faster': tc < fc})
                        comparisons.append({'scenario': scenario, 'format': fmt, 'selection': selection, 'V': V, 'control': comparator,
                            'V04_total_ms': t, 'control_total_ms': f, 'V04_increment_ms': t - t1, 'control_increment_ms': f - f1,
                            'V04_growth': t / t1, 'control_growth': f / f1, 'speedup_control_over_V04': f / t,
                            'relative_advantage_change': (f / t) / (f1 / t1), 'campaigns': per_campaign,
                            'pilot_all_campaigns_faster_and_relative_gain': all(p['V04_faster'] and p['relative_advantage_change'] > 1 for p in per_campaign)})
    return {'complete': True, 'cells': summaries, 'comparisons': comparisons,
            'inference': 'pilot only; three campaign units; no significance or automatic mainline decision; retain all formats/scenarios/selections'}


def run(args):
    from experiments.cross_version.pilot import corpus
    from experiments.cross_version.pilot import publish
    from common import load_manifest
    from experiments.cross_version.version_directory import v03_n128 as prior
    from experiments.cross_version.version_directory.v03_startup_wrapper import load_runner, adjust_mount
    from experiments.cross_version.version_directory.v04_n128 import journal_digest
    from experiments.cross_version.version_matrix.n8_matrix_prepare import prepare_all, _probe
    from experiments.cross_version.version_directory.v00_head_probe import save_json
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    require(not args.work.exists() and not args.report.exists(), 'use new work/report, never silently retry')
    args.work.mkdir(parents=True, mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {'protocol': 'iotdbfs-n8-version-matrix/1', 'complete': False, 'credentials_recorded': False,
        'spec': validate_spec(json.loads(args.spec.read_text())), 'spec_sha256': sha(args.spec), 'controller_sha256': sha(__file__),
        'source_binding_sha256': sha(args.source_binding),
        'namespaces': [], 'records_file': str(args.work / 'queries.jsonl'), 'query_records': 0,
        'scope': 'N8 pilot, independent V03/V04 namespaces; same scientific byte/version matrix and native FS copied histories; full app-operation clocks'}
    def checkpoint():
        save_json(args.report, report)
        args.report.chmod(0o600)
    checkpoint()
    require(all(sha(getattr(args, 'daemon_' + system.lower())) == digest for system, digest in DAEMON_SHAS.items()), 'pinned daemon differs')
    runner = load_runner(args.frozen_runner)
    adjust_mount(runner.sf, 900, args.work / 'startup-receipts.jsonl')
    descriptor, _, env = publish.runtime(args.environment, args.source_pid, prior.DATABASE)
    require(env['IOTDB_PORT'] == '33667' and env['IOTDB_HOST'] in ('127.0.0.1', 'localhost'), 'unexpected endpoint')
    manifest = corpus.load(args.input)
    report['raw_pre_audit'] = corpus.verify(args.input)
    require(all(manifest['controls'][k] == v for k, v in {'N': 8, 'K': 2, 'D': 4, 'depth': 2}.items()), 'pilot controls differ')
    report['corpus_sha256'] = manifest['manifest_sha256']
    report['binaries'] = {s: sha(getattr(args, 'daemon_' + s.lower())) for s in ('V04', 'V03')}
    report['native_sha256'] = sha(args.native)
    templates = {fmt: load_manifest(manifest['bases'][fmt]['root']) for fmt in FORMATS}
    prepare_fs(args, manifest, runner, env, report, checkpoint)
    records = []
    with (args.work / 'queries.jsonl').open('x') as output:
        try:
            with prior.sync_window(args, report):
                prepare_all(args, report, env, runner, manifest, checkpoint)
                require(len(report['namespaces']) == 16, 'namespace matrix incomplete')
                for campaign in range(3):
                    for group_index, (scenario, V) in enumerate(campaign_order(campaign)):
                        shift = (campaign + group_index) % 4
                        systems = SYSTEMS[shift:] + SYSTEMS[:shift]
                        for system in systems:
                            root = args.work / ('c%d-%s-V%d-%s' % (campaign, scenario, V, system))
                            cell_order = [(fmt, selection) for fmt in FORMATS for selection in SELECTIONS]
                            cell_order = cell_order[campaign:] + cell_order[:campaign]
                            def append(row, fmt, selection, repeat):
                                row.update(system=system, scenario=scenario, V=V, format=fmt, selection=selection,
                                           campaign=campaign, repeat=repeat, warm=repeat > 0)
                                output.write(json.dumps(row, separators=(',', ':'), allow_nan=False) + '\n')
                                output.flush()
                                records.append(row)
                                report['query_records'] += 1
                            if system in ('V03', 'V04'):
                                namespace = next(n for n in report['namespaces'] if (n['system'], n['scenario'], n['V']) == (system, scenario, V))
                                before = journal_digest(Path(namespace['journal']))
                                with runner.sf.private_mount(Path(namespace['recover_launch']['path']), root, env, args.writer_lock) as context:
                                    offset = 0
                                    if campaign == 0:
                                        for b in namespace['bindings']:
                                            require(sha(publish.history_path(context['mount'], b['logical_path'], b['version'])) == b['sha256'], 'recovered exact file hash differs')
                                        namespace['recovery_full_file_hashes'] = len(namespace['bindings'])
                                        for fmt in FORMATS:
                                            prefix = namespace['logical_prefix'] + '/' + fmt
                                            request = runner.discovery_request(prefix, templates[fmt], False)
                                            request['request']['sources']['versions'] = {'kind': 'all'}
                                            response, batches, _, offset = runner.profile_call(context, request, offset)
                                            rows = runner.discovered(response, batches, fmt, prefix)
                                            wanted = {(b['logical_path'], b['version']) for b in namespace['bindings'] if b['format'] == fmt}
                                            require(len(rows) == 32 * V and {(r['path'], r['version']) for r in rows} == wanted,
                                                    'recovered all-scientific version inventory differs')
                                        namespace['recovery_scientific_units'] = 64 * V
                                    for fmt, selection in cell_order:
                                        for repeat in range(5):
                                            row, offset = iotdb_query(runner, context, namespace, manifest, templates[fmt], fmt, selection, offset)
                                            append(row, fmt, selection, repeat)
                                        os.fsync(output.fileno())
                                    namespace.setdefault('query_mounts', []).append({'campaign': campaign, 'startup_ns': context['startup_ns']})
                                require(journal_digest(Path(namespace['journal'])) == before, 'read-only matrix queries changed owner journal')
                                probe_root = root / 'post-query-head'
                                probe_root.mkdir(mode=0o700)
                                head, receipt = _probe(getattr(args, 'probe_' + system.lower()), 'head', namespace['prefix'], env, probe_root)
                                require(head == namespace['head'], 'matrix query changed published head')
                                namespace['query_mounts'][-1]['head_receipt'] = receipt
                            else:
                                worker = runner.NativeWorker(args.native, env)
                                try:
                                    for fmt, selection in cell_order:
                                        fs = next(f for f in report['fs'] if (f['scenario'], f['V'], f['format']) == (scenario, V, fmt))
                                        expected = oracle_for(manifest, scenario, fmt, V, selection)
                                        for repeat in range(5):
                                            row = worker.call({'op': 'query', 'root': fs['root'], 'format': fmt, 'version_count': V,
                                                'mode': 'scan' if system == 'FS-scan' else 'index', 'selection': selection,
                                                'exact_ordinal': 1, 'index': fs['index'], 'predicate': manifest['controls']['query'], 'values': True})
                                            check_native(row, expected, fmt, V, selection, system)
                                            row['oracle_verified'] = True
                                            append(row, fmt, selection, repeat)
                                        os.fsync(output.fileno())
                                finally:
                                    worker.close()
                            report['last_completed_block'] = {'campaign': campaign, 'scenario': scenario, 'V': V, 'system': system}
                            checkpoint()
                            print(json.dumps({'event': 'query_block_complete', **report['last_completed_block'], 'records': len(records)}), flush=True)
                report['raw_post_audit'] = corpus.verify(args.input)
                for fs in report['fs']:
                    require(sha(fs['index']) == fs['index_sha256'], 'FS index changed during queries')
                    require(all(sha(f['path']) == f['sha256'] for f in fs['files']), 'FS histories changed during queries')
                analysis = summarize(records)
                save_json(args.work / 'analysis.json', analysis)
                report['analysis_sha256'] = sha(args.work / 'analysis.json')
                report['queries_sha256'] = sha(args.work / 'queries.jsonl')
                report['complete'] = True
        except BaseException as error:
            report['complete'] = False
            report['failure'] = {'type': type(error).__name__, 'message': 'matrix failed; inspect private logs; no automatic retry'}
            raise
        finally:
            checkpoint()
    print(json.dumps({'complete': report['complete'], 'records': len(records), 'service_restored': report.get('original_service_restored')}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('input', 'work', 'report', 'spec', 'source-binding', 'environment', 'frozen-runner', 'native', 'daemon-v03', 'daemon-v04',
                 'probe-v03', 'probe-v04', 'payload-probe-v04', 'writer-lock', 'runtime', 'release'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-pid', type=int, default=1001546)
    parser.add_argument('--datanode-pid', type=int, required=True)
    run(parser.parse_args())


if __name__ == '__main__':
    main()
