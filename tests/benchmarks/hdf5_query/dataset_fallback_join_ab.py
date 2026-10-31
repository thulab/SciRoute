#!/usr/bin/env python3
"""Read-only interleaved TRUE-fallback JOIN ablation with native HDF5 oracle.

Uses frozen published files. No ingestion, chmod, database or setting mutations.
Prime/correctness/warmup requests are excluded from all timed summaries.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import h5py

import geos_pilot as pilot
import geos_time_directory as directory
import geos_time_scale as scale
from grib_hdf5_boundary_probe import connection_environment, grib_request, hdf5_request, run_campaign


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def compare(op, value, name='standard_name'):
    return {'kind': 'compare', 'field': 'hdf5.attr:' + name, 'operator': op,
            'value': {'type': 'utf8', 'value': value}}


def logical(kind, *args):
    return {'kind': kind, 'operands': list(args)}


def evaluate(expression, attrs):
    """Independent three-valued native scalar oracle; None means SQL UNKNOWN."""
    if expression['kind'] == 'compare':
        actual = attrs.get(expression['field'].split(':', 1)[1])
        if actual is None:
            return None
        wanted = expression['value']['value']
        return {'lt': actual < wanted, 'gt': actual > wanted, 'eq': actual == wanted}[expression['operator']]
    values = [evaluate(e, attrs) for e in expression['operands']]
    if expression['kind'] == 'not':
        return None if values[0] is None else not values[0]
    if expression['kind'] == 'and':
        return False if False in values else (None if None in values else True)
    return True if True in values else (None if None in values else False)


def native_records(entries):
    records = []
    for entry in entries:
        with h5py.File(entry['raw'], 'r') as file:
            def visit(name, obj):
                if not isinstance(obj, h5py.Dataset):
                    return
                attrs = {}
                for attr in ('standard_name', 'units'):
                    if attr in obj.attrs:
                        value = obj.attrs[attr]
                        if isinstance(value, bytes):
                            value = value.decode('utf8')
                        pilot.check(isinstance(value, str), 'non-scalar native oracle attribute')
                        attrs[attr] = value
                records.append({'path': entry['path'], 'version': entry['version'],
                                'dataset_path': '/' + name, 'utc_second': entry['utc_second'],
                                'attributes': attrs})
            file.visititems(visit)
    return records


def identities(rows, include_object=False):
    return sorted(tuple(r[k] for k in (('path', 'version', 'dataset_path', 'object_id')
                                     if include_object else ('path', 'version', 'dataset_path'))) for r in rows)


def request(entries, expression=None, dataset_path='/T', lower=None):
    versions = {e['version'] for e in entries}
    # Frozen cohort files were published with different versions, so paths use
    # latest selection and every returned path/version is checked against pilot.
    selection = {'sources': {'paths': [{'kind': 'exact', 'path': e['path']} for e in entries]}}
    if len(versions) == 1:
        selection['sources']['versions'] = {'kind': 'exact', 'exact': next(iter(versions))}
    if expression is not None:
        selection['predicate'] = expression
    if dataset_path is not None:
        selection['dataset_path'] = dataset_path
    if lower is not None:
        selection['time_range'] = {'lower_unix_s': lower, 'upper_unix_s': lower + 1}
    return {'kind': 'dataset_catalog', 'request': selection}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after', 'pilot-dir', 'typed-pilot', 'writer-lock', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-pid', type=int, default=1001546)
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--repeats', type=int, default=5)
    args = parser.parse_args()
    if not 3 <= args.pairs <= 5 or not 5 <= args.repeats <= 10:
        raise ValueError('campaign must be bounded and meet repeat minimum')
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((args.pilot_dir / 'pilot.json').read_text())
    pilot.check(manifest['complete'] and len(manifest['sources']) == 16, 'requires frozen GEOS16')
    index = directory.build(args.pilot_dir / 'pilot.json', args.output / 'fs-index.json')
    entries = index['entries']
    target = next(e for e in entries if e['utc_second'] == 1309597200)
    priority = sorted(entries, key=lambda e: (abs(e['utc_second'] - target['utc_second']), e['utc_second']))
    native = native_records(entries)
    save(args.output / 'native-attribute-oracle.json', native)
    lt, gt = compare('lt', 'b'), compare('gt', 'b')
    correctness = {
        'lt-true': lt, 'lt-false': compare('lt', 'A'),
        'gt-true': compare('gt', 'A'), 'gt-false': compare('gt', 'z'),
        'not-order': logical('not', lt),
        'and-order': logical('and', lt, compare('eq', 'K', 'units')),
        'or-order': logical('or', gt, compare('eq', 'K', 'units')),
        'missing-name': compare('lt', 'z', 'iotdbfs_definitely_absent_20261011'),
        'not-missing-name': logical('not', compare('lt', 'z', 'iotdbfs_definitely_absent_20261011'))}
    timed = {'fallback-hit': (lt, None), 'fallback-miss': (compare('gt', 'z'), None),
             'equality': (scale.PREDICATE, None), 'time-hit': (scale.PREDICATE, target['utc_second']),
             'time-miss': (scale.PREDICATE, target['utc_second'] + 3600)}
    report = {'protocol': 'iotdbfs-fallback-join-ab/1', 'complete': False,
        'database': manifest['database'], 'pairs': args.pairs, 'repeats': args.repeats,
        'binary_sha256': {v: pilot.sha(getattr(args, v)) for v in ('before', 'after')},
        'index_build_ns': index['build_ns'], 'observations': [], 'typed_controls': [],
        'semantics': 'Independent interleaved mounts, warm OS/IoTDB caches, discovery TTL=0. Native oracle/index/mount/prime/correctness/warmup excluded; timed requests still revalidate manifests/markers, authorize, and run both full-source COUNTs.'}
    object_map = {}
    os.environ['GEOS_FUSE_OPTIONS'] = 'attr_timeout=0,entry_timeout=0'
    save(args.output / 'report.json', report)
    for pair in range(args.pairs):
        for variant in (('before', 'after') if pair % 2 == 0 else ('after', 'before')):
            for count in ((1, 16) if pair % 2 == 0 else (16, 1)):
                cohort = sorted(priority[:count], key=lambda e: e['utc_second'])
                keys = {(e['path'], e['version']) for e in cohort}
                records = [r for r in native if (r['path'], r['version']) in keys]
                run = args.output / f'{variant}-{pair}-n{count}'
                run.mkdir()
                env = pilot.connection_environment(args.source_pid, manifest['database'])
                env.update(IOTDB_CONFIG_FILE=str(args.pilot_dir / 'profile.properties'),
                           IOTDBFS_NAMESPACE_LOCK=str(args.writer_lock.resolve()),
                           IOTDBFS_QUERY_PROFILE_JSONL=str(run / 'profile.jsonl'))
                socket = Path(f'/tmp/fjoin-{os.getpid()}-{variant}-{pair}-{count}.sock')
                env['IOTDBFS_QUERY_SOCKET'] = str(socket)
                process, log = pilot.mount(getattr(args, variant), run / 'mount', env, socket)
                emitted = []
                def submit(case, expression, dataset_path, lower, repeat):
                    expected = [r for r in records if (dataset_path is None or r['dataset_path'] == dataset_path)
                                and (lower is None or lower <= r['utc_second'] < lower + 1)
                                and (expression is None or evaluate(expression, r['attributes']) is True)]
                    response, elapsed = pilot.control(socket, request(cohort, expression, dataset_path, lower))
                    actual = response['datasets']
                    pilot.check(identities(actual) == identities(expected), 'native residual object identity mismatch: ' + case)
                    for row in actual:
                        identity = (row['path'], row['version'], row['dataset_path'])
                        frozen = (row['object_id'], row['revision'], row['layout_digest'], row['layout_protocol'])
                        pilot.check(identity not in object_map or object_map[identity] == frozen, 'frozen object metadata changed')
                        object_map[identity] = frozen
                    k = count if lower is None else sum(lower <= e['utc_second'] < lower + 1 for e in cohort)
                    pilot.check(response['selected_sources'] == count and response['time_candidate_sources'] == k,
                                'source accounting mismatch')
                    emitted.append({'variant': variant, 'pair': pair, 'n': count, 'repeat': repeat,
                        'case': case, 'k': k, 'identities': identities(actual, True), 'iotdb_client_ns': elapsed,
                        'iotdb_sql_queries': response['sql_queries'], 'iotdb_catalog_read_bytes': response['catalog_read_bytes']})
                try:
                    submit('prime', None, None, None, -3)
                    for case, expression in correctness.items():
                        submit(case, expression, None, None, -2)
                    for repeat in range(-1, args.repeats):
                        cases = list(timed)
                        if (repeat + pair) % 2:
                            cases.reverse()
                        for case in cases:
                            expression, lower = timed[case]
                            submit(case, expression, '/T', lower, repeat)
                finally:
                    pilot.unmount(run / 'mount', process, log)
                profiles = [json.loads(s) for s in (run / 'profile.jsonl').read_text().splitlines()]
                pilot.check(len(profiles) == len(emitted), 'profile length mismatch')
                for row, profile in zip(emitted, profiles):
                    pilot.check(profile['ok'], 'failed discovery profile')
                    row.update(server_total_ns=profile['server_total_ns'], counters=profile['counters'],
                               iotdb_delta=profile['iotdb_delta'], stages_ns=profile['stages_ns'])
                    c, k = row['counters'], row['k']
                    expected_sql = 2 + (4 if k else 0) + (1 if row['case'] == 'prime' else 0)
                    pilot.check(row['iotdb_delta']['metadata_query_calls'] == expected_sql and row['iotdb_sql_queries'] == expected_sql,
                                'metadata SQL count changed')
                    pilot.check(c.get('dataset_catalog_index_completeness.calls', 0) == (2 if k else 0), 'two full COUNTs missing')
                    pilot.check(c.get('dataset_catalog_filter_sql_calls', 0) == (1 if k else 0), 'filter call changed')
                    pilot.check(c.get('dataset_catalog_manifest_revalidation.keys', 0) == count and c.get('dataset_catalog_manifest_revalidation.rows', 0) == count,
                                'all-source revalidation missing')
                    pilot.check(c.get('dataset_catalog_marker_read.keys', 0) == k and c.get('dataset_catalog_parsed_cache.hits', 0) == (0 if row['case'] == 'prime' else k),
                                'fresh marker/cache contract changed')
                report['observations'].extend(emitted)
                save(args.output / 'report.json', report)
                print(json.dumps({'variant': variant, 'pair': pair, 'n': count, 'verified': len(emitted)}), flush=True)
    typed_manifest = json.loads(args.typed_pilot.read_text())
    requests = {'hdf5': hdf5_request(typed_manifest['sources'][0]), 'grib': grib_request('/geos-pilot-9856a092a812/grib-control.grib2')}
    env = connection_environment(args.source_pid, 'h09probe1790939889dbfc1f')
    env.update(IOTDB_CONFIG_FILE=str(args.pilot_dir / 'profile.properties'),
               IOTDBFS_NAMESPACE_LOCK=str(args.output / 'typed-namespace.lock'))
    # Keep Unix-domain socket names below 108 bytes without moving evidence.
    short = Path(f'/tmp/fjtyped-{os.getpid()}')
    short.symlink_to(args.output.resolve(), target_is_directory=True)
    try:
        for variant in ('before', 'after'):
            (args.output / ('typed-' + variant)).mkdir()
        for pair in range(args.pairs):
            for variant in (('before', 'after') if pair % 2 == 0 else ('after', 'before')):
                rows = run_campaign(short / ('typed-' + variant), pair, getattr(args, variant), env, requests, args.repeats)
                report['typed_controls'].extend(dict(r, variant=variant, pair=pair) for r in rows)
                save(args.output / 'report.json', report)
    finally:
        short.unlink()
    report['summary'] = []
    for count in (1, 16):
        for case in timed:
            item = {'n': count, 'case': case}
            for variant in ('before', 'after'):
                rows = [r for r in report['observations'] if r['n'] == count and r['case'] == case and r['variant'] == variant and r['repeat'] >= 0]
                item[variant] = {'samples': len(rows), 'client_p50_ms': statistics.median(r['iotdb_client_ns'] for r in rows) / 1e6,
                    'server_p50_ms': statistics.median(r['server_total_ns'] for r in rows) / 1e6,
                    'metadata_sql_calls': sorted({r['iotdb_sql_queries'] for r in rows}),
                    'catalog_bytes': sorted({r['iotdb_catalog_read_bytes'] for r in rows}),
                    'pair_client_p50_ms': [statistics.median(r['iotdb_client_ns'] for r in rows if r['pair'] == p) / 1e6 for p in range(args.pairs)]}
            pilot.check(item['before']['catalog_bytes'] == item['after']['catalog_bytes'], 'logical byte budget changed')
            report['summary'].append(item)
    report['typed_summary'] = {}
    for fmt in ('grib', 'hdf5'):
        report['typed_summary'][fmt] = {}
        for variant in ('before', 'after'):
            rows = [r for r in report['typed_controls'] if r['format'] == fmt and r['variant'] == variant and r['repetition'] >= 0]
            report['typed_summary'][fmt][variant] = {'samples': len(rows), 'ipc_sha256': sorted({r['ipc_sha256'] for r in rows}),
                'ipc_bytes': sorted({r['ipc_bytes'] for r in rows}), 'metadata_sql_calls': sorted({r['iotdb_delta']['metadata_query_calls'] for r in rows}),
                'server_p50_ms': statistics.median(r['server_total_ns'] for r in rows) / 1e6,
                'complete_client_p50_ms': statistics.median(r['control_ns'] + r['stat_ns'] + r['read_ns'] for r in rows) / 1e6}
        pilot.check(report['typed_summary'][fmt]['before']['ipc_sha256'] == report['typed_summary'][fmt]['after']['ipc_sha256']
                    and report['typed_summary'][fmt]['before']['ipc_bytes'] == report['typed_summary'][fmt]['after']['ipc_bytes'], 'typed IPC changed')
        pilot.check(all(report['typed_summary'][fmt][v]['metadata_sql_calls'] == [1] for v in ('before', 'after')),
                    'typed metadata SQL must remain one call')
    report['complete'] = True
    save(args.output / 'report.json', report)
    print(json.dumps({'complete': True, 'summary': report['summary'], 'typed_summary': report['typed_summary']}))


if __name__ == '__main__':
    main()
