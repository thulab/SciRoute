#!/usr/bin/env python3
"""Compression, residual filtering and concurrent A–E Arrow validation.

Read-only queries after prepare_arrow_query_variants.py ingestion. One cold burst
and ten warm bursts per cell; weak-scaling clients=1/2/4. Hashes are outside all
query and wave timers. E shared runtime deltas are never summed across clients.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import threading
import time
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
from public_iotdbfs import control
from run_cache_parallel_ablation import Mount
from run_arrow_query_paths import declaration, digest, owned, save
from summarize_query_profile import read_server_log


def read_batches(source):
    with pa.memory_map(str(source), 'r') as stream:
        return list(ipc.open_stream(stream))


def leaf(batch, child):
    return batch.column('target')[0].values[child].values


def make_cases(batches, channels):
    # Freeze a selective finite threshold, not >0 on a mostly-positive dataset.
    active = [(j, c) for j, batch in enumerate(batches)
              for c in range(1, min(channels, 7))
              if len(leaf(batch, c)) >= 4160 and
              np.unique(leaf(batch, c).slice(0, 4096).to_numpy()).size > 1]
    if not active:
        raise ValueError('no variable filter channel')
    batch_index, child = active[0]
    independent = sorted({j for j, c in active})[:4]
    if len(independent) != 4:
        raise ValueError('need four nonconstant batches for disjoint concurrency')
    values, valid = owned(leaf(batches[batch_index], child).slice(0, 4096))
    finite = values[valid.astype(bool) & np.isfinite(values)]
    threshold = np.float32(np.median(finite))
    if not np.any(finite > threshold):
        threshold = np.min(finite)
    common = dict(batch=batch_index, row=0, field='target')
    def case(name, cols, begin, length, filter=None, client_batches=None):
        result = dict(common, id=name, children=list(reversed(cols)), begin=begin, length=length)
        if filter is not None: result['filter'] = filter
        if client_batches is not None: result['client_batches'] = client_batches
        return result
    selected = dict(child=child, op='gt', threshold=float(threshold))
    empty = dict(child=child, op='gt', threshold=float(np.max(finite)))
    return [case('c1-small', [0], 37, 128),
            case('c7-boundary', list(range(7)), 4032, 128),
            case('c7-tile', list(range(7)), 0, 4096),
            case('all-tile', list(range(channels)), 0, 4096),
            case('c7-filter', list(range(7)), 0, 4096, selected),
            case('c1-hidden-filter', [0], 0, 4096, selected),
            case('c7-reject-all', list(range(7)), 0, 4096, empty),
            case('c7-disjoint-batches', list(range(7)), 37, 128,
                 client_batches=independent)]


def arrays_for(batches, case, client):
    batch = batches[case.get('client_batches', [case['batch']]*4)[client]]
    begin, length = case['begin'], case['length']
    columns = [owned(leaf(batch, c).slice(begin, length)) for c in case['children']]
    index = np.arange(begin, begin+length, dtype='<u8')
    if 'filter' in case:
        f = case['filter']
        values, valid = owned(leaf(batch, f['child']).slice(begin, length))
        keep = valid.astype(bool) & np.isfinite(values) & (values > np.float32(f['threshold']))
        columns = [(v[keep].copy(), m[keep].copy()) for v, m in columns]
        index = index[keep].copy()
    return columns, index


def expected_results(batches, cases):
    result = {}
    for case in cases:
        result[case['id']] = []
        for client in range(4):
            columns, index = arrays_for(batches, case, client)
            result[case['id']].append(dict(digest(columns, index), rows=len(index),
                                                  selectivity=len(index)/case['length']))
    return result


def filtered_declaration(logical, version, case, client):
    selected = dict(case, batch=case.get('client_batches', [case['batch']]*4)[client])
    request = declaration(logical, version, selected)
    if 'filter' in case:
        f = case['filter']
        subset = request['request']['subset']
        if f['child'] not in case['children']:
            subset['physical_array']['values'].append({'child_indices': [f['child']]})
        subset['predicate'] = {'kind': 'compare', 'field': f"value.{f['child']}",
            'operator': f['op'], 'value': {'type': 'float32', 'value': f['threshold']}}
    return request


def execute_e(socket, request, case, client, barrier):
    timing = {}
    barrier.wait()
    start = time.perf_counter_ns()
    response = control(socket, request, timing)
    registered = time.perf_counter_ns()
    with pa.memory_map(response['path'], 'r') as mapped:
        size = mapped.size()
        table = ipc.open_stream(mapped).read_all()
        decoded = time.perf_counter_ns()
        expected_names = ['index'] + [f'value.{c}' for c in case['children']]
        if table.column_names != expected_names:
            raise ValueError('unexpected projected columns or hidden filter leak')
        columns = [owned(table.column(name)) for name in expected_names[1:]]
        index = np.array(table.column('index').to_numpy(), dtype='<u8', copy=True)
        consumed = time.perf_counter_ns()
        del table
    end = time.perf_counter_ns()
    row = dict(type='query', case=case['id'], client_id=client, wall_ns=end-start,
               registration_ns=registered-start, ipc_read_ns=decoded-registered,
               consume_ns=consumed-decoded, close_ns=end-consumed, end_ns=end,
               rows=len(index), encoded_bytes=size, path=response['path'], **timing)
    return row, columns, index


def verify(row, expected):
    for key in ('digest', 'index_digest', 'rows'):
        if row[key] != expected[key]:
            raise ValueError(f"{row.get('case')}/{row.get('client_id')}: {key} mismatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('manifest', 'mount', 'socket', 'binary', 'fallback', 'probe', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--daemon-pid', type=int, required=True)
    parser.add_argument('--repeats', type=int, default=10)
    parser.add_argument('--clients', type=int, nargs='+', default=[1, 2, 4])
    parser.add_argument('--dataset', action='append')
    parser.add_argument('--codec', action='append')
    parser.add_argument('--case', action='append')
    args = parser.parse_args()
    if args.repeats < 1 or any(c not in (1, 2, 4) for c in args.clients):
        parser.error('positive repeats and clients 1/2/4 required')
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    variants = json.loads(args.manifest.read_text())
    if isinstance(variants, dict): variants = variants['variants']
    settings = {'query.cache.max_entries': 16384, 'query.cache.max_inflight': 256,
                'query.cache.worker_threads': 4, 'query.cache.memory_budget_bytes': 1073741824}
    config = args.output/'benchmark-private.properties'
    config.write_text('\n'.join(line for line in Path(mount.original_config).read_text().splitlines()
        if line.split('=', 1)[0].strip() not in settings)+'\n'+
        '\n'.join(f'{k}={v}' for k,v in settings.items())+'\n')
    config.chmod(0o600)
    saved, waves = [], []
    try:
        for variant in variants:
            dataset, codec = variant['dataset'], variant['codec']
            if args.dataset and dataset not in args.dataset: continue
            if args.codec and codec not in args.codec: continue
            batches = read_batches(variant['local'])
            channels = len(batches[0].column('target')[0].values)
            cases = [c for c in make_cases(batches, channels) if not args.case or c['id'] in args.case]
            expected = expected_results(batches, cases)
            del batches
            save(args.output/f'{dataset}-{codec}-oracle.json', expected)
            casefile = args.output/f'{dataset}-{codec}-cases.json'
            save(casefile, cases)
            for clients in args.clients:
                tagbase = f'{dataset}-{codec}-n{clients}'
                mount.start(config, None, tagbase+'-direct')
                arms = [('A','mmap'), ('A-read','read'), ('B','read'), ('C','read'), ('D','read')]
                if clients == 2: arms.reverse()
                for arm, io in arms:
                    tag = tagbase+'-'+arm
                    output = args.output/f'{tag}.jsonl'
                    command = [str(args.probe), '--path', arm[0], '--native-io', io,
                        '--config', str(config), '--source', str(variant['mounted'] if arm=='D' else variant['local']),
                        '--logical', variant['logical'], '--version', str(variant['version']),
                        '--cases', str(casefile), '--output', str(output), '--repeats', str(args.repeats),
                        '--clients', str(clients)]
                    print('START',tag,flush=True)
                    with (args.output/f'{tag}.log').open('w') as log:
                        subprocess.run(command, check=True, env=mount.env, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
                    for line in output.read_text().splitlines():
                        row = json.loads(line)
                        row.update(arm=arm, dataset=dataset, codec=codec, clients=clients)
                        if row['type'] == 'query':
                            verify(row, expected[row['case']][row['client_id']])
                            saved.append(row)
                        elif row['type'] == 'wave': waves.append(row)
                        elif row['type'] == 'error': raise ValueError(row)
                    print('PASS',tag,flush=True)
                for case in cases:
                    tag = tagbase+'-E-'+case['id']
                    profile = args.output/f'{tag}-profile.jsonl'
                    mount.start(config, profile, tag)
                    rows = []
                    with ThreadPoolExecutor(max_workers=clients) as pool:
                        for repeat in range(args.repeats+1):
                            clock = {}
                            barrier = threading.Barrier(clients+1, action=lambda: clock.update(start=time.perf_counter_ns()))
                            futures = [pool.submit(execute_e, args.socket,
                                filtered_declaration(variant['logical'],variant['version'],case,client),
                                case, client, barrier) for client in range(clients)]
                            barrier.wait()
                            results = [future.result(timeout=300) for future in futures]
                            end = max(item[0]['end_ns'] for item in results)
                            common = dict(arm='E',dataset=dataset,codec=codec,clients=clients,
                                          case=case['id'],repeat=repeat,temperature='cold' if repeat==0 else 'warm')
                            waves.append(dict(common,type='wave',wave_wall_ns=end-clock['start'],
                                              runtime_attribution='unavailable-overlap' if clients>1 else 'per-request'))
                            for row,columns,index in results:
                                row.update(common, **digest(columns,index))
                                verify(row,expected[case['id']][row['client_id']])
                                rows.append(row)
                    mount.stop()
                    profiles = read_server_log(profile)
                    by_path = {p['path']:p for p in profiles}
                    if len(profiles)!=len(rows) or len(by_path)!=len(rows):
                        raise ValueError('missing/duplicate server profiles')
                    for row in rows:
                        server = by_path[row['path']]
                        if not server['stages_ns'].get('typed_prepare') or not server['stages_ns'].get('typed_encode'):
                            raise ValueError('result cache bypassed query')
                        row['server'] = server
                    saved.extend(rows)
                    save(args.output/f'{tag}.json',rows)
                    print('PASS',tag,flush=True)
                save(args.output/'measurements.json',saved)
                save(args.output/'waves.json',waves)
    finally:
        try: mount.start(mount.original_config,None,'restored')
        except Exception: mount.start(mount.original_config,None,'fallback',args.fallback)
        save(args.output/'restored.json',dict(pid=mount.pid,profile_enabled=False))
        print('RESTORED daemon pid='+str(mount.pid),flush=True)


if __name__ == '__main__': main()
