"""Opt-in HDF5 cache/batch client; preserve the sealed FM1 client unchanged.

The clock includes control, all result reads and client-owned arrays. Profile
harvesting and independent oracle checks remain outside t2 in every configuration.
"""
from __future__ import annotations
import time
from . import client


def batch_request(rows):
    client.need(0 < len(rows) <= 64, 'typed batch bound differs')
    return dict(kind='typed_batch', items=[client.value_request('hdf5', row) for row in rows])


def append_unit(row, parts, units):
    import numpy as np
    for part in parts:
        meta = part['meta']
        client.need(meta.get(b'iotdbfs.sources') == b'1' and
            meta.get(b'iotdbfs.source.0.path') == row['path'].encode() and
            meta.get(b'iotdbfs.source.0.version') == str(row['version']).encode(),
            'value provenance differs')
    arrays = [p['array'] for p in parts]; masks = [p['mask'] for p in parts]
    client.need(arrays, 'empty value result')
    units.append((row, arrays[0] if len(arrays) == 1 else np.concatenate(arrays),
                  masks[0] if len(masks) == 1 else np.concatenate(masks)))


def query(context, group, scope, version_selector, expected, *, batch):
    client.need(group['format'] == 'hdf5', 'HDF5 ablation only')
    stats = []; units = []
    t0 = time.perf_counter_ns()
    response, parts, stat = client.call(context, client.discover_request('hdf5', scope, version_selector))
    stats.append(stat); rows = client.discovered('hdf5', response, parts)
    client.need(all(r['path'] in scope for r in rows), 'discovery escaped fixed candidate scope')
    t1 = time.perf_counter_ns()
    if batch and rows:
        response, elapsed = client.control(context, batch_request(rows))
        results = response.get('items', [])
        client.need(response.get('kind') == 'published_routes' and len(results) == len(rows)
                    and [r.get('index') for r in results] == list(range(len(rows))), 'batch result order differs')
        stat = dict(control_ns=elapsed, ipc_bytes=0, response_path=None, response_paths=[], batch_items=len(rows))
        for row, result in zip(rows, results):
            parts, size = client.ipc(context, result, True)
            client.need('path' in result, 'batch item result path missing')
            stat['ipc_bytes'] += size; stat['response_paths'].append(result['path'])
            append_unit(row, parts, units)
        stats.append(stat)
    else:
        for row in rows:
            _, parts, stat = client.call(context, client.value_request('hdf5', row), True)
            stats.append(stat); append_unit(row, parts, units)
    t2 = time.perf_counter_ns()
    prof = client.profiles(context, len(stats))
    for stat, profile in zip(stats, prof):
        stat['server_profile'] = profile
        if stat.get('response_path'):
            client.need(profile.get('path') == stat['response_path'], 'profile/result path differs')
        if 'response_paths' in stat:
            items=profile.get('items',[])
            client.need(isinstance(items,list) and [item.get('index') for item in items]==list(range(len(stat['response_paths'])))
                and [item.get('path') for item in items]==stat['response_paths'], 'batch profile/result paths differ')
        client.need(profile.get('server_hdf5_execution', {}).get('mode') == 'native-hdf5', 'actual mode differs')
    output = client.validate(units, expected, 'hdf5')
    client.need(output == group['owned_output_bytes'], 'owned output size differs')
    return dict(total_ns=t2-t0, discovery_ns=t1-t0, value_ns=t2-t1,
        oracle_verified=True, mode_verified=True, execution_mode='native-hdf5',
        owned_output_bytes=output, selected_units=len(rows), discovery_call=stats[0],
        value_calls=stats[1:], batch_enabled=batch)
