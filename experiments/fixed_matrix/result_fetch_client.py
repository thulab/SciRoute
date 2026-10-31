"""Bounded result-fetch transport for the existing single-loop query load.

Registration, ordering, owned-output clocks, profiles and independent oracles
match query_task_client. Only delivery of each already published IPC result is
asynchronous. Arrow decoding and owned copies still run on this event loop.
"""
from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
import struct
import time

from . import client, grib_catalog_client, hdf5_batch_client, query_task_client

LIMIT = query_task_client.LIMIT
MAX_RESULT_BYTES = 64 * 1024 * 1024
FETCH_CHUNK_BYTES = 64 * 1024
WaveError = query_task_client.WaveError
_FirstFrames = query_task_client._FirstFrames
_control = query_task_client._control
_append = query_task_client._append
_harvest = query_task_client._harvest
_bind_profiles = query_task_client._bind_profiles
analyze_intervals = query_task_client.analyze_intervals


def _decode(payload, values):
    import numpy as np
    import pyarrow as pa
    parts = []
    with pa.BufferReader(payload) as stream:
        reader = pa.ipc.open_stream(stream)
        try:
            for item in reader.iter_batches_with_custom_metadata():
                batch, meta = item.batch, dict(item.custom_metadata or {})
                if values:
                    client.need(batch.num_columns == 1 and batch.column(0).type == pa.float32(),
                                'value schema differs')
                    array = np.array(batch.column(0).to_numpy(zero_copy_only=False), dtype='<f4', copy=True)
                    mask = np.array(batch.column(0).is_valid().to_numpy(zero_copy_only=False), dtype='u1', copy=True)
                    array[mask == 0] = 0
                    parts.append(dict(meta=meta, array=array, mask=mask))
                else:
                    parts.append(dict(meta=meta, columns=[c.to_pylist() for c in batch.columns]))
                del batch, item
        finally:
            reader.close()
            del reader
    return parts


async def _fetch(context, response, values=False):
    """Fresh authenticated open of one exact published result, with bounded body.

    At most one fetch per query coroutine is active. No thread, executor, task
    fan-out, speculative next query, profile harvesting or oracle hash is used.
    """
    client.need('path' in response, 'typed result path missing')
    path = Path(response['path'])
    client.need(path.is_absolute() and path.is_relative_to(context['mount']) and
                '..' not in path.parts, 'IPC escaped owned mount')
    request = dict(kind='result_fetch', version=1, path=str(path))
    payload = json.dumps(request, separators=(',', ':'), allow_nan=False).encode()
    client.need(0 < len(payload) <= LIMIT, 'control request over limit')
    started, writer = time.perf_counter_ns(), None
    try:
        reader, writer = await asyncio.open_unix_connection(str(context['socket']), limit=FETCH_CHUNK_BYTES)
        writer.write(struct.pack('>I', len(payload)) + payload)
        await writer.drain()
        length, = struct.unpack('>I', await reader.readexactly(4))
        client.need(0 < length <= LIMIT, 'invalid response bound')
        header = json.loads(await reader.readexactly(length))
        client.need(isinstance(header, dict), 'invalid result-fetch response')
        client.need(header.get('ok') is True,
                    'result fetch failed: ' + str(header.get('error', 'unknown')))
        client.need(header.get('kind') == 'result_fetch' and type(header.get('version')) is int and
                    header['version'] == 1, 'result-fetch protocol differs')
        size = header.get('size')
        client.need(type(size) is int and 0 < size <= MAX_RESULT_BYTES, 'invalid result-fetch body bound')
        body = bytearray(size)
        for offset in range(0, size, FETCH_CHUNK_BYTES):
            count = min(FETCH_CHUNK_BYTES, size - offset)
            body[offset:offset+count] = await reader.readexactly(count)
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
    fetched = time.perf_counter_ns()
    parts = _decode(body, values)
    owned = time.perf_counter_ns()
    return parts, size, dict(fetch_start_ns=started, fetch_end_ns=fetched,
                            fetch_ns=fetched-started, decode_start_ns=fetched,
                            decode_end_ns=owned, decode_owned_ns=owned-fetched, owned_ready_ns=owned)


async def _query(context, group, scope, versions, index, barrier):
    fmt = group['format']
    t0 = time.perf_counter_ns()
    stats, units = [], []
    request = (grib_catalog_client.catalog_request(scope, versions) if fmt == 'grib'
               else client.discover_request(fmt, scope, versions))
    response, elapsed = await asyncio.wait_for(_control(context, request, barrier), 300)
    rows = (grib_catalog_client.catalog_rows(response) if fmt == 'grib'
            else client.discovered(fmt, response, []))
    client.need(all(r['path'] in scope for r in rows), 'discovery escaped fixed candidate scope')
    stats.append(dict(control_ns=elapsed, ipc_bytes=0, response_path=None))
    t1 = time.perf_counter_ns()
    if fmt == 'hdf5' and rows:
        response, elapsed = await asyncio.wait_for(_control(context, hdf5_batch_client.batch_request(rows)), 300)
        items = response.get('items', [])
        client.need(response.get('kind') == 'published_routes' and len(items) == len(rows)
                    and [r.get('index') for r in items] == list(range(len(rows))), 'batch result order differs')
        stat = dict(control_ns=elapsed, ipc_bytes=0, response_path=None,
                    response_paths=[], batch_items=len(rows), fetches=[], fetch_ns=0, decode_owned_ns=0)
        for row, item in zip(rows, items):
            client.need('path' in item, 'batch item result path missing')
            parts, size, fetch = await asyncio.wait_for(_fetch(context, item, True), 300)
            stat['ipc_bytes'] += size
            stat['response_paths'].append(item['path'])
            append_start = time.perf_counter_ns()
            _append(row, parts, units)
            fetch['owned_ready_ns'] = time.perf_counter_ns()
            fetch['append_start_ns'] = append_start
            fetch['append_owned_ns'] = fetch['owned_ready_ns'] - append_start
            fetch['decode_owned_ns'] += fetch['append_owned_ns']
            stat['fetches'].append(fetch)
            stat['fetch_ns'] += fetch['fetch_ns']
            stat['decode_owned_ns'] += fetch['decode_owned_ns']
            stat['owned_ready_ns'] = fetch['owned_ready_ns']
        stats.append(stat)
    elif fmt == 'grib':
        for row in rows:
            response, elapsed = await asyncio.wait_for(_control(context, client.value_request(fmt, row)), 300)
            parts, size, fetch = await asyncio.wait_for(_fetch(context, response, True), 300)
            client.need('path' in response, 'typed result path missing')
            append_start = time.perf_counter_ns()
            _append(row, parts, units)
            fetch['owned_ready_ns'] = time.perf_counter_ns()
            fetch['append_start_ns'] = append_start
            fetch['append_owned_ns'] = fetch['owned_ready_ns'] - append_start
            fetch['decode_owned_ns'] += fetch['append_owned_ns']
            stats.append(dict(control_ns=elapsed, ipc_bytes=size, response_path=response['path'], **fetch))
    t2 = time.perf_counter_ns()
    return dict(query_index=index, transport='result_fetch_v1', t0_ns=t0, t1_ns=t1, t2_ns=t2,
                total_ns=t2-t0, discovery_ns=t1-t0, value_ns=t2-t1,
                selected_units=len(rows), discovery_call=stats[0], value_calls=stats[1:],
                discovery_protocol='format_catalog' if fmt == 'grib' else 'dataset_catalog',
                batch_enabled=fmt == 'hdf5', _units=units, _locators=rows)


async def _drive(context, group, scope, versions, inflight, count):
    semaphore = asyncio.Semaphore(inflight)
    barrier = _FirstFrames(min(inflight, count))
    active = peak = 0

    async def one(index):
        nonlocal active, peak
        async with semaphore:
            active += 1
            peak = max(peak, active)
            try:
                return await _query(context, group, scope, versions, index,
                                    barrier if index < inflight else None)
            finally:
                active -= 1
    results = await asyncio.gather(*(one(i) for i in range(count)), return_exceptions=True)
    return results, peak


def run_wave(context, group, scope, versions, expected, inflight=8, *, query_count=None):
    """Repeat one sealed query under a fixed (1 or 8) scientific-query limit.

    The caller keeps inflight/count identical across server pool configurations.
    No executor, worker thread, profile read or output hash enters t0..t2.
    """
    client.need(inflight in (1, 8), 'offered inflight must be 1 or 8')
    count = inflight if query_count is None else query_count
    client.need(type(count) is int and count > 0, 'query count must be positive')
    client.need(group['format'] in ('grib', 'hdf5'), 'unsupported fixed-matrix format')
    # Lazy imports in _decode must not add first-use dependency setup to T.
    for module in ('numpy', 'pyarrow'):
        importlib.import_module(module)
    offset = context.get('profile_offset', 0)
    outer_start = time.perf_counter_ns()
    results, peak = asyncio.run(_drive(context, group, scope, versions, inflight, count))
    drive_end = time.perf_counter_ns()
    rows = [r for r in results if isinstance(r, dict)]
    errors = [dict(query_index=i, error_type=type(r).__name__, message=str(r))
              for i, r in enumerate(results) if isinstance(r, BaseException)]
    lo = min((r['t0_ns'] for r in rows), default=outer_start)
    hi = max((r['t2_ns'] for r in rows), default=drive_end)
    result = dict(rows=rows, profiles=[], catalog_profiles=[], inflight=inflight,
                  transport='result_fetch_v1', max_result_bytes=MAX_RESULT_BYTES,
                  query_count=count, client_peak_inflight=peak, start_ns=lo, end_ns=hi,
                  makespan_ns=hi-lo, load_window=dict(start_ns=lo, end_ns=hi),
                  throughput_qps=len(rows)*1e9/(hi-lo) if hi > lo else 0,
                  profile_harvest_ns=0, profile_validation_ns=0, oracle_validation_ns=0,
                  errors=errors, complete=False)
    try:
        harvest_start = time.perf_counter_ns()
        profiles = _harvest(context, offset, sum(1+len(r['value_calls']) for r in rows), not errors)
        result['profile_harvest_ns'] = time.perf_counter_ns()-harvest_start
        result['profiles'] = profiles
        if not errors:
            audit_start = time.perf_counter_ns()
            result['catalog_profiles'] = _bind_profiles(rows, profiles, group['format'], (lo, hi))
            result['intervals'] = analyze_intervals(profiles, (lo, hi))
            result['profile_validation_ns'] = time.perf_counter_ns()-audit_start
        oracle_start = time.perf_counter_ns()
        for row in rows:
            if group['format'] == 'grib':
                grib_catalog_client.verify_locators(row['_locators'], expected)
            output = client.validate(row['_units'], expected, group['format'])
            client.need(output == group['owned_output_bytes'], 'owned output size differs')
            client.need(row['selected_units'] == group['K_units'], 'selected unit count differs')
            row.update(oracle_verified=True, mode_verified=not errors,
                       owned_output_bytes=output,
                       execution_mode='native-hdf5' if group['format']=='hdf5' else 'iotdb-indexed')
        result['oracle_validation_ns'] = time.perf_counter_ns()-oracle_start
        result['complete'] = not errors
    except Exception as error:
        errors.append(dict(error_type=type(error).__name__, message=str(error), phase='post-wave-validation'))
    finally:
        for row in rows:
            row.pop('_units', None)
            row.pop('_locators', None)
    if errors:
        raise WaveError('query wave failed: ' + errors[0]['message'], result)
    return result

