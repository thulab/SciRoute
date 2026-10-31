"""One event loop and fixed offered load for public query-task pool experiments.

Scientific clocks end after owned values/masks. Profile harvesting, route binding
and full oracle hashes run after the wave; route-less catalogs stay anonymous.
Server intervals measure registered-task overlap, not physical CPU parallelism.
"""
from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
import struct
import time

from . import client, grib_catalog_client, hdf5_batch_client

LIMIT = 1048576


class WaveError(RuntimeError):
    """A failed wave retaining completed clocks, profiles and failure details."""
    def __init__(self, message, result):
        super().__init__(message)
        self.result = result


class _FirstFrames:
    def __init__(self, count):
        self.remaining = count
        self.ready = asyncio.Event()
        self.error = None

    async def sent(self):
        self.remaining -= 1
        if self.remaining == 0:
            self.ready.set()
        await self.ready.wait()
        if self.error is not None:
            raise RuntimeError('initial frame barrier aborted') from self.error

    def abort(self, error):
        self.error = error
        self.ready.set()


async def _control(context, request, first=None):
    start = time.perf_counter_ns()
    writer = None
    try:
        payload = json.dumps(request, separators=(',', ':'), allow_nan=False).encode()
        client.need(0 < len(payload) <= LIMIT, 'control request over limit')
        reader, writer = await asyncio.open_unix_connection(str(context['socket']))
        writer.write(struct.pack('>I', len(payload)) + payload)
        await writer.drain()
        if first is not None:
            await first.sent()
        length, = struct.unpack('>I', await reader.readexactly(4))
        client.need(0 < length <= LIMIT, 'invalid response bound')
        response = json.loads(await reader.readexactly(length))
        client.need(response.get('ok') is True, 'query control failed: ' + str(response.get('error', 'unknown')))
        return response, time.perf_counter_ns() - start
    except BaseException as error:
        if first is not None:
            first.abort(error)
        raise
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()


def _append(row, parts, units):
    for part in parts:
        meta = part['meta']
        client.need(meta.get(b'iotdbfs.sources') == b'1' and
                    meta.get(b'iotdbfs.source.0.path') == row['path'].encode() and
                    meta.get(b'iotdbfs.source.0.version') == str(row['version']).encode(),
                    'value provenance differs')
    client.need(bool(parts), 'empty value result')
    arrays = [p['array'] for p in parts]
    masks = [p['mask'] for p in parts]
    if len(parts) > 1:
        import numpy as np
        array, mask = np.concatenate(arrays), np.concatenate(masks)
    else:
        array, mask = arrays[0], masks[0]
    units.append((row, array, mask))


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
                    response_paths=[], batch_items=len(rows))
        for row, item in zip(rows, items):
            client.need('path' in item, 'batch item result path missing')
            parts, size = client.ipc(context, item, True)
            stat['ipc_bytes'] += size
            stat['response_paths'].append(item['path'])
            _append(row, parts, units)
        stats.append(stat)
    elif fmt == 'grib':
        for row in rows:
            response, elapsed = await asyncio.wait_for(_control(context, client.value_request(fmt, row)), 300)
            parts, size = client.ipc(context, response, True)
            client.need('path' in response, 'typed result path missing')
            stats.append(dict(control_ns=elapsed, ipc_bytes=size, response_path=response['path']))
            _append(row, parts, units)
    t2 = time.perf_counter_ns()
    return dict(query_index=index, t0_ns=t0, t1_ns=t1, t2_ns=t2,
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


def _harvest(context, offset, count, strict):
    path = Path(context['root']) / 'queries.jsonl'
    records = []
    deadline = time.monotonic() + (30 if strict else 0)
    while True:
        if path.exists():
            with path.open('rb') as stream:
                stream.seek(offset)
                raw = stream.read(16*1024*1024+1)
            client.need(len(raw) <= 16*1024*1024, 'profile envelope too large')
            end = raw.rfind(b'\n')
            if end >= 0:
                consumed = raw[:end+1]
                records.extend(json.loads(line) for line in consumed.splitlines() if line.strip())
                offset += len(consumed)
        if not strict or len(records) >= count or time.monotonic() >= deadline:
            break
        time.sleep(.001)
    context['profile_offset'] = offset
    records.sort(key=lambda r: r['sequence'])
    client.need(len({r['sequence'] for r in records}) == len(records), 'duplicate profile sequence')
    if strict:
        client.need(len(records) == count and all(r.get('ok') is True for r in records),
                    'wave profiles incomplete, extra, or failed')
    return records


def _bind_profiles(rows, profiles, fmt, window):
    expected_operation = 'format_catalog' if fmt == 'grib' else 'dataset_catalog'
    catalogs = [p for p in profiles if p.get('operation') == expected_operation]
    client.need(len(catalogs) == len(rows), 'anonymous catalog profile count differs')
    for profile in profiles:
        client.need(window[0] <= profile['steady_start_ns'] <= profile['steady_end_ns'] <= window[1],
                    'profile escaped client load window or incompatible steady clock epoch')
    by_route = {}
    for profile in profiles:
        routes = ([profile['path']] if 'path' in profile else
                  [i['path'] for i in profile.get('items', [])])
        for route in routes:
            client.need(route not in by_route, 'duplicate profile route')
            by_route[route] = profile
    used = set()
    for row in rows:
        for stat in row['value_calls']:
            routes = stat.get('response_paths', [stat['response_path']])
            client.need(bool(routes) and all(p in by_route for p in routes), 'value route profile missing')
            profile = by_route[routes[0]]
            client.need(all(by_route[p] is profile for p in routes), 'batch spans multiple profiles')
            client.need(profile['sequence'] not in used, 'profile bound to multiple calls')
            used.add(profile['sequence'])
            if fmt == 'hdf5':
                client.need(profile.get('operation') == 'typed_batch' and
                            [i['path'] for i in profile.get('items', [])] == routes,
                            'batch profile paths/order differ')
                _validate_native_profile(profile)
            else:
                client.need(profile.get('operation') == 'typed', 'GRIB value operation differs')
            stat['server_profile'] = profile
    client.need(len(used) + len(catalogs) == len(profiles), 'unmatched wave profile')
    if fmt == 'hdf5':
        client.need(all(p.get('server_hdf5_execution', {}).get('mode') == 'native-hdf5'
                        for p in catalogs), 'HDF5 catalog mode differs')
    return catalogs


def _validate_native_profile(profile):
    """Native/cache proof equivalent to the sealed cache/batch validator."""
    counters = profile.get('counters', {})
    mode = profile.get('server_hdf5_execution', {})
    cached = mode.get('native_cache_enabled')
    client.need(mode.get('mode') == 'native-hdf5' and mode.get('payload_backend') == 'filesystem',
                'wrong native HDF5 backend')
    client.need(type(cached) is bool and bool(counters.get('hdf5.native.session.cache.enabled', 0)) == cached,
                'native cache policy evidence differs')
    client.need(counters.get('hdf5.native.slabs', 0) > 0 and
                not profile.get('iotdb_delta', {}).get('payload_calls', 0),
                'no native slab or database payload fallback')
    for key in ('payload_map.block_reads', 'payload_map.data_batches', 'payload_map.hash_bytes',
                'hdf5.selection.decoder_compiles', 'hdf5.layout.pages.calls'):
        client.need(not counters.get(key, 0), 'forbidden native work: '+key)
    local = counters.get('hdf5.native.filesystem.calls', 0)
    source = counters.get('hdf5.native.source.calls', 0)
    client.need(local == source and counters.get('hdf5.native.filesystem.bytes', 0) ==
                counters.get('hdf5.native.source.bytes', 0), 'source not exclusively filesystem')
    if local:
        client.need(counters.get('hdf5.native.filesystem.bytes', 0) > 0, 'filesystem read has no bytes')
    else:
        client.need(cached and counters.get('hdf5.native.session.hits', 0) > 0 and
                    counters.get('hdf5.native.session.rebinds', 0) > 0 and
                    counters.get('hdf5.native.session.validations', 0) >= 2 and
                    counters.get('hdf5.native.logical.calls', 0) > 0 and
                    counters.get('hdf5.native.logical.bytes', 0) > 0 and
                    counters.get('hdf5.native.logical.replays', 0) > 0,
                    'zero IO without fresh validated and budgeted cache hit')


def run_wave(context, group, scope, versions, expected, inflight=8, *, query_count=None):
    """Repeat one sealed query under a fixed (1 or 8) scientific-query limit.

    The caller keeps inflight/count identical across server pool configurations.
    No executor, worker thread, profile read or output hash enters t0..t2.
    """
    client.need(inflight in (1, 8), 'offered inflight must be 1 or 8')
    count = inflight if query_count is None else query_count
    client.need(type(count) is int and count > 0, 'query count must be positive')
    client.need(group['format'] in ('grib', 'hdf5'), 'unsupported fixed-matrix format')
    # Lazy imports in client.ipc must not add first-use dependency setup to T.
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


def _overlap(intervals, lo, hi):
    events = []
    for start, end in intervals:
        start, end = max(start, lo), min(end, hi)
        if start < end:
            events.extend(((start, 1), (end, -1)))
    # End events precede starts at the same endpoint: intervals are [start,end).
    events.sort()
    active = peak = area = occupied = 0
    previous = lo
    for point, change in events:
        area += active*(point-previous)
        if active:
            occupied += point-previous
        active += change
        peak = max(peak, active)
        previous = point
    return dict(max_parallel=peak, integral_active_ns=area, occupied_ns=occupied,
                mean_parallel=area/(hi-lo) if hi > lo else 0,
                busy_fraction=occupied/(hi-lo) if hi > lo else 0)


def analyze_intervals(profiles, window=None, *, start_ns=None, end_ns=None):
    """Intersect server steady intervals with a same-host steady-clock window."""
    if window is not None:
        start_ns, end_ns = ((window['start_ns'], window['end_ns']) if isinstance(window, dict) else window)
    if start_ns is None:
        start_ns = min((p['steady_start_ns'] for p in profiles), default=0)
    if end_ns is None:
        end_ns = max((p['steady_end_ns'] for p in profiles), default=start_ns)
    client.need(end_ns >= start_ns, 'interval window reversed')
    execution, queues, operations, per_profile = [], [], {}, []
    for profile in profiles:
        start, end = profile['steady_start_ns'], profile['steady_end_ns']
        wait = profile.get('counters', {}).get('query_tasks.queue_nanoseconds', 0)
        client.need(type(start) is int and type(end) is int and start <= end and
                    type(wait) is int and wait >= 0, 'invalid server/queue interval')
        queued = start-wait
        execution.append((start, end))
        queues.append((queued, start))
        operations.setdefault(profile['operation'], []).append((start, end))
        per_profile.append(dict(sequence=profile.get('sequence'), operation=profile['operation'],
                                queued_at_ns=queued, start_ns=start, end_ns=end, queue_ns=wait))
    return dict(semantics='server registration overlap; excludes profile sink and socket IO; not physical CPU concurrency',
                window=dict(start_ns=start_ns, end_ns=end_ns),
                execution=_overlap(execution, start_ns, end_ns),
                queue=_overlap(queues, start_ns, end_ns),
                operations={op:_overlap(spans,start_ns,end_ns) for op,spans in sorted(operations.items())},
                profiles=per_profile)
