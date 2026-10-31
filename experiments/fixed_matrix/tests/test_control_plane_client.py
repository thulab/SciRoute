import asyncio
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import time
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa

from experiments.fixed_matrix import control_plane_client as subject


def frame(value):
    payload = json.dumps(value).encode()
    return struct.pack('>I', len(payload)) + payload


def ipc_bytes(meta=None, batches=1, wrong_schema=False):
    sink = pa.BufferOutputStream()
    batch = pa.record_batch([pa.array([1., 2., None, 4.], type=pa.float64() if wrong_schema else pa.float32())], names=['value'])
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        for _ in range(batches):
            writer.write_batch(batch, custom_metadata=meta)
    return sink.getvalue().to_pybytes()


class FetchTest(unittest.IsolatedAsyncioTestCase):
    api = "streams"

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='result-fetch-')
        self.root = Path(self.tmp.name)
        self.context = dict(root=self.root, mount=self.root/'mount', socket=self.root/'control.sock', control_plane_transport=self.api)
        self.route = str(self.context['mount']/'typed.ipc?id=sample&result=exact')
        self.handlers = set()
        self.requests = []
        self.errors = []
        self.handler = None

        async def handle(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)
            try:
                length, = struct.unpack('>I', await reader.readexactly(4))
                request = json.loads(await reader.readexactly(length))
                self.requests.append(request)
                await self.handler(reader, writer, request)
            except (ConnectionResetError, BrokenPipeError):
                pass
            except Exception as error:
                self.errors.append(error)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionResetError, BrokenPipeError):
                    pass
                self.handlers.remove(task)
        self.server = await asyncio.start_unix_server(handle, path=str(self.context['socket']))

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        if self.handlers:
            await asyncio.gather(*self.handlers)
        self.tmp.cleanup()
        self.assertEqual(self.errors, [])

    async def transfer(self, raw, header=None, fragment=1024):
        async def send(reader, writer, request):
            response = header if header is not None else dict(ok=True, kind='result_fetch', version=1, size=len(raw))
            writer.write(frame(response))
            for offset in range(0, len(raw), fragment):
                writer.write(raw[offset:offset+fragment])
                await writer.drain()
                await asyncio.sleep(0)
        self.handler = send
        return await subject._fetch(self.context, dict(path=self.route), True)

    async def test_fragmented_ipc_owned_copies_nulls_metadata_and_clocks(self):
        meta = {b'iotdbfs.source.0.path': b'/sealed/a.grib', b'custom': b'value'}
        payload = ipc_bytes(meta, batches=2)
        parts, size, stat = await self.transfer(payload, fragment=1)
        self.assertEqual(size, len(payload))
        self.assertEqual(len(parts), 2)
        for part in parts:
            self.assertEqual(part['meta'], meta)
            np.testing.assert_array_equal(part['array'], [1., 2., 0., 4.])
            np.testing.assert_array_equal(part['mask'], [1, 1, 0, 1])
            self.assertTrue(part['array'].flags.owndata)
            self.assertTrue(part['mask'].flags.owndata)
        self.assertEqual(self.requests, [dict(kind='result_fetch', version=1, path=self.route)])
        self.assertLessEqual(stat['fetch_start_ns'], stat['fetch_end_ns'])
        self.assertEqual(stat['fetch_ns'], stat['fetch_end_ns']-stat['fetch_start_ns'])
        self.assertEqual(stat['decode_owned_ns'], stat['owned_ready_ns']-stat['decode_start_ns'])

    async def test_error_header_preserves_original_error(self):
        with self.assertRaisesRegex(RuntimeError, 'fresh authorization denied'):
            await self.transfer(b'', dict(ok=False, code='PermissionDenied', error='fresh authorization denied'))

    async def test_invalid_header_kinds_versions_and_bounds_rejected(self):
        bad = [dict(ok=True, kind='typed', version=1, size=10),
               dict(ok=True, kind='result_fetch', version=2, size=10),
               dict(ok=True, kind='result_fetch', version=True, size=10),
               dict(ok=True, kind='result_fetch', version=1, size=True),
               dict(ok=True, kind='result_fetch', version=1, size=0),
               dict(ok=True, kind='result_fetch', version=1, size=-1),
               dict(ok=True, kind='result_fetch', version=1, size=subject.MAX_RESULT_BYTES+1),
               dict(ok=True, kind='result_fetch', version=1, size=1.5), []]
        for header in bad:
            with self.subTest(header=header), self.assertRaises(RuntimeError):
                await self.transfer(b'', header)

    async def test_truncated_header_and_body_fail(self):
        for raw in [b'\x00\x00', struct.pack('>I', 12)+b'{',
                    frame(dict(ok=True, kind='result_fetch', version=1, size=15))+b'partial']:
            async def send(reader, writer, request):
                writer.write(raw)
                await writer.drain()
            self.handler = send
            with self.assertRaises(asyncio.IncompleteReadError):
                await subject._fetch(self.context, dict(path=self.route), True)

    async def test_unbounded_header_rejected_before_read(self):
        for length in [0, subject.LIMIT+1]:
            async def send(reader, writer, request):
                writer.write(struct.pack('>I', length))
                await writer.drain()
            self.handler = send
            with self.assertRaisesRegex(RuntimeError, 'response bound'):
                await subject._fetch(self.context, dict(path=self.route), True)

    async def test_wrong_arrow_schema_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'value schema differs'):
            await self.transfer(ipc_bytes(wrong_schema=True))

    async def test_invalid_arrow_body_is_not_success(self):
        with self.assertRaises(pa.ArrowInvalid):
            await self.transfer(b'not arrow')

    async def test_path_cannot_escape_mount(self):
        for response in [{}, dict(path='/outside/typed.ipc'), dict(path='relative/typed.ipc'),
                         dict(path=str(self.context['mount']/'..'/'outside'))]:
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                await subject._fetch(self.context, response, True)
        self.assertEqual(self.requests, [])

    async def test_cancellation_closes_socket(self):
        received, closed = asyncio.Event(), asyncio.Event()
        async def send(reader, writer, request):
            received.set()
            self.assertEqual(await reader.read(), b'')
            closed.set()
        self.handler = send
        task = asyncio.create_task(subject._fetch(self.context, dict(path=self.route), True))
        await asyncio.wait_for(received.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(closed.wait(), 1)

    async def test_read_body_calls_are_chunk_bounded(self):
        sizes = []
        channel_type = subject._Streams if self.api == 'streams' else subject._RawSocket
        original = channel_type.readinto
        async def counted(channel, target, offset, count):
            sizes.append(count)
            return await original(channel, target, offset, count)
        payload = ipc_bytes()*1000
        # Decoding is separately covered; this case isolates transport bounds.
        with patch.object(channel_type, 'readinto', counted), patch.object(subject, '_decode', return_value=[]):
            parts, size, _ = await self.transfer(payload, fragment=subject.FETCH_CHUNK_BYTES)
        self.assertEqual(parts, [])
        self.assertEqual(size, len(payload))
        self.assertTrue(sizes)
        self.assertLessEqual(max(sizes), subject.FETCH_CHUNK_BYTES)
        self.assertGreater(sum(count == subject.FETCH_CHUNK_BYTES for count in sizes), 1)

    async def test_control_large_request_short_response_and_fresh_one_shot_connections(self):
        async def send(reader, writer, request):
            raw = frame(dict(ok=True, echoed=len(request['padding'])))
            for byte in raw:
                writer.write(bytes([byte]))
                await writer.drain()
                await asyncio.sleep(0)
            self.assertEqual(await reader.read(), b'')
        self.handler = send
        for _ in range(2):
            response, elapsed = await subject._control(
                self.context, dict(kind='probe', padding='p'*900000))
            self.assertEqual(response['echoed'], 900000)
            self.assertGreater(elapsed, 0)
        self.assertEqual(len(self.requests), 2)

    async def test_control_error_and_bounds(self):
        cases = [(frame(dict(ok=False, error='fresh authorization denied')), RuntimeError),
                 (struct.pack('>I', 0), RuntimeError),
                 (struct.pack('>I', subject.LIMIT+1), RuntimeError),
                 (b'\x00\x00', asyncio.IncompleteReadError),
                 (struct.pack('>I', 100)+b'partial', asyncio.IncompleteReadError),
                 (struct.pack('>I', 1)+b'{', json.JSONDecodeError)]
        for raw, error in cases:
            async def send(reader, writer, request):
                writer.write(raw)
                await writer.drain()
            self.handler = send
            with self.subTest(raw=raw), self.assertRaises(error):
                await subject._control(self.context, dict(kind='probe'))
        before = len(self.requests)
        with self.assertRaisesRegex(RuntimeError, 'request over limit'):
            await subject._control(self.context, dict(padding='p'*subject.LIMIT))
        self.assertEqual(len(self.requests), before)

    async def test_control_timeout_closes_socket_without_retry(self):
        received, closed = asyncio.Event(), asyncio.Event()
        async def send(reader, writer, request):
            received.set()
            self.assertEqual(await reader.read(), b'')
            closed.set()
        self.handler = send
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(subject._control(self.context, dict(kind='probe')), .05)
        self.assertTrue(received.is_set())
        await asyncio.wait_for(closed.wait(), 1)
        self.assertEqual(len(self.requests), 1)


class WaveTest(unittest.TestCase):
    api = "streams"

    def fixture(self, fmt):
        paths = ['/sealed/a.'+fmt, '/sealed/b.'+fmt]
        rows = [dict(path=path, version=123, offset=i*12+8, length=12, format_id='grib', revision=1,
                     unit_id='message:'+str(i*12+8)+':12', dataset='/temperature') for i, path in enumerate(paths)]
        values = np.array([1., 2., 0., 4.], dtype='<f4')
        mask = np.array([1, 1, 0, 1], dtype='u1')
        expected = [dict(binding=dict(path=row['path'], version=123),
                         unit='dataset:/temperature' if fmt=='hdf5' else row['unit_id'],
                         native_oracle=dict(elements=4, value_sha256=hashlib.sha256(values.tobytes()).hexdigest(),
                                            mask_sha256=hashlib.sha256(mask.tobytes()).hexdigest())) for row in rows]
        return dict(format=fmt, K_units=2, owned_output_bytes=40), paths, rows, expected

    def execute(self, fmt='grib', inflight=8, count=None, fail_fetch=False):
        with tempfile.TemporaryDirectory(prefix='result-fetch-wave-') as tmp:
            root = Path(tmp)
            context = dict(root=root, mount=root/'mount', socket=root/'control.sock', control_plane_transport=self.api)
            group, scope, rows, expected = self.fixture(fmt)
            count = inflight if count is None else count
            original_run = asyncio.run
            routes, profiles, requests = {}, [], []
            first_count = active_fetch = peak_fetch = 0
            initial = None

            async def serve(coro):
                nonlocal first_count, active_fetch, peak_fetch, initial
                initial = asyncio.Event()
                handlers = set()
                async def handle(reader, writer):
                    nonlocal first_count, active_fetch, peak_fetch
                    task = asyncio.current_task(); handlers.add(task)
                    try:
                        length, = struct.unpack('>I', await reader.readexactly(4))
                        request = json.loads(await reader.readexactly(length)); kind = request['kind']
                        requests.append(request)
                        if kind == 'result_fetch':
                            active_fetch += 1; peak_fetch = max(peak_fetch, active_fetch)
                            try:
                                await asyncio.sleep(.002)
                                row = routes[request['path']]
                                payload = ipc_bytes({b'iotdbfs.sources':b'1', b'iotdbfs.source.0.path':row['path'].encode(),
                                                     b'iotdbfs.source.0.version':b'123'})
                                if fail_fetch and request['path'].endswith('route0'):
                                    writer.write(frame(dict(ok=False, error='injected fetch failure')))
                                else:
                                    writer.write(frame(dict(ok=True, kind='result_fetch', version=1, size=len(payload)))+payload)
                                await writer.drain()
                            finally:
                                active_fetch -= 1
                            return
                        if kind in ('format_catalog', 'dataset_catalog'):
                            first_count += 1
                            if first_count == min(inflight, count): initial.set()
                            await initial.wait()
                        start = time.perf_counter_ns()
                        response = dict(ok=True, kind=kind)
                        profile = dict(operation=kind, ok=True, counters={})
                        if kind == 'format_catalog': response['units'] = rows
                        elif kind == 'dataset_catalog':
                            response['datasets'] = [dict(path=r['path'], version=123, dataset_path=r['dataset']) for r in rows]
                        else:
                            selected = rows if kind == 'typed_batch' else [next(r for r in rows if r['path']==request['request']['sources']['paths'][0]['path'])]
                            items = []
                            for i, row in enumerate(selected):
                                route = str(context['mount']/('route'+str(len(routes))))
                                routes[route] = row; items.append(dict(index=i, path=route))
                            if kind == 'typed_batch': response.update(kind='published_routes', items=items); profile['items']=items
                            else: response['path']=items[0]['path']; profile['path']=response['path']
                        if fmt == 'hdf5':
                            profile['server_hdf5_execution']=dict(mode='native-hdf5', payload_backend='filesystem', native_cache_enabled=True)
                            if kind == 'typed_batch':
                                profile['counters'].update({'hdf5.native.slabs':2,'hdf5.native.session.cache.enabled':1,
                                    'hdf5.native.session.hits':2,'hdf5.native.session.rebinds':2,'hdf5.native.session.validations':4,
                                    'hdf5.native.logical.calls':2,'hdf5.native.logical.bytes':32,'hdf5.native.logical.replays':2})
                        profile.update(sequence=len(profiles)+1, steady_start_ns=start, steady_end_ns=time.perf_counter_ns())
                        profiles.append(profile)
                        with (root/'queries.jsonl').open('a') as output: output.write(json.dumps(profile)+'\n')
                        writer.write(frame(response)); await writer.drain()
                    finally:
                        writer.close(); await writer.wait_closed(); handlers.remove(task)
                server = await asyncio.start_unix_server(handle, path=str(context['socket']))
                try: return await coro
                finally:
                    server.close(); await server.wait_closed()
                    if handlers: await asyncio.gather(*handlers)
            def eventloop(coro): return original_run(serve(coro))
            with patch.object(subject.asyncio, 'run', side_effect=eventloop), \
                 patch.object(subject.client, 'ipc', side_effect=AssertionError('synchronous FUSE IPC used')), \
                 patch('concurrent.futures.ThreadPoolExecutor', side_effect=AssertionError('client worker added')):
                result = subject.run_wave(context, group, scope, dict(kind='latest'), expected, inflight, query_count=count)
            return result, peak_fetch, requests

    def test_grib_eight_coroutines_async_overlap_owned_oracle_and_unchanged_profiles(self):
        result, peak, requests = self.execute()
        self.assertEqual(result['transport'], 'result_fetch_v1')
        self.assertTrue(result['complete'])
        self.assertEqual(result['client_peak_inflight'], 8)
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 8)
        self.assertEqual(len(result['profiles']), 24)
        self.assertEqual(len(result['catalog_profiles']), 8)
        self.assertEqual(sum(r['kind']=='result_fetch' for r in requests), 16)
        for row in result['rows']:
            self.assertTrue(row['oracle_verified'])
            self.assertEqual(row['total_ns'], row['discovery_ns']+row['value_ns'])
            self.assertEqual(len(row['value_calls']), 2)
            previous = row['t1_ns']
            for call in row['value_calls']:
                self.assertLessEqual(previous, call['fetch_start_ns'])
                self.assertLessEqual(call['fetch_end_ns'], call['decode_start_ns'])
                self.assertLessEqual(call['decode_end_ns'], call['append_start_ns'])
                self.assertLessEqual(call['owned_ready_ns'], row['t2_ns'])
                self.assertEqual(call['decode_owned_ns'], call['decode_end_ns']-call['decode_start_ns']+call['append_owned_ns'])
                previous = call['owned_ready_ns']
        self.assertEqual(result, json.loads(json.dumps(result, allow_nan=False)))

    def test_hdf5_batch_item_order_and_same_query_sequential_fetch(self):
        result, peak, _ = self.execute('hdf5')
        self.assertEqual(len(result['profiles']), 16)
        self.assertGreater(peak, 1)
        for row in result['rows']:
            call = row['value_calls'][0]
            self.assertEqual(call['batch_items'], 2)
            self.assertEqual(len(call['fetches']), 2)
            self.assertLessEqual(call['fetches'][0]['owned_ready_ns'], call['fetches'][1]['fetch_start_ns'])
            self.assertEqual(call['fetch_ns'], sum(s['fetch_ns'] for s in call['fetches']))
            self.assertTrue(row['oracle_verified'])

    def test_fixed_eight_limit_for_more_than_one_wave(self):
        result, peak, _ = self.execute(count=17)
        self.assertEqual(len(result['rows']), 17)
        self.assertEqual(result['client_peak_inflight'], 8)
        self.assertLessEqual(peak, 8)

    def test_single_inflight_fetch_has_no_cross_query_parallelism(self):
        result, peak, _ = self.execute(inflight=1, count=3)
        self.assertEqual(result['client_peak_inflight'], 1)
        self.assertEqual(peak, 1)

    def test_failed_fetch_is_query_failure_with_completed_rows_retained(self):
        with self.assertRaises(subject.WaveError) as caught:
            self.execute(fail_fetch=True)
        self.assertIn('injected fetch failure', str(caught.exception))
        result = caught.exception.result
        self.assertFalse(result['complete'])
        self.assertEqual(len(result['rows']), 7)
        self.assertTrue(all(r['oracle_verified'] for r in result['rows']))

    def test_oracle_hashing_stays_after_owned_result_clocks(self):
        original = subject.client.validate
        times = []
        def validate(*args):
            times.append(time.perf_counter_ns())
            return original(*args)
        with patch.object(subject.client, 'validate', side_effect=validate):
            result, _, _ = self.execute()
        self.assertGreater(min(times), result['end_ns'])

    def test_exact_four_queries_no_preconnect_or_extra_requests(self):
        result, peak, requests = self.execute(inflight=4)
        self.assertEqual(result['client_socket_api'], self.api)
        self.assertEqual(result['client_peak_inflight'], 4)
        self.assertEqual(len(result['rows']), 4)
        self.assertEqual(len(requests), 20)
        self.assertEqual(sum(r['kind']=='format_catalog' for r in requests), 4)
        self.assertEqual(sum(r['kind']=='typed' for r in requests), 8)
        self.assertEqual(sum(r['kind']=='result_fetch' for r in requests), 8)
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 4)


class RawFetchTest(FetchTest):
    api = 'raw_socket'


class RawWaveTest(WaveTest):
    api = 'raw_socket'


if __name__ == '__main__':
    unittest.main()
