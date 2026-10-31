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

from experiments.fixed_matrix import query_task_client as subject


class IntervalTest(unittest.TestCase):
    def profile(self, start, end, operation='typed', wait=0, seq=1):
        return dict(sequence=seq, operation=operation, steady_start_ns=start, steady_end_ns=end,
                    counters={'query_tasks.queue_nanoseconds': wait})

    def test_adjacent_endpoints_do_not_invent_parallelism(self):
        out = subject.analyze_intervals([self.profile(0, 5), self.profile(5, 10, seq=2)], (0, 10))
        self.assertEqual(out['execution']['max_parallel'], 1)
        self.assertEqual(out['execution']['mean_parallel'], 1)
        self.assertEqual(out['execution']['busy_fraction'], 1)

    def test_clipping_queue_and_operation_overlap(self):
        out = subject.analyze_intervals([self.profile(10, 30, 'hdf5', 8),
                                        self.profile(20, 40, 'arrow', 2, 2)], (15, 35))
        self.assertEqual(out['execution']['max_parallel'], 2)
        self.assertEqual(out['execution']['integral_active_ns'], 30)
        self.assertEqual(out['execution']['mean_parallel'], 1.5)
        self.assertEqual(out['operations']['arrow']['max_parallel'], 1)
        self.assertEqual(out['queue']['integral_active_ns'], 2)
        self.assertEqual(out['profiles'][0]['queued_at_ns'], 2)
        self.assertIn('not physical CPU', out['semantics'])

    def test_hdf5_waiting_while_arrow_executes_has_causal_evidence(self):
        # HDF2 waited from t=1 to20; Arrow ran during that wait and before HDF1 released.
        profiles = [self.profile(0,20,'hdf5'), self.profile(20,30,'hdf5',19,2),
                    self.profile(4,8,'arrow',0,3)]
        out = subject.analyze_intervals(profiles,(0,30))
        self.assertEqual(out['operations']['hdf5']['max_parallel'],1)
        self.assertEqual(out['execution']['max_parallel'],2)
        arrow = out['profiles'][2]; waiting = out['profiles'][1]
        self.assertLessEqual(waiting['queued_at_ns'],arrow['start_ns'])
        self.assertLess(arrow['end_ns'],waiting['start_ns'])

    def test_empty_and_invalid_intervals(self):
        self.assertEqual(subject.analyze_intervals([])['execution']['max_parallel'],0)
        for p in [self.profile(4,3), self.profile(1,2,wait=-1)]:
            with self.assertRaises(RuntimeError):subject.analyze_intervals([p])


class WaveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='query-task-client-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.context = dict(root=self.root, mount=self.root/'mount', socket=self.root/'control.sock')
        self.values = np.array([1.,2.,3.,4.],dtype='<f4')
        self.mask = np.array([1,1,0,1],dtype='u1')
        self.values[2]=0
        self.queries = 0
        self.routes = {}
        self.frames_before_first_response = None
        self.first_catalogs = 0
        self.sequence = 0
        self.response_number = 0
        self.profile_order = []

    def fixture(self, fmt):
        paths = ['/sealed/a.'+fmt,'/sealed/b.'+fmt]
        rows = [dict(path=path,version=123,offset=i*12+8,length=12,format_id='grib',revision=1,
                     unit_id='message:'+str(i*12+8)+':12',dataset='/temperature')
                for i,path in enumerate(paths)]
        sha = hashlib.sha256(self.values.tobytes()).hexdigest()
        masksha = hashlib.sha256(self.mask.tobytes()).hexdigest()
        expected = [dict(binding=dict(path=row['path'],version=123),
                         unit='dataset:/temperature' if fmt=='hdf5' else row['unit_id'],
                         native_oracle=dict(elements=4,value_sha256=sha,mask_sha256=masksha)) for row in rows]
        return dict(format=fmt,K_units=2,owned_output_bytes=40), paths, rows, expected

    def ipc(self, context, response, values):
        self.assertTrue(values)
        row=self.routes[response['path']]
        meta={b'iotdbfs.sources':b'1',b'iotdbfs.source.0.path':row['path'].encode(),
              b'iotdbfs.source.0.version':b'123'}
        return [dict(meta=meta,array=self.values.copy(),mask=self.mask.copy())],64

    def execute(self, fmt='grib', inflight=8, count=None, fail_value=False):
        group,scope,rows,expected=self.fixture(fmt)
        count=inflight if count is None else count
        original_run=asyncio.run
        async def serve(coro):
            initial=asyncio.Event()
            slot=asyncio.Semaphore(4)
            handlers=set()
            async def handle(reader,writer):
                task=asyncio.current_task();handlers.add(task)
                try:
                    length,=struct.unpack('>I',await reader.readexactly(4))
                    request=json.loads(await reader.readexactly(length))
                    kind=request['kind'];queued=time.perf_counter_ns()
                    if kind in ('format_catalog','dataset_catalog'):
                        self.first_catalogs+=1
                        if self.first_catalogs>=min(inflight,count):initial.set()
                        await initial.wait()
                    async with slot:
                        start=time.perf_counter_ns()
                        await asyncio.sleep(.001)
                        response=dict(ok=True,kind=kind)
                        profile=dict(operation=kind,ok=True,
                                     counters={'query_tasks.queue_nanoseconds':start-queued})
                        if kind=='format_catalog':response['units']=rows
                        elif kind=='dataset_catalog':
                            response['datasets']=[dict(path=r['path'],version=123,dataset_path=r['dataset']) for r in rows]
                        else:
                            if kind=='typed_batch':
                                selected=rows;response['kind']='published_routes'
                            else:
                                path=request['request']['sources']['paths'][0]['path']
                                selected=[next(r for r in rows if r['path']==path)]
                            results=[]
                            for i,row in enumerate(selected):
                                route=str(self.root/'mount'/('route'+str(len(self.routes))))
                                self.routes[route]=row;results.append(dict(index=i,path=route))
                            if kind=='typed_batch':response['items']=results;profile['items']=results
                            else:response['path']=results[0]['path'];profile['path']=response['path']
                            if fail_value and self.response_number==inflight:
                                response=dict(ok=False,error='injected read failure');profile['ok']=False
                        if fmt=='hdf5':
                            profile['server_hdf5_execution']=dict(mode='native-hdf5',payload_backend='filesystem',native_cache_enabled=True)
                            if kind=='typed_batch':
                                profile['counters'].update({'hdf5.native.slabs':2,
                                    'hdf5.native.session.cache.enabled':1,'hdf5.native.session.hits':2,
                                    'hdf5.native.session.rebinds':2,'hdf5.native.session.validations':4,
                                    'hdf5.native.logical.calls':2,'hdf5.native.logical.bytes':32,
                                    'hdf5.native.logical.replays':2})
                        self.sequence+=1;profile.update(sequence=self.sequence,steady_start_ns=start,
                                                       steady_end_ns=time.perf_counter_ns())
                        self.profile_order.append(profile)
                        with (self.root/'queries.jsonl').open('a') as stream:stream.write(json.dumps(profile)+'\n')
                        payload=json.dumps(response).encode()
                        if self.frames_before_first_response is None:self.frames_before_first_response=self.first_catalogs
                        self.response_number+=1
                        writer.write(struct.pack('>I',len(payload))+payload);await writer.drain()
                finally:
                    writer.close();await writer.wait_closed();handlers.remove(task)
            server=await asyncio.start_unix_server(handle,path=str(self.context['socket']))
            try:return await coro
            finally:
                server.close();await server.wait_closed()
                if handlers:await asyncio.gather(*handlers)
        def eventloop(coro):return original_run(serve(coro))
        with patch.object(subject.asyncio,'run',side_effect=eventloop),patch.object(subject.client,'ipc',side_effect=self.ipc):
            return subject.run_wave(self.context,group,scope,dict(kind='latest'),expected,
                                    inflight=inflight,query_count=count)

    def test_grib_eight_first_frames_owned_oracle_and_anonymous_catalogs(self):
        result=self.execute()
        self.assertTrue(result['complete'])
        self.assertEqual(self.frames_before_first_response,8)
        self.assertEqual(result['client_peak_inflight'],8)
        self.assertEqual(len(result['rows']),8)
        self.assertEqual(len(result['profiles']),24)
        self.assertEqual(len(result['catalog_profiles']),8)
        self.assertGreater(result['intervals']['execution']['max_parallel'],1)
        for row in result['rows']:
            self.assertEqual(row['total_ns'],row['discovery_ns']+row['value_ns'])
            self.assertTrue(row['oracle_verified'])
            self.assertNotIn('server_profile',row['discovery_call'])
            self.assertEqual(len(row['value_calls']),2)
            for call in row['value_calls']:
                self.assertEqual(call['response_path'],call['server_profile']['path'])
        self.assertEqual(result,json.loads(json.dumps(result,allow_nan=False)))

    def test_hdf5_one_batch_owned_oracle_and_exact_route_binding(self):
        result=self.execute('hdf5')
        self.assertEqual(len(result['profiles']),16)
        self.assertEqual(result['client_peak_inflight'],8)
        for row in result['rows']:
            self.assertEqual(len(row['value_calls']),1)
            self.assertEqual(row['value_calls'][0]['batch_items'],2)
            self.assertTrue(row['mode_verified'])

    def test_fixed_eight_limit_and_single_query_control(self):
        result=self.execute(count=17)
        self.assertEqual(result['client_peak_inflight'],8)
        self.assertEqual(result['query_count'],17)
        self.assertEqual(len(result['rows']),17)

    def test_single_inflight_control(self):
        result=self.execute(inflight=1,count=3)
        self.assertEqual(result['client_peak_inflight'],1)
        self.assertEqual(self.frames_before_first_response,1)
        self.assertEqual(len(result['rows']),3)

    def test_request_failure_retains_completed_rows_and_original_error(self):
        with self.assertRaises(subject.WaveError) as caught:self.execute(fail_value=True)
        result=caught.exception.result
        self.assertFalse(result['complete'])
        self.assertEqual(len(result['rows']),7)
        self.assertTrue(all(r['oracle_verified'] for r in result['rows']))
        self.assertIn('injected read failure',str(caught.exception))
        self.assertEqual(result,json.loads(json.dumps(result,allow_nan=False)))

    def test_oracle_failure_is_after_all_owned_results_and_not_swallowed(self):
        original=subject.client.validate
        calls=[]
        def wrong(*args):
            calls.append(time.perf_counter_ns())
            original(*args)
            raise RuntimeError('injected oracle mismatch')
        with patch.object(subject.client,'validate',side_effect=wrong):
            with self.assertRaises(subject.WaveError) as caught:self.execute()
        result=caught.exception.result
        self.assertEqual(len(result['rows']),8)
        self.assertGreater(calls[0],result['end_ns'])
        self.assertGreater(calls[0],max(p['steady_end_ns'] for p in result['profiles']))
        self.assertIn('oracle mismatch',str(caught.exception))
        self.assertEqual(result,json.loads(json.dumps(result,allow_nan=False)))


if __name__=='__main__':unittest.main()
