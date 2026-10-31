"""Offline paired schedule, lifecycle, mechanism and complete collector probes."""
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import selector_handle_pair as protocol

REPO=Path(__file__).resolve().parents[3]

class SelectorHandlePairTests(unittest.TestCase):
    def test_exact240_grid_balanced_formal_first_and_adjacent_pairs(self):
        keys=[]
        for campaign in range(4):
            first={arm:{mode:0 for mode in protocol.CONFIGS} for arm in protocol.ARMS}
            for repeat in range(15):
                order=protocol.query_order(campaign,repeat)
                self.assertEqual(len(set(order)),4)
                for arm in protocol.ARMS:
                    positions=[i for i,pair in enumerate(order) if pair[0]==arm]
                    self.assertEqual(positions[1],positions[0]+1)
                    if repeat>=5:first[arm][order[positions[0]][1]]+=1
                keys.extend((campaign,mode,arm,repeat) for arm,mode in order)
            self.assertTrue(all(v==5 for counts in first.values() for v in counts.values()))
        self.assertEqual(len(keys),240);self.assertEqual(len(set(keys)),240)
        self.assertEqual([protocol.query_order(c,0)[0][1] for c in range(4)],['A','B','B','A'])

    def test_prepare_no_predicate_or_dynamic_budget_and_handle_input_constant(self):
        helper=protocol.load_helper(REPO)
        prepare=helper.prepare_request(['/b','/a'],{'kind':'latest'})
        self.assertEqual(prepare['kind'],'catalog_selector_prepare')
        self.assertEqual(set(prepare['request']),{'sources','format_id','max_sources'})
        execute=helper.execute_request('opaque')
        self.assertNotIn('sources',execute)
        self.assertEqual(set(execute),{'kind','selector_id','predicate','max_datasets','max_catalog_bytes'})
        with self.assertRaisesRegex(RuntimeError,'ID'):helper.execute_request('')

    def test_lifecycle_harvest_own_profile_and_cost_before_harvest(self):
        helper=protocol.load_helper(REPO);context={}
        def profiles(ctx,count):ctx['profile_offset']=25;return [{'operation':'catalog_selector_prepare'}]
        with patch.object(helper.client,'control',return_value=({'ok':True},7)),\
             patch.object(helper.client,'profiles',side_effect=profiles),\
             patch.object(helper.time,'perf_counter_ns',side_effect=[10,30]):
            response,row=helper.lifecycle(context,{'kind':'catalog_selector_prepare'})
        self.assertEqual(row['total_ns'],20);self.assertEqual(row['control_ns'],7)
        self.assertEqual(row['profile_offset_after'],25)

    def test_formal_mechanism_gate_rejects_compile_or_missed_ref(self):
        runner=protocol.SelectorHandlePairRunner.__new__(protocol.SelectorHandlePairRunner)
        row={'phase':'measurement','discovery_call':{'server_profile':{'counters':dict(protocol.COUNTERS,**{'read_context.admissions':2})}}}
        with patch.object(protocol.NativeABRunner,'validate_row'):
            runner.validate_row(row,{},'B')
            for key in protocol.COUNTERS:
                copy=json.loads(json.dumps(row));copy['discovery_call']['server_profile']['counters'][key]+=1
                with self.assertRaisesRegex(RuntimeError,'mechanism'):runner.validate_row(copy,{},'B')
            row['discovery_call']['server_profile']['counters']['read_context.admissions']=1
            with self.assertRaisesRegex(RuntimeError,'admissions'):runner.validate_row(row,{},'B')

    def test_both_modes_same_full_clock_scope_and_owned_validation(self):
        helper=protocol.load_helper(REPO);requests=[];events=[]
        row={'path':'/a','version':1,'dataset':'/d'}
        def tick():events.append('tick');return (10,20,40)[events.count('tick')-1]
        def call(context,request):
            requests.append(request);events.append('discover');return {'datasets':[dict(row,dataset_path='/d')]},[],{}
        def control(context,request):
            events.append('batch');return {'kind':'published_routes','items':[{'index':0,'path':'/result'}]},1
        def profiles(context,count):
            self.assertEqual(events[-1],'tick')
            events.append('profiles')
            return [{'server_hdf5_execution':{'mode':'native-hdf5'}},
                {'server_hdf5_execution':{'mode':'native-hdf5'},'items':[{'index':0,'path':'/result'}]}]
        for selector in (None,'opaque'):
            events.clear()
            with patch.object(helper.time,'perf_counter_ns',side_effect=tick),\
                 patch.object(helper.client,'call',side_effect=call),\
                 patch.object(helper.client,'control',side_effect=control),\
                 patch.object(helper.client,'ipc',return_value=([],0)),\
                 patch.object(helper,'append_unit',side_effect=lambda row,parts,units:units.append(row)),\
                 patch.object(helper.client,'profiles',side_effect=profiles),\
                 patch.object(helper.client,'validate',return_value=40960) as validate:
                result=helper.query({}, {'format':'hdf5','owned_output_bytes':40960},['/a'],
                    {'kind':'latest'},['oracle'],batch=True,selector_id=selector)
            self.assertEqual([result[k] for k in ('total_ns','discovery_ns','value_ns')],[30,10,20])
            self.assertEqual(validate.call_args.args[1:],(['oracle'],'hdf5'))
            self.assertEqual(events,['tick','discover','tick','batch','tick','profiles'])
        self.assertEqual([r['kind'] for r in requests],['dataset_catalog','dataset_catalog_by_selector'])
        with patch.object(helper.client,'call',return_value=({'datasets':[dict(row,path='/escaped',dataset_path='/d')]},[],{})):
            with self.assertRaisesRegex(RuntimeError,'escaped'):
                helper.query({}, {'format':'hdf5'},['/a'],{},[],batch=True,selector_id='opaque')

    def collector_probe(self,fail=False):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);work=root/'work';work.mkdir();repo=root/'repo';repo.mkdir()
            journal=root/'journal';journal.mkdir();(journal/'seal').write_text('unchanged')
            data=root/'data';data.mkdir();(data/'index.sqlite').write_text('sealed')
            runtime=repo.parent/'runtime/run';runtime.mkdir(parents=True);(runtime/'datanode.pid').write_text('123')
            execution=root/'execution';execution.mkdir()
            group={'group_id':'g','K_units':2,'owned_output_bytes':40960}
            item={'namespace':'n','mounts':[]}
            plan=dict(group=group,item=item,execution_root=execution,journal=journal,journal_digest='journal',
                scientific_head='head',root=data,cfg={},native_root=data,prep={'index_sha256':'candidate'})
            binding=dict(case_id='case',corpus_id='corpus',oracle_sha256='oracle',scope=['/a'],
                native_selection={},physical_versions={'kind':'latest'},expected_matches=[],run_ids={})
            args=SimpleNamespace(repo=repo,work=work,binary=root/'binary',library=root/'library',inspector=root/'inspector')
            runner=protocol.SelectorHandlePairRunner.__new__(protocol.SelectorHandlePairRunner)
            runner.spec=SimpleNamespace(protocol=protocol.PROTOCOL)
            runner.launcher=lambda path,*a,**kw:dict(path=str(path),settings={})
            natives=[];calls=[];cursor=[0];sequence=[0]
            class Native:
                def __init__(self,*a):self.closed=False;natives.append(self)
                def __enter__(self):return self
                def __exit__(self,*a):self.closed=True
                def explain(self,*a):return {'plan':'same'}
            @contextmanager
            def mount(*a,**kw):
                yield dict(pid=1,startup_ns=1,root=root/'profile',mount=root/'mount',socket=root/'socket',profile_offset=0)
            def lifecycle(context,request):
                calls.append(request['kind']);sequence[0]+=1;cursor[0]+=10;context['profile_offset']=cursor[0]
                response=({'kind':'prepared_catalog_selector','selector_id':'opaque','idle_ttl_ms':300000}
                    if request['kind']=='catalog_selector_prepare' else {'kind':'released_catalog_selector','released':True})
                return response,dict(kind=request['kind'],total_ns=2,control_ns=1,profile_offset_after=cursor[0],
                    server_profile={'sequence':sequence[0],'operation':request['kind'],'ok':True})
            def row():return dict(total_ns=3,discovery_ns=1,value_ns=2,oracle_verified=True,
                mode_verified=True,selected_units=2,owned_output_bytes=40960)
            def ioquery(context,*a,selector_id=None,**kw):
                calls.append('handle' if selector_id else 'explicit')
                if fail:raise RuntimeError('injected query failure')
                cursor[0]+=20;context['profile_offset']=cursor[0]
                return row()
            helper=SimpleNamespace(prepare_request=lambda *a:{'kind':'catalog_selector_prepare'},
                lifecycle=lifecycle,query=ioquery)
            registry=SimpleNamespace(get_case=lambda cid:{},register_run=lambda model:{'run_id':'run'+str(len(calls))})
            old_read=protocol.read_json
            def read(path):
                if path.name=='resource-paths.json':return {}
                if path.name=='profile.launch.json':return {'settings':{}}
                return old_read(path)
            with patch.object(protocol,'load_helper',return_value=helper),\
                 patch.object(protocol,'read_json',side_effect=read),\
                 patch.object(protocol,'file_sha',return_value='candidate'),\
                 patch.object(protocol.run.infrastructure,'journal_snapshot',return_value='journal'),\
                 patch.object(protocol.run,'head',return_value=('head',{})),\
                 patch.object(protocol,'NativeContext',Native),\
                 patch.object(protocol.telemetry,'environment',return_value={'processes':{'client':{'pid':protocol.os.getpid(),'start_ticks':1}}}),\
                 patch.object(protocol.telemetry,'sample',side_effect=lambda paths:{'monotonic_ns':protocol.time.monotonic_ns()}),\
                 patch.object(protocol.run,'validate_sample'),\
                 patch.object(protocol.client,'query_native',side_effect=lambda *a:row()),\
                 patch.object(protocol,'build_run',return_value={}),\
                 patch.object(runner,'validate_row'):
                report={};paired=SimpleNamespace(sf=SimpleNamespace(private_mount=mount))
                if fail:
                    with self.assertRaisesRegex(RuntimeError,'injected'):runner.collect(args,[plan],{'g':binding},registry,
                        {'sources':{},'resource_id':'resource'},report,{},paired)
                else:
                    count=runner.collect(args,[plan],{'g':binding},registry,
                        {'sources':{},'resource_id':'resource'},report,{},paired)
                    self.assertEqual(count,240)
                    rows=[json.loads(s) for s in (work/'queries.jsonl').read_text().splitlines()]
                    self.assertEqual([(r['campaign'],r['arm'],r['configuration'],r['repeat']) for r in rows],
                        [(c,a,m,r) for c in range(4) for r in range(15) for a,m in protocol.query_order(c,r)])
                    self.assertTrue(all(r['product_sha256']=='candidate' for r in rows))
                    self.assertEqual(len(item['mounts']),4);self.assertEqual(len(natives),8)
                self.assertTrue(all(n.closed for n in natives))
                self.assertEqual(calls[-1],'catalog_selector_release')
                self.assertEqual(calls.count('catalog_selector_prepare'),1 if fail else 4)
                self.assertEqual(calls.count('catalog_selector_release'),1 if fail else 4)

    def test_complete_fake_collector240_same_candidate_and_cleanup(self):self.collector_probe()
    def test_query_failure_releases_handle_and_native_contexts(self):self.collector_probe(True)

if __name__=='__main__':unittest.main()
