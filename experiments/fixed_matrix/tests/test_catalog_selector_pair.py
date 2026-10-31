"""Offline paired schedule, lifecycle, mechanism and complete collector probes."""
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import tempfile
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import catalog_selector_pair as protocol

REPO=Path(__file__).resolve().parents[3]

class CatalogSelectorPairTests(unittest.TestCase):
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
        prepare=helper.prepare_request('hdf5',['/b','/a'],{'kind':'latest'})
        self.assertEqual(prepare['kind'],'catalog_selector_prepare')
        self.assertEqual(set(prepare['request']),{'sources','format_id','max_sources'})
        execute=helper.execute_request('opaque')
        self.assertNotIn('sources',execute)
        self.assertEqual(set(execute),{'kind','selector_id','predicate','max_datasets','max_catalog_bytes'})
        with self.assertRaisesRegex(RuntimeError,'ID'):helper.execute_request('')

    def test_grib_prepare_freezes_original_shape_limits_and_no_predicate(self):
        helper=protocol.load_helper(REPO)
        request=helper.prepare_request('grib',['/a'],{'kind':'latest'})['request']
        self.assertEqual(request['format_id'],'grib')
        self.assertEqual(request['subset'],{'projection':['grib.message.offset']})
        self.assertEqual(request['limits'],helper.GRIB_LIMITS)
        self.assertEqual(request['sources']['formats']['adapter_ids'],['grib'])
        execute=helper.execute_request('opaque','grib')
        self.assertEqual(set(execute),{'kind','selector_id','predicate'})
        self.assertEqual(execute['kind'],'format_catalog_by_selector')

    def test_grib_limits_exactly_match_all_current_queryplanning_defaults(self):
        helper=protocol.load_helper(REPO)
        work=REPO/protocol.WORK
        name='include/iotdbfs/query/core/QueryPlanning.h'
        manifest=json.loads((work/'candidate-source-manifest.json').read_text())
        with tarfile.open(work/'candidate-source.tar.gz','r:gz') as archive:
            member=archive.getmember(name)
            self.assertTrue(member.isfile())
            self.assertLess(member.size,1048576)
            raw=archive.extractfile(member).read()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),manifest[name])
        text=raw.decode('utf-8')
        body=text.split('struct QueryLimits {',1)[1].split('\n};',1)[0]
        defaults={}
        for name,expression in re.findall(r'std::\w+\s+(\w+)\s*\{([^}]+)\}',body):
            expression=expression.replace('ULL','')
            self.assertRegex(expression,r'^\s*\d+(?:\s*\*\s*\d+)*\s*$')
            defaults[name]=math.prod(int(v.strip()) for v in expression.split('*'))
        self.assertEqual(len(defaults),7)
        self.assertEqual(helper.GRIB_LIMITS,defaults)

    def test_format_entrypoints_keep_real_supervisor_and_only_four_sealed_groups(self):
        self.assertIs(protocol.CatalogSelectorPairRunner.supervise,protocol.NativeABRunner.supervise)
        for fmt in ('grib','hdf5'):
            runner=protocol.CatalogSelectorPairRunner.__new__(protocol.CatalogSelectorPairRunner);runner.format=fmt
            for tier in ('n8','n128'):
                self.assertEqual(runner.groups(REPO,tier)[0]['group_id'],
                    f'FM1-{fmt}-N{int(tier[1:])}-V1-C{int(tier[1:])}-S0-base-latest')
            with self.assertRaisesRegex(RuntimeError,'sealed'):runner.groups(REPO,'n32')

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
        runner=protocol.CatalogSelectorPairRunner.__new__(protocol.CatalogSelectorPairRunner);runner.format='hdf5'
        row={'phase':'measurement','value_calls':[],'discovery_call':{'server_profile':{'counters':dict(protocol.COUNTERS,**{'read_context.admissions':2,'hot_query.proof_hits':2,'hot_query.fee_summary_stages':4,'hot_query.fee_replay_entries':0,'hot_query.fee_summary_fallbacks':0,'hot_query.fee_summary_collector_exits':0})}}}
        row['discovery_call']['server_profile']['counters'].update({'request_metadata.'+k:0 for k in ('fee_entries_visited','fee_entries_skipped','summary_stages','memo_hits','overlay_entries_inspected','collector_exits','fallbacks','consumed_bytes')})
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

    def test_grib_modes_keep_same_serial_q_full_clock_and_validation(self):
        helper=protocol.load_helper(REPO);requests=[];events=[]
        units=[dict(path='/a',version=1,format_id='grib',revision=1,unit_id='message:0:10',offset=0,length=10),
               dict(path='/b',version=1,format_id='grib',revision=1,unit_id='message:0:10',offset=0,length=10)]
        def tick():events.append('tick');return (10,20,50)[events.count('tick')-1]
        def control(context,request):requests.append(request);events.append('discover');return {'kind':'format_catalog','units':units},1
        def call(context,request,values):
            self.assertEqual(request['kind'],'typed');self.assertTrue(values);events.append('typed')
            return {},[],{'response_path':'/result'+str(events.count('typed'))}
        def profiles(context,count):
            self.assertEqual(count,3);self.assertEqual(events[-1],'tick');events.append('profiles')
            return [dict(operation=requests[-1]['kind'])]+[dict(path='/result'+str(i)) for i in (1,2)]
        for selector in (None,'opaque'):
            events.clear()
            with patch.object(helper.time,'perf_counter_ns',side_effect=tick),\
                 patch.object(helper.client,'control',side_effect=control),\
                 patch.object(helper.client,'call',side_effect=call),\
                 patch.object(helper,'append_unit'),patch.object(helper.client,'profiles',side_effect=profiles),\
                 patch.object(helper.grib_catalog_client,'verify_locators') as verify,\
                 patch.object(helper.client,'validate',return_value=40960) as validate:
                result=helper.query({},dict(format='grib',owned_output_bytes=40960),['/a','/b'],
                    {'kind':'latest'},['oracle'],batch=False,selector_id=selector)
            self.assertEqual([result[k] for k in ('total_ns','discovery_ns','value_ns')],[40,10,30])
            self.assertEqual(len(result['value_calls']),2);self.assertFalse(result['batch_enabled'])
            self.assertEqual(events,['tick','discover','tick','typed','typed','tick','profiles'])
            verify.assert_called_once();self.assertEqual(validate.call_args.args[1:],(['oracle'],'grib'))
        self.assertEqual([r['kind'] for r in requests],['format_catalog','format_catalog_by_selector'])

    def grib_formal_fixture(self):
        runner=protocol.CatalogSelectorPairRunner.__new__(protocol.CatalogSelectorPairRunner);runner.format='grib'
        counters=dict(protocol.COUNTERS,**{'registration_attempts':1,'hot_query.proof_hits':3,
            'read_context.admissions':3,'version_context.admissions':3,'hot_query.candidate_domain_hits':1,
            'hot_query.adapter_domain_hits':1})
        for prefix in ('candidate','adapter'):
            counters.update({'hot_query.'+prefix+'_fee_'+key:value for key,value in
                dict(logical_entries=100,entries_visited=100,entries_skipped=0,summary_stages=0,collector_untracked=0).items()})
        counters.pop('hot_query.adapter_fee_collector_untracked')
        row=dict(phase='measurement',oracle_verified=True,mode_verified=True,execution_mode='iotdb-indexed',
            selected_units=2,batch_enabled=False,discovery_call={'server_profile':{
                'operation':'format_catalog_by_selector','counters':counters}},
            value_calls=[{'server_profile':{'operation':'typed','counters':{'registration_attempts':1}}} for _ in range(2)])
        for call in [row['discovery_call']]+row['value_calls']:
            call['server_profile']['counters'].update({'request_metadata.'+k:0 for k in ('fee_entries_visited','fee_entries_skipped','summary_stages','memo_hits','overlay_entries_inspected','collector_exits','fallbacks','consumed_bytes')})
        return runner,row,counters

    def test_grib_formal_gate_keeps_three_admissions_and_original_linear_fees(self):
        runner,row,counters=self.grib_formal_fixture()
        runner.validate_row(row,{'format':'grib','K_units':2},'B')
        counters['read_context.admissions']=2
        with self.assertRaisesRegex(RuntimeError,'admissions'):runner.validate_row(row,{'format':'grib','K_units':2},'B')

    def test_only_bound_adapter_collector_field_is_expected_absent(self):
        runner,row,counters=self.grib_formal_fixture()
        self.assertNotIn('hot_query.adapter_fee_collector_untracked',counters)
        runner.validate_row(row,{'format':'grib','K_units':2},'B')
        counters['hot_query.adapter_fee_collector_untracked']=0
        with self.assertRaisesRegex(RuntimeError,'source-bound'):runner.validate_row(row,{'format':'grib','K_units':2},'B')
        counters.pop('hot_query.adapter_fee_collector_untracked')
        counters.pop('hot_query.candidate_fee_collector_untracked')
        with self.assertRaisesRegex(RuntimeError,'source-bound'):runner.validate_row(row,{'format':'grib','K_units':2},'B')
        counters['hot_query.candidate_fee_collector_untracked']=0
        counters['request_metadata.collector_exits']=1
        with self.assertRaisesRegex(RuntimeError,'exited'):runner.validate_row(row,{'format':'grib','K_units':2},'B')

    def collector_probe(self,fail=False,fmt='hdf5'):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);work=root/'work';work.mkdir();repo=root/'repo';repo.mkdir()
            journal=root/'journal';journal.mkdir();(journal/'seal').write_text('unchanged')
            data=root/'data';data.mkdir();(data/'index.sqlite').write_text('sealed')
            runtime=repo.parent/'runtime/run';runtime.mkdir(parents=True);(runtime/'datanode.pid').write_text('123')
            execution=root/'execution';execution.mkdir()
            group={'group_id':'g','format':fmt,'C_paths':8,'K_units':2,'owned_output_bytes':40960}
            item={'namespace':'n','mounts':[]}
            plan=dict(group=group,item=item,execution_root=execution,journal=journal,journal_digest='journal',
                scientific_head='head',root=data,cfg={},native_root=data,prep={'index_sha256':'candidate'})
            binding=dict(case_id='case',corpus_id='corpus',oracle_sha256='oracle',scope=['/a'],
                native_selection={},physical_versions={'kind':'latest'},expected_matches=[],run_ids={})
            args=SimpleNamespace(repo=repo,work=work,binary=root/'binary',library=root/'library',inspector=root/'inspector')
            runner=protocol.CatalogSelectorPairRunner.__new__(protocol.CatalogSelectorPairRunner);runner.format=fmt
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
                cursor[0]+=30 if fmt=='grib' else 20;context['profile_offset']=cursor[0]
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
    def test_grib_complete240_same_candidate_shared_cursor_and_cleanup(self):self.collector_probe(fmt='grib')
    def test_grib_failure_also_releases_handle_and_all_contexts(self):self.collector_probe(True,fmt='grib')

if __name__=='__main__':unittest.main()
