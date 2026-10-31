"""Offline proof of paired schedule, request shapes and management clocks."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from experiments.fixed_matrix import query_template_pair as p

REPO=Path(__file__).resolve().parents[3]


class TemplatePairTests(unittest.TestCase):
    def test_joint960_qualification480_and_group_position_balance(self):
        runner=p.QueryTemplatePairRunner.__new__(p.QueryTemplatePairRunner)
        self.assertEqual(len(runner.groups(REPO,'n8')),2);self.assertEqual(len(runner.groups(REPO,'n128')),4)
        for tier,count in [('n8',480),('n128',960)]:
            grid=[(key,c,m,a,r) for c in range(4) for key in p.group_order(tier,c)
                for r in range(15) for a,m in p.query_order(c,r)]
            self.assertEqual(len(grid),count);self.assertEqual(len(set(grid)),count)
        for key in p.NAMES:self.assertEqual(sorted(p.group_order('n128',c).index(key) for c in range(4)),[0,1,2,3])
        self.assertIs(type(runner).supervise,p.NativeABRunner.supervise)

    def test_modes_adjacent_and_five_formal_first_each_arm(self):
        for c in range(4):
            first={a:{m:0 for m in p.CONFIGS} for a in p.ARMS}
            for r in range(15):
                order=p.query_order(c,r)
                for a in p.ARMS:
                    positions=[i for i,x in enumerate(order) if x[0]==a]
                    self.assertEqual(positions[1],positions[0]+1)
                    if r>=5:first[a][order[positions[0]][1]]+=1
            self.assertTrue(all(v==5 for by in first.values() for v in by.values()))

    def test_grib_two_templates_come_from_actual_d_results_and_rebuild_versions(self):
        h=p.helper(REPO,'query_template_client');manager=h.Templates('grib');seen=[]
        rows=[dict(path='/actual-a',version=7,offset=0),dict(path='/actual-b',version=9,offset=1)]
        def control(ctx,request):
            seen.append(request)
            if request['kind']=='query_template_prepare':return dict(kind='prepared_query_template',template_id=f'{len(seen):048x}',idle_ttl_ms=300000),1
            return dict(kind='released_query_template',released=True),1
        with patch.object(h.client,'control',side_effect=control):
            events=manager.ensure({},rows);self.assertEqual(len(events),2)
            self.assertEqual(manager.ensure({},copy.deepcopy(rows)),[])
            changed=copy.deepcopy(rows);changed[0]['version']=8
            self.assertEqual(len(manager.ensure({},changed)),4)
        self.assertEqual([v['kind'] for v in seen],['query_template_prepare']*2+['query_template_release']*2+['query_template_prepare']*2)
        for request,row in zip(seen[:2],rows):
            self.assertEqual(request['declaration'],h.client.value_request('grib',row))
            self.assertEqual(request['declaration']['kind'],'typed')

    def test_hdf5_uses_one_complete_original_batch(self):
        h=p.helper(REPO,'query_template_client');manager=h.Templates('hdf5')
        rows=[dict(path='/a',version=2,dataset='/d'),dict(path='/b',version=3,dataset='/e')]
        with patch.object(h,'management',return_value=(dict(kind='prepared_query_template',template_id='a'*48,idle_ttl_ms=3),{'kind':'query_template_prepare','total_ns':1})) as manage:
            self.assertEqual(len(manager.ensure({},rows)),1)
        self.assertEqual(manage.call_args.args[1],dict(kind='query_template_prepare',declaration=h.hdf5_batch_client.batch_request(rows)))
        self.assertEqual(manager.ids,['a'*48])

    def test_grib_first_query_exact_management_gap_and_post_owned_profile_harvest(self):
        h=p.helper(REPO,'query_template_client');manager=h.Templates('grib');events=[];requests=[]
        rows=[dict(path='/a',version=1,offset=0),dict(path='/b',version=1,offset=0)]
        ticks=iter([10,20,30,40,50,60,100])
        def tick():v=next(ticks);events.append(('tick',v));return v
        def control(ctx,request):
            requests.append(request);kind=request['kind'];events.append(kind)
            if kind=='format_catalog_by_selector':return {},1
            return dict(kind='prepared_query_template',template_id=f'{len(requests):048x}',idle_ttl_ms=300000),1
        def call(ctx,request,values):
            requests.append(request);events.append('owned');return {},[],dict(response_path='/r'+str(len(requests)))
        def profiles(ctx,count):
            self.assertEqual(events[-1],('tick',100));self.assertEqual(count,5);events.append('profiles');ctx['profile_offset']=50
            return [dict(operation=requests[i]['kind'],path='/r'+str(i+1)) for i in range(5)]
        with patch.object(h.time,'perf_counter_ns',side_effect=tick),patch.object(h.client,'control',side_effect=control),\
             patch.object(h.client,'call',side_effect=call),patch.object(h.grib_catalog_client,'catalog_rows',return_value=rows),\
             patch.object(h.hdf5_batch_client,'append_unit'),patch.object(h.client,'profiles',side_effect=profiles),\
             patch.object(h.grib_catalog_client,'verify_locators'),patch.object(h.client,'validate',return_value=40960):
            row=h.query({},dict(format='grib',owned_output_bytes=40960),['/a','/b'],{},['oracle'],'selector','B',manager)
        self.assertEqual([row[k] for k in ('discovery_ns','value_ns','total_ns','management_control_ns','outer_query_wall_ns')],[10,60,70,20,90])
        self.assertEqual([v['kind'] for v in requests],['format_catalog_by_selector','query_template_prepare','query_template_prepare','query_template_execute','query_template_execute'])
        self.assertEqual([set(v) for v in requests[-2:]],[{'kind','template_id'}]*2)

    def test_ordinary_hdf_batch_has_original_declaration_and_identical_endpoint_checks(self):
        h=p.helper(REPO,'query_template_client');rows=[dict(path='/a',version=1,dataset='/d'),dict(path='/b',version=1,dataset='/d')]
        requests=[]
        def control(ctx,request):
            requests.append(request)
            return ({'datasets':[dict(r,dataset_path=r['dataset']) for r in rows]} if len(requests)==1 else
                    {'kind':'published_routes','items':[dict(index=i,path='/r'+str(i)) for i in range(2)]}),1
        profiles=[dict(operation='dataset_catalog_by_selector',server_hdf5_execution={'mode':'native-hdf5'}),
            dict(operation='typed_batch',items=[dict(index=i,path='/r'+str(i)) for i in range(2)],server_hdf5_execution={'mode':'native-hdf5'})]
        def harvest(ctx,count):ctx['profile_offset']=22;return profiles
        with patch.object(h.client,'control',side_effect=control),patch.object(h.client,'ipc',return_value=([],1)),\
             patch.object(h.hdf5_batch_client,'append_unit'),patch.object(h.client,'profiles',side_effect=harvest),\
             patch.object(h.client,'validate',return_value=40960),patch.object(h.time,'perf_counter_ns',side_effect=[10,20,50]):
            row=h.query({},dict(format='hdf5',owned_output_bytes=40960),['/a','/b'],{},[],'s','A',h.Templates('hdf5'))
        self.assertEqual(requests[1],h.hdf5_batch_client.batch_request(rows));self.assertEqual(row['management_control_ns'],0)
        self.assertEqual((row['total_ns'],row['discovery_ns'],row['value_ns']),(40,10,30))

    def test_scope_rejects_before_template_prepare(self):
        h=p.helper(REPO,'query_template_client');manager=h.Templates('grib')
        with patch.object(h.client,'control',return_value=({},1)),patch.object(h.grib_catalog_client,'catalog_rows',return_value=[dict(path='/escape')]),\
             patch.object(manager,'ensure') as ensure:
            with self.assertRaisesRegex(RuntimeError,'escaped'):h.query({},dict(format='grib'),['/a'],{},[],'s','B',manager)
        ensure.assert_not_called()

    def test_management_response_checked_before_profiles_and_strict_operation(self):
        h=p.helper(REPO,'query_template_client');context={}
        def profiles(ctx,count):ctx['profile_offset']=22;return [dict(operation='query_template_release')]
        with patch.object(h.client,'control',return_value=({'kind':'released_query_template','released':True},3)),\
             patch.object(h.client,'profiles',side_effect=profiles),patch.object(h.time,'perf_counter_ns',side_effect=[10,20]):
            _,record=h.standalone(context,dict(kind='query_template_release',template_id='a'*48))
        self.assertEqual(record['total_ns'],10);self.assertEqual(record['profile_offset_before'],0);self.assertEqual(record['profile_offset_after'],22)

    def analyze_fixture(self,path,tier='n128'):
        runner=p.QueryTemplatePairRunner.__new__(p.QueryTemplatePairRunner)
        selected=runner.groups(REPO,tier);gm={g['format']+str(g['N']):g for g in selected}
        bindings={g['group_id']:dict(corpus_id='c'+g['group_id'],case_id='case'+g['group_id'],oracle_sha256='o',
            run_ids={str(c):{m:{a:g['group_id']+str(c)+m+a for a in p.ARMS} for m in p.CONFIGS} for c in range(4)}) for g in selected}
        rows=[];lifecycle=[]
        def profile(op,sequence,fmt,mode=None,number=None):
            c={'request_metadata.consumed_bytes':100}
            if op in ('typed','typed_batch','query_template_execute'):
                c['registration_attempts']=3 if fmt=='hdf5' else 1
                if fmt=='hdf5':c['typed_batch_items']=2
                if mode=='B':c.update({'query_template.executes':1,'query_template.declaration_reuses':2 if fmt=='hdf5' else 1,
                    'query_template.batch_executes':1 if fmt=='hdf5' else 0,'query_template.single_executes':0 if fmt=='hdf5' else 1,
                    'selector.compiles':0,'selector.input_path_items':0})
            return dict(operation=op,sequence=sequence,counters=c)
        for campaign in range(4):
            for position,key in enumerate(p.group_order(tier,campaign)):
                g=gm[key];gid=g['group_id'];fmt=g['format'];b=bindings[gid];number=1
                who=dict(pid=100+len(lifecycle),start_ticks=10+len(lifecycle));prepared=False
                lc=dict(group_id=gid,campaign=campaign,kind='catalog_selector_prepare',selector_id='d',total_ns=2,
                    server_profile=profile('catalog_selector_prepare',number,fmt));lifecycle.append(lc)
                for repeat in range(15):
                    for arm,mode in p.query_order(campaign,repeat):
                        row=dict(group_id=gid,campaign=campaign,configuration=mode,arm=arm,repeat=repeat,
                            phase='warmup' if repeat<5 else 'measurement',product_sha256='new',resource_id='res',
                            **{k:b[k] for k in ('corpus_id','case_id','oracle_sha256')},run_id=b['run_ids'][str(campaign)][mode][arm],
                            oracle_verified=True,mode_verified=True,ok=True,selected_units=2,owned_output_bytes=40960,
                            total_ns=10,discovery_ns=3,value_ns=7,client_identity=who,
                            client_call_begin=dict(monotonic_ns=2),client_call_end=dict(monotonic_ns=99),
                            resource_before=dict(monotonic_ns=1,sampler=dict(pid=who['pid']),processes={str(who['pid']):dict(start_ticks=who['start_ticks'])}),
                            resource_after=dict(monotonic_ns=100,sampler=dict(pid=who['pid']),processes={str(who['pid']):dict(start_ticks=who['start_ticks'])}))
                        if arm=='IoTDB-FS':
                            number+=1;row['discovery_call']=dict(server_profile=profile('dataset_catalog_by_selector' if fmt=='hdf5' else 'format_catalog_by_selector',number,fmt))
                            row.update(lifecycle_events=[],batch_enabled=fmt=='hdf5',execution_mode='native-hdf5' if fmt=='hdf5' else 'iotdb-indexed')
                            if mode=='B' and not prepared:
                                for i in range(2 if fmt=='grib' else 1):
                                    number+=1;event=dict(group_id=gid,campaign=campaign,kind='query_template_prepare',template_id=str(i),total_ns=2,
                                        server_profile=profile('query_template_prepare',number,fmt))
                                    row['lifecycle_events'].append(event);lifecycle.append(event)
                                prepared=True
                            row['management_control_ns']=sum(e['total_ns'] for e in row['lifecycle_events']);row['outer_query_wall_ns']=10+row['management_control_ns']
                            row['value_calls']=[]
                            for i in range(1 if fmt=='hdf5' else 2):
                                number+=1;row['value_calls'].append(dict(server_profile=profile(
                                    ('typed_batch' if fmt=='hdf5' else 'typed') if mode=='A' else 'query_template_execute',number,fmt,mode)))
                        rows.append(row)
                for i in range(2 if fmt=='grib' else 1):
                    number+=1;lifecycle.append(dict(group_id=gid,campaign=campaign,kind='query_template_release',template_id=str(i),total_ns=2,
                        server_profile=profile('query_template_release',number,fmt)))
                number+=1;lifecycle.append(dict(group_id=gid,campaign=campaign,kind='catalog_selector_release',selector_id='d',total_ns=2,
                    server_profile=profile('catalog_selector_release',number,fmt)))
        def write():
            (path/'queries.jsonl').write_text(''.join(json.dumps(v)+'\n' for v in rows))
            (path/'selector-lifecycle.jsonl').write_text(''.join(json.dumps(v)+'\n' for v in lifecycle))
        write();return runner,rows,lifecycle,bindings,selected,write

    def test_producer_complete_joint_and_qualification_grid_and_profiles(self):
        for tier,n in [('n8',480),('n128',960)]:
            with self.subTest(tier=tier),tempfile.TemporaryDirectory() as temp:
                path=Path(temp);runner,rows,lc,b,g,write=self.analyze_fixture(path,tier)
                with patch.object(p.run,'validate_sample'):
                    result=runner.analyze(path/'queries.jsonl',b,g,'res','new')
                self.assertEqual(result['query_rows'],n);self.assertEqual(result['independent_query_clients'],len(g)*4)
                self.assertEqual(len(result['lifecycle']),len(g)*4)
                self.assertTrue(result['all_rows_retained'])

    def test_reject_missing_slow_row_shared_client_charge_and_profile_gap(self):
        for mutation in ('missing','client','charge','sequence'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as temp:
                path=Path(temp);runner,rows,lc,b,g,write=self.analyze_fixture(path,'n8')
                if mutation=='missing':rows.pop()
                if mutation=='client':
                    row=rows[-1];row['client_identity']=rows[0]['client_identity']
                if mutation=='charge':
                    row=next(r for r in rows if r['arm']=='IoTDB-FS' and r['configuration']=='B' and r['repeat']==5)
                    row['value_calls'][0]['server_profile']['counters']['request_metadata.consumed_bytes']+=1
                if mutation=='sequence':lc[0]['server_profile']['sequence']+=1
                write()
                with patch.object(p.run,'validate_sample'):
                    with self.assertRaises(RuntimeError):runner.analyze(path/'queries.jsonl',b,g,'res','new')


if __name__=='__main__':unittest.main()
