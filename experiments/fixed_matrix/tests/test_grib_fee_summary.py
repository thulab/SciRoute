"""Offline true-binary same-handle acquisition and cleanup regression checks."""
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from experiments.fixed_matrix import grib_fee_summary as protocol
from experiments.fixed_matrix import hdf5_native_ab
REPO=Path(__file__).resolve().parents[3]
def external(name):return protocol.load_module(REPO,name)

class GribFeeSummaryTests(unittest.TestCase):
    def test_original_supervisor_worker_and_only_confirmed_groups(self):
        self.assertIs(protocol.GribFeeSummaryRunner.supervise,hdf5_native_ab.NativeABRunner.supervise)
        self.assertIs(protocol.GribFeeSummaryRunner.worker,hdf5_native_ab.NativeABRunner.worker)
        r=protocol.GribFeeSummaryRunner.__new__(protocol.GribFeeSummaryRunner)
        self.assertEqual([g['N'] for g in r.groups(REPO,'n8')],[8]);self.assertEqual([g['N'] for g in r.groups(REPO,'n128')],[8,128])
        with self.assertRaises(RuntimeError):r.groups(REPO,'n32')

    def test_grib_limits_from_mandatory_frozen_archive(self):
        helper=external('grib_handle_client');work=REPO/protocol.WORK
        name='include/iotdbfs/query/core/QueryPlanning.h';manifest=json.loads((work/'candidate-source-manifest.json').read_text())
        with tarfile.open(work/'candidate-source.tar.gz','r:gz') as archive:
            member=archive.getmember(name);self.assertTrue(member.isfile());self.assertLess(member.size,1048576);raw=archive.extractfile(member).read()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),manifest[name])
        body=raw.decode().split('struct QueryLimits {',1)[1].split('\n};',1)[0];defaults={}
        for name,expr in re.findall(r'std::\w+\s+(\w+)\s*\{([^}]+)\}',body):
            expr=expr.replace('ULL','');self.assertRegex(expr,r'^\s*\d+(?:\s*\*\s*\d+)*\s*$');defaults[name]=math.prod(int(v.strip()) for v in expr.split('*'))
        self.assertEqual(len(defaults),7);self.assertEqual(helper.GRIB_LIMITS,defaults)
        request=helper.prepare_request('grib',['/a'],{'kind':'latest'})['request']
        self.assertEqual(request['limits'],defaults);self.assertEqual(request['subset'],{'projection':['grib.message.offset']})
        self.assertEqual(set(helper.execute_request('opaque','grib')),{'kind','selector_id','predicate'})

    def test_real_formal_mechanism_and_budget_gate(self):
        fixture=external('test_offline');runner=protocol.GribFeeSummaryRunner.__new__(protocol.GribFeeSummaryRunner)
        for config in 'AB':
            c=fixture.counters(config);row=dict(phase='measurement',oracle_verified=True,mode_verified=True,execution_mode='iotdb-indexed',
                selected_units=2,owned_output_bytes=40960,batch_enabled=False,value_calls=[dict(server_profile=dict(operation='typed')) for _ in range(2)],
                discovery_call=dict(server_profile=dict(operation='format_catalog_by_selector',counters=c)))
            runner.validate_row(row,{'format':'grib'},config)
            self.assertNotIn('hot_query.adapter_fee_collector_untracked',c)
            c['read_context.admissions']=2
            with self.assertRaisesRegex(RuntimeError,'admission'):runner.validate_row(row,{'format':'grib'},config)
            c['read_context.admissions']=3;c.pop('request_metadata.collector_exits')
            with self.assertRaises(RuntimeError):runner.validate_row(row,{'format':'grib'},config)

    def collector_probe(self,joint=False,fail=False):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);work=root/'work';work.mkdir();repo=root/'repo';repo.mkdir()
            journal=root/'journal';journal.mkdir();(journal/'seal').write_text('sealed');data=root/'data';data.mkdir();(data/'index.sqlite').write_text('sealed')
            runtime=root/'runtime/run';runtime.mkdir(parents=True);(runtime/'datanode.pid').write_text('123')
            plans=[];bindings={};independent_audit=external('audit_fee_summary')
            for N in ((8,128) if joint else (8,)):
                gid=f'FM1-grib-N{N}-V1-C{N}-S0-base-latest';execution=root/str(N);execution.mkdir()
                plans.append(dict(group=dict(group_id=gid,N=N,format='grib',K_units=2,owned_output_bytes=40960),item=dict(namespace='n',mounts=[]),
                    execution_root=execution,journal=journal,journal_digest='journal',scientific_head='head',root=data,cfg={},prep=dict(index_sha256='new')))
                bindings[gid]=dict(case_id='case',corpus_id='corpus',oracle_sha256='oracle',scope=['/a'],native_selection={},physical_versions={'kind':'latest'},expected_matches=[],run_ids={})
            args=SimpleNamespace(repo=repo,work=work,binary=root/'new',baseline_binary=root/'old',library=root/'library',inspector=root/'inspector',tier='n128' if joint else 'n8')
            runner=protocol.GribFeeSummaryRunner.__new__(protocol.GribFeeSummaryRunner);runner.spec=SimpleNamespace(protocol=protocol.PROTOCOL)
            active=[None]
            def launcher(path,binary,*a,**kw):active[0]=binary;return dict(path=str(path),settings={})
            runner.launcher=launcher;natives=[];calls=[]
            class Native:
                def __init__(self,*a):self.closed=False;natives.append(self)
                def __enter__(self):return self
                def __exit__(self,*a):self.closed=True
                def explain(self,*a):return {}
            @contextmanager
            def mount(*a,**kw):yield dict(pid=1,startup_ns=1,root=root/'profile',profile_offset=0)
            def lifecycle(ctx,request):
                calls.append(request['kind']);ctx['profile_offset']+=1
                response=dict(kind='prepared_catalog_selector',selector_id='opaque',idle_ttl_ms=300000) if request['kind']=='catalog_selector_prepare' else dict(kind='released_catalog_selector',released=True)
                return response,dict(kind=request['kind'],total_ns=2,control_ns=1,profile_offset_after=ctx['profile_offset'],server_profile=dict(operation=request['kind'],sequence=ctx['profile_offset']))
            def row():return dict(total_ns=3,discovery_ns=1,value_ns=2,oracle_verified=True,mode_verified=True,selected_units=2,owned_output_bytes=40960)
            def query(ctx,*a,selector_id=None,**kw):
                self.assertEqual(selector_id,'opaque');calls.append('handle')
                if fail:raise RuntimeError('injected')
                ctx['profile_offset']+=3;return row()
            helper=SimpleNamespace(prepare_request=lambda *a:dict(kind='catalog_selector_prepare'),lifecycle=lifecycle,query=query)
            registry=SimpleNamespace(get_case=lambda cid:{},register_run=lambda model:dict(run_id='run'))
            old=protocol.read_json
            def read(p):return {} if p.name=='resource-paths.json' else dict(settings={}) if p.name=='profile.launch.json' else old(p)
            def sha(p):return 'old' if p==args.baseline_binary or (str(p).startswith('/proc/') and active[0]==args.baseline_binary) else 'new'
            with patch.object(protocol,'load_module',return_value=helper),patch.object(protocol,'read_json',side_effect=read),\
                patch.object(protocol,'file_sha',side_effect=sha),patch.object(protocol.run.infrastructure,'journal_snapshot',return_value='journal'),\
                patch.object(protocol.run,'head',return_value=('head',{})),patch.object(protocol,'NativeContext',Native),\
                patch.object(protocol.telemetry,'environment',return_value=dict(processes=dict(client=dict(pid=protocol.os.getpid(),start_ticks=1)))),\
                patch.object(protocol.telemetry,'sample',return_value={}),patch.object(protocol.run,'validate_sample'),\
                patch.object(protocol.client,'query_native',side_effect=lambda *a:row()),patch.object(protocol,'build_run',return_value={}),patch.object(runner,'validate_row'):
                report={};paired=SimpleNamespace(sf=SimpleNamespace(private_mount=mount))
                if fail:
                    with self.assertRaisesRegex(RuntimeError,'injected'):runner.collect(args,plans,bindings,registry,dict(sources={},resource_id='res'),report,{},paired)
                else:
                    count=runner.collect(args,plans,bindings,registry,dict(sources={},resource_id='res'),report,{},paired)
                    expected=480 if joint else 240;self.assertEqual(count,expected)
                    rows=[json.loads(v) for v in (work/'queries.jsonl').read_text().splitlines()]
                    audit=independent_audit
                    self.assertEqual([(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows],audit.schedule(bindings))
                    self.assertTrue(all(r['product_sha256']==('old' if r['configuration']=='A' else 'new') for r in rows))
                    self.assertEqual(len(natives),16 if joint else 8);self.assertTrue(all(len(p['item']['mounts'])==8 for p in plans))
                self.assertTrue(all(n.closed for n in natives));self.assertEqual(calls[-1],'catalog_selector_release')
                self.assertEqual(calls.count('catalog_selector_prepare'),1 if fail else 16 if joint else 8)
                self.assertEqual(calls.count('catalog_selector_prepare'),calls.count('catalog_selector_release'))
    def test_qualification_full240_real_binary_grid_and_cleanup(self):self.collector_probe()
    def test_joint_full480_real_binary_balanced_tier_grid_and_cleanup(self):self.collector_probe(joint=True)
    def test_failure_still_releases_handle_and_closes_context(self):self.collector_probe(fail=True)
if __name__=='__main__':unittest.main()
