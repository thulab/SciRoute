from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import hdf5_cache_batch as subject
from experiments.fixed_matrix import hdf5_batch_client as client


class AblationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo=Path(__file__).resolve().parents[3]
        cls.groups=subject.groups(cls.repo)

    def fixture(self):
        rows=[];bindings={}
        for index,g in enumerate(self.groups):
            gid=g['group_id'];b=dict(corpus_id='c'+str(index),case_id='case'+str(index),oracle_sha256='oracle',run_ids={})
            bindings[gid]=b
            for campaign in range(3):
                b['run_ids'][str(campaign)]={}
                for config in subject.CONFIGS:
                    b['run_ids'][str(campaign)][config]={a:gid+config+a+str(campaign) for a in subject.ARMS}
                    for arm in subject.ARMS:
                        for repeat in range(12):
                            r=dict(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256='oracle',
                                campaign=campaign,configuration=config,arm=arm,repeat=repeat,phase='warmup' if repeat<2 else 'measurement',
                                run_id=b['run_ids'][str(campaign)][config][arm],ok=True,oracle_verified=True,mode_verified=True,
                                execution_mode='native-hdf5',selected_units=g['K_units'],total_ns=3,discovery_ns=1,value_ns=2,
                                resource_before={},resource_after={})
                            if arm=='IoTDB-FS':
                                c={'hdf5.native.session.cache.enabled':1 if config[1]=='1' else 0,'hdf5.native.slabs':1,'hdf5.native.filesystem.calls':18,'hdf5.native.source.calls':18,
                                    'hdf5.native.filesystem.bytes':19000,'hdf5.native.source.bytes':19000}
                                profile=dict(operation='typed_batch' if config[3]=='1' else 'typed',
                                    server_hdf5_execution=dict(mode='native-hdf5',payload_backend='filesystem',native_cache_enabled=config[1]=='1'),counters=c,iotdb_delta=dict(payload_calls=0))
                                r.update(batch_enabled=config[3]=='1',value_calls=[dict(server_profile=deepcopy(profile),batch_items=g['K_units'])
                                    for _ in range(1 if config[3]=='1' else g['K_units'])])
                            rows.append(r)
        return rows,bindings

    def analyze(self,rows,bindings):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'queries.jsonl';p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            with patch.object(subject.run,'validate_sample'):
                return subject.analyze(p,bindings,self.groups,(0,1,2))

    def test_only_approved_cases_and_counts(self):
        self.assertEqual([g['group_id'] for g in self.groups],list(subject.GROUPS))
        self.assertEqual(subject.expected_rows(SimpleNamespace(repo=self.repo,smoke=False)),864)
        self.assertEqual(subject.expected_rows(SimpleNamespace(repo=self.repo,smoke=True)),96)

    def test_configuration_order_has_no_missing_or_repeated_position(self):
        for i in range(3):
            orders=[subject.configuration_order(c,i) for c in range(3)]
            self.assertTrue(all(set(o)==set(subject.CONFIGS) for o in orders))
            for config in subject.CONFIGS:self.assertEqual(len({o.index(config) for o in orders}),3)

    def test_complete_coverage_and_paired_ratios(self):
        rows,bindings=self.fixture();out=self.analyze(rows,bindings)
        self.assertEqual(out['query_rows'],864);self.assertEqual(len(out['cells']),12)
        self.assertTrue(all(c['R']==c['Rd']==c['Rq']==1 for c in out['cells']))

    def test_partial_and_duplicate_rejected(self):
        rows,bindings=self.fixture()
        with self.assertRaises(RuntimeError):self.analyze(rows[:-1],bindings)
        rows[-1]=rows[0]
        with self.assertRaises(RuntimeError):self.analyze(rows,bindings)

    def test_zero_io_requires_validated_cache_hit(self):
        rows,_=self.fixture();r=next(r for r in rows if r['arm']=='IoTDB-FS' and r['configuration']=='C1B1')
        c=r['value_calls'][0]['server_profile']['counters']
        for key in ('hdf5.native.filesystem.calls','hdf5.native.source.calls','hdf5.native.filesystem.bytes','hdf5.native.source.bytes'):c[key]=0
        with self.assertRaises(RuntimeError):subject.validate_row(r,r['selected_units'],'C1B1')
        c.update({'hdf5.native.session.hits':1,'hdf5.native.session.rebinds':1,'hdf5.native.session.validations':2,'hdf5.native.logical.calls':18,'hdf5.native.logical.bytes':19000,'hdf5.native.logical.replays':1})
        subject.validate_row(r,r['selected_units'],'C1B1')
        r['batch_enabled']=False
        with self.assertRaises(RuntimeError):subject.validate_row(r,r['selected_units'],'C1B0')

    def test_database_fallback_and_indexed_engine_rejected(self):
        rows,_=self.fixture();r=next(r for r in rows if r['arm']=='IoTDB-FS')
        p=r['value_calls'][0]['server_profile'];p['iotdb_delta']['payload_calls']=1
        with self.assertRaises(RuntimeError):subject.validate_row(r,r['selected_units'],'C0B0')
        p['iotdb_delta']['payload_calls']=0;p['counters']['hdf5.selection.decoder_compiles']=1
        with self.assertRaises(RuntimeError):subject.validate_row(r,r['selected_units'],'C0B0')

    def test_slow_campaign_retained(self):
        rows,b=self.fixture()
        for r in rows:
            if r['campaign']==1 and r['arm']=='IoTDB-FS':
                for key in ('total_ns','discovery_ns','value_ns'):r[key]*=100
        out=self.analyze(rows,b)
        for c in out['cells']:
            self.assertAlmostEqual(c['R'],1);self.assertAlmostEqual(c['R_range'][0],1);self.assertAlmostEqual(c['R_range'][1],100)

    def test_batch_request_preserves_exact_version_and_dataset(self):
        row=dict(path='/sealed/file',version=123,dataset='/x')
        request=client.batch_request([row])
        self.assertEqual(request['kind'],'typed_batch')
        item=request['items'][0]
        self.assertEqual(item['kind'],'typed')
        self.assertEqual(item['request']['sources']['versions'],dict(kind='exact',exact=123))
        self.assertEqual(item['request']['subset']['projection'],['hdf5.value:%2Fx'])
        with self.assertRaises(RuntimeError):client.batch_request([])
        with self.assertRaises(RuntimeError):client.batch_request([row]*65)


class BatchClientTest(unittest.TestCase):
    def test_one_control_for_all_units_and_profile_after_clock(self):
        rows=[dict(path='/x',version=1,dataset='/a'),dict(path='/y',version=2,dataset='/b')]
        events=[]
        def clock():events.append('clock');return len(events)*100
        def append(row,parts,units):events.append('owned');units.append((row,None,None))
        def profile(context,count):
            events.append('profiles');self.assertEqual(count,2)
            return [dict(server_hdf5_execution=dict(mode='native-hdf5')),
                dict(server_hdf5_execution=dict(mode='native-hdf5'),items=[dict(index=0,path='/m/a'),dict(index=1,path='/m/b')])]
        with patch.object(client.time,'perf_counter_ns',side_effect=clock), \
             patch.object(client.client,'call',return_value=({},[],dict(response_path=None))), \
             patch.object(client.client,'discovered',return_value=rows), \
             patch.object(client.client,'control',return_value=(dict(kind='published_routes',items=[dict(index=0,path='/m/a'),dict(index=1,path='/m/b')]),123)) as control, \
             patch.object(client.client,'ipc',side_effect=lambda *a:([],20)), \
             patch.object(client,'append_unit',side_effect=append), \
             patch.object(client.client,'profiles',side_effect=profile), \
             patch.object(client.client,'validate',return_value=40):
            result=client.query({},dict(format='hdf5',owned_output_bytes=40),['/x','/y'],dict(kind='latest'),[],batch=True)
        self.assertEqual(control.call_count,1);self.assertEqual(result['selected_units'],2)
        self.assertEqual(result['value_calls'][0]['ipc_bytes'],40)
        self.assertEqual(events,['clock','clock','owned','owned','clock','profiles'])
        self.assertEqual(result['total_ns'],result['discovery_ns']+result['value_ns'])

    def test_batch_reordered_response_rejected(self):
        rows=[dict(path='/x',version=1,dataset='/a')]
        with patch.object(client.client,'call',return_value=({},[],{})), \
             patch.object(client.client,'discovered',return_value=rows), \
             patch.object(client.client,'control',return_value=(dict(kind='published_routes',items=[dict(index=1,path='/m/a')]),1)):
            with self.assertRaises(RuntimeError):
                client.query({},dict(format='hdf5'),['/x'],dict(kind='latest'),[],batch=True)


    def test_batch_profile_path_or_index_mismatch_rejected(self):
        rows=[dict(path='/x',version=1,dataset='/a')]
        for items in ([dict(index=0,path='/wrong')],[dict(index=1,path='/m/a')],[]):
            with self.subTest(items=items), \
                 patch.object(client.client,'call',return_value=({},[],dict(response_path=None))), \
                 patch.object(client.client,'discovered',return_value=rows), \
                 patch.object(client.client,'control',return_value=(dict(kind='published_routes',items=[dict(index=0,path='/m/a')]),1)), \
                 patch.object(client.client,'ipc',return_value=([],20)), \
                 patch.object(client,'append_unit'), \
                 patch.object(client.client,'profiles',return_value=[dict(server_hdf5_execution=dict(mode='native-hdf5')),
                     dict(server_hdf5_execution=dict(mode='native-hdf5'),items=items)]):
                with self.assertRaisesRegex(RuntimeError,'batch profile/result paths differ'):
                    client.query({},dict(format='hdf5'),['/x'],dict(kind='latest'),[],batch=True)


class BuildGateTest(unittest.TestCase):
    def fixture(self,root):
        root=root.resolve()
        source=root/'source';source.mkdir();(source/'x.cpp').write_text('int x;')
        binary=root/'iotdb_fs';binary.write_bytes(b'candidate-unpinned')
        unit=root/'unit_tests';unit.write_bytes(b'unit')
        tests=root/'tests.xml';tests.write_text('<testsuites tests="1" failures="0" errors="0"><testsuite tests="1" failures="0" errors="0"><testcase name="gate"/></testsuite></testsuites>')
        receipt=dict(complete=True,source_root=str(source),source_files={'x.cpp':subject.file_sha(source/'x.cpp')},
            binary=str(binary),binary_sha256=subject.file_sha(binary),unit_binary=str(unit),unit_binary_sha256=subject.file_sha(unit),
            focused_tests=str(tests),focused_tests_sha256=subject.file_sha(tests),all_unit_tests=str(tests),all_unit_tests_sha256=subject.file_sha(tests))
        path=root/'binding.json';path.write_text(json.dumps(receipt))
        return SimpleNamespace(binary=binary,build_binding=path),receipt

    def test_new_candidate_not_pinned_to_old_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            args,_=self.fixture(Path(tmp));result=subject.verify_build_binding(args)
            self.assertTrue(result['complete']);self.assertEqual(result['source_files'],1)

    def test_source_or_test_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            args,receipt=self.fixture(Path(tmp));Path(receipt['source_root'],'x.cpp').write_text('changed')
            with self.assertRaises(RuntimeError):subject.verify_build_binding(args)
        with tempfile.TemporaryDirectory() as tmp:
            args,receipt=self.fixture(Path(tmp));test=Path(receipt['focused_tests'])
            test.write_text('<testsuites tests="1" failures="1"><testsuite tests="1"><testcase><failure/></testcase></testsuite></testsuites>')
            receipt.update(focused_tests_sha256=subject.file_sha(test),all_unit_tests_sha256=subject.file_sha(test))
            args.build_binding.write_text(json.dumps(receipt))
            with self.assertRaises(RuntimeError):subject.verify_build_binding(args)

    def test_unsafe_source_path_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            args,receipt=self.fixture(Path(tmp));receipt['source_files']={'../iotdb_fs':receipt['binary_sha256']}
            args.build_binding.write_text(json.dumps(receipt))
            with self.assertRaises(RuntimeError):subject.verify_build_binding(args)


class PackageTest(unittest.TestCase):
    def fixture(self,root):
        from experiments.fixed_matrix import hdf5_cache_batch_package as pkg
        repo=root/'repo';repo.mkdir();extracted=root/'package';extracted.mkdir()
        sources={}
        for name in (*pkg.DEPLOY,'experiments/old/work/sealed.py'):
            path=extracted/'source'/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('# '+name)
            sources[name]=pkg.sha(path)
            if name not in pkg.DEPLOY:
                target=repo/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(path.read_bytes())
        (extracted/'manifest.json').write_text(json.dumps(dict(protocol=pkg.PROTOCOL,complete=True,deployment_files=list(pkg.DEPLOY),sources=sources)))
        return pkg,repo,extracted

    def test_deploy_only_allowlisted_scripts_and_preserve_work(self):
        with tempfile.TemporaryDirectory() as tmp:
            pkg,repo,extracted=self.fixture(Path(tmp));old=repo/'experiments/old/work/sealed.py';before=old.read_bytes()
            result=pkg.verify(repo,extracted,apply=True)
            self.assertTrue(result['existing_dependencies_unchanged']);self.assertEqual(old.read_bytes(),before)
            self.assertTrue(all((repo/n).is_file() for n in pkg.DEPLOY))

    def test_changed_dependency_rejected_without_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            pkg,repo,extracted=self.fixture(Path(tmp));(repo/'experiments/old/work/sealed.py').write_text('changed')
            with self.assertRaises(RuntimeError):pkg.verify(repo,extracted,apply=True)
            self.assertTrue(all(not (repo/n).exists() for n in pkg.DEPLOY))


if __name__=='__main__':unittest.main()
