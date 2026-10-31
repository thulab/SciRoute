"""Offline protocol acceptance: no service, mounts, network or cgroup mutation."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from experiments.fixed_matrix import query_task_scheduler as m
from experiments.fixed_matrix.hdf5_native_ab import ProtocolSpec
from experiments.fixed_matrix.tests.test_shared_candidates import resource
from experiments.identity.model import digest
from experiments.identity.registry import file_sha


REPO = Path(__file__).resolve().parents[3]
CANDIDATE_BYTES = b'one immutable accepted scheduler executable'
CANDIDATE = hashlib.sha256(CANDIDATE_BYTES).hexdigest()
REGRESSION = ('SocketFixture', 'ExternalNativeHdf5MutexLeavesSingleWorkerAvailableForArrow')


def runner():
    return m.QueryTaskRunner(ProtocolSpec(m.PROTOCOL,
        'experiments.fixed_matrix.query_task_scheduler', 'offline', CANDIDATE,
        frozenset({'src/query/core/QueryTaskScheduler.cpp'}), (REGRESSION,), ()))


def science(groups, qualification=False):
    """Known queued waves: fixed service time, worker slots change only overlap."""
    bindings, rows, waves = {}, [], []
    for group in groups:
        gid = group['group_id']
        bound = dict(corpus_id='corpus-'+gid, case_id='case-'+gid,
                     oracle_sha256='oracle-'+gid, run_ids={})
        bindings[gid] = bound
        for campaign in ((0,) if qualification else (0, 1, 2)):
            for config, workers in (('S1', 1), ('S2', 2), ('S4', 4)):
                bound['run_ids'].setdefault(str(campaign), {})[config] = {
                    load: gid+str(campaign)+config+load for load in ('single', 'burst8', 'FS-index')}
                for load in ('single', 'burst8', 'FS-index'):
                    for repeat in range(1 if qualification else 12):
                        count = 8 if load == 'burst8' else 1
                        service_ns = (1_000_000 if load == 'FS-index' else 10_000_000)*(1, 2, 4)[campaign]
                        start = 1_000_000_000
                        common = dict(group_id=gid, corpus_id=bound['corpus_id'], case_id=bound['case_id'],
                            oracle_sha256=bound['oracle_sha256'], campaign=campaign, configuration=config,
                            scheduler_workers=workers, reader_pool_size=4, load=load,
                            arm='FS-index' if load == 'FS-index' else 'IoTDB-FS', repeat=repeat,
                            run_id=bound['run_ids'][str(campaign)][config][load],
                            phase='qualification' if qualification else 'warmup' if repeat < 2 else 'measurement',
                            ok=True, resource_id='resource', environment_sha256='environment', product_sha256=CANDIDATE)
                        wave_id = digest(dict(group=gid, campaign=campaign, configuration=config,
                                              load=load, repeat=repeat, qualification=qualification))
                        common['wave_id'] = wave_id
                        query_rows, profiles = [], []
                        for index in range(count):
                            queued_ns = index//workers*service_ns if load == 'burst8' else 0
                            total = queued_ns+service_ns
                            row = dict(common, query_index=index, t0_ns=start,
                                t1_ns=start+queued_ns+service_ns//2, t2_ns=start+total,
                                total_ns=total, discovery_ns=queued_ns+service_ns//2, value_ns=service_ns//2,
                                oracle_verified=True, mode_verified=True, selected_units=group['K_units'],
                                owned_output_bytes=group['owned_output_bytes'],
                                execution_mode='native-hdf5' if group['format'] == 'hdf5' else 'iotdb-indexed')
                            query_rows.append(row)
                            if load != 'FS-index':
                                parts = 2 if group['format'] == 'hdf5' else 3
                                for part in range(parts):
                                    lo = start+queued_ns+service_ns*part//parts
                                    hi = start+queued_ns+service_ns*(part+1)//parts
                                    profiles.append(dict(sequence=index*parts+part+1, ok=True,
                                        operation=('dataset_catalog' if group['format']=='hdf5' else 'format_catalog')
                                            if part == 0 else 'typed_batch' if group['format']=='hdf5' else 'typed',
                                        steady_start_ns=lo, steady_end_ns=hi, server_total_ns=hi-lo,
                                        counters={'query_tasks.queue_nanoseconds':queued_ns if part == 0 else 0},
                                        runtime_config=dict(query_task_workers=workers, query_task_max_queued=64,
                                                            query_task_max_batch=min(4,workers))))
                        end = max(row['t2_ns'] for row in query_rows)
                        sample_before, sample_after = resource(start-1), resource(end+1)
                        for row in query_rows:
                            row.update(resource_before=sample_before, resource_after=sample_after)
                        intervals = m.query_task_client.analyze_intervals(profiles,(start,end)) if profiles else {}
                        waves.append(dict(common, resource_before=sample_before, resource_after=sample_after,
                            complete=True, profiles=profiles, intervals=intervals, start_ns=start, end_ns=end,
                            makespan_ns=end-start, throughput_qps=count*1e9/(end-start), query_count=count,
                            inflight=count, client_peak_inflight=count, errors=[]))
                        rows.extend(query_rows)
    return bindings, rows, waves


class QueryTaskProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.runner = runner()

    def test_exact_2160_formal_and_60_qualification_queries_reuse_the_two_scales(self):
        formal, qual = m.selected_groups(REPO,128), m.selected_groups(REPO,8)
        self.assertEqual(len(m.expected_keys(formal)),2160)
        self.assertEqual(len(m.expected_keys(qual,True)),60)
        self.assertEqual(self.runner.expected_rows(SimpleNamespace(repo=REPO,tier='n128')),2160)
        for groups, n in ((formal,128),(qual,8)):
            self.assertEqual({g['format'] for g in groups},{'grib','hdf5'})
            for g in groups:
                self.assertEqual((g['N'],g['C_paths'],g['C_versions'],g['V_stored'],g['D'],g['K_units'],
                                  g['owned_output_bytes'],g['selection']),(n,n,n,1,4,2,40960,'latest'))

    def test_rotating_order_gives_each_pool_each_position_once(self):
        self.assertEqual([m.configuration_order(c) for c in (0,1,2)],
                         [('S1','S2','S4'),('S2','S4','S1'),('S4','S1','S2')])
        for position in range(3):
            self.assertEqual({m.configuration_order(c)[position] for c in (0,1,2)},set(m.CONFIGS))
        for bad in (-1,3):
            with self.assertRaises(RuntimeError):m.configuration_order(bad)

    def test_offered_queries_and_repeats_do_not_scale_with_server_workers(self):
        keys = m.expected_keys(m.selected_groups(REPO,128))
        for config in ('S1','S2','S4'):
            cell = [k for k in keys if k[2] == config]
            self.assertEqual(len(cell),720)
            self.assertEqual(sum(k[3]=='burst8' for k in cell),576)
            self.assertEqual(sum(k[3]=='single' for k in cell),72)
            self.assertEqual(sum(k[3]=='FS-index' for k in cell),72)

    def test_launcher_changes_only_pool_controls_and_keeps_owner_pool_four(self):
        base = {'unchanged.cache':'true','storage.version_directory.reader_pool_size':'4'}
        def emit(path,*args,**kwargs):
            path.write_text('SETTINGS='+repr(base)+'\n')
            return dict(path=str(path),settings=deepcopy(base),sha256=file_sha(path))
        for fmt in ('grib','hdf5'):
            for config,workers in (('S1',1),('S2',2),('S4',4)):
                path = self.root/(fmt+config)
                with patch.object(m.NativeABRunner,'launcher',side_effect=lambda _,*a,**k:emit(*a,**k)), \
                     patch.object(m.run.infrastructure,'launcher',side_effect=emit):
                    receipt = self.runner.launcher(path,Path('/binary'),CANDIDATE,'namespace',Path('/journal'),
                                                   dict(group=dict(format=fmt)),config)
                self.assertEqual(receipt['settings'],dict(base,**{'query.tasks.worker_threads':str(workers),
                    'query.tasks.max_queued_tasks':'64','query.tasks.max_batch_size':str(min(4,workers))}))
                self.assertEqual(receipt['sha256'],file_sha(path))
                self.assertEqual(base,{'unchanged.cache':'true','storage.version_directory.reader_pool_size':'4'})

    def test_source_proof_keeps_binary_and_scientific_identity(self):
        inherited = dict(product=dict(path='/immutable/product',sha256=CANDIDATE),
            baseline=dict(path='/immutable/product',sha256=CANDIDATE),
            cases=[dict(corpus_id='frozen-corpus',case_id='frozen-case',head='frozen-head')],
            scientific_publication=False,index_rebuilt=False,new_cases=False,location_bindings_created=False)
        before = deepcopy(inherited)
        with patch.object(m.OwnerReaderPoolRunner,'source_proof',return_value=deepcopy(inherited)), \
             patch.object(self.runner,'verify_auxiliary',return_value=(dict(complete=True,binary='probe'),'auxiliary-sha')):
            proof = self.runner.source_proof(SimpleNamespace(repo=REPO,tier='n128'))
        for key in before:self.assertEqual(proof[key],before[key])
        self.assertEqual((proof['expected_queries'],proof['qualification_queries']),(2160,60))
        self.assertEqual(proof['load_concurrency'],{'single':1,'burst8':8,'FS-index':1})
        self.assertEqual(proof['reader_pool_size'],4)
        self.assertEqual(proof['auxiliary_binding_sha256'],'auxiliary-sha')

    def test_auxiliary_gate_rejects_frozen_probe_input_and_reused_object_drift(self):
        repo=self.root/'aux-repo';stage=repo/m.WORK;stage.mkdir(parents=True)
        build=self.root/'accepted-build';(build/'bin').mkdir(parents=True)
        candidate=build/'bin/iotdb_fs';candidate.write_bytes(CANDIDATE_BYTES)
        paths={}
        for name in ('binary','hdf5_source','probe_source'):
            paths[name]=stage/name;paths[name].write_bytes(('frozen '+name).encode())
        obj=build/'product.cpp.o';obj.write_bytes(b'accepted candidate object')
        build_manifest=stage/'build-manifest.json'
        build_manifest.write_text(json.dumps(dict(build_root=str(build),binary_sha256=file_sha(paths['binary']),
            reused_files_unchanged_after_build=True,reused_files=[dict(path=str(obj),sha256=file_sha(obj))])))
        libraries={'libfrozen.so':dict(path='/same/library',sha256='bytes')}
        binding=dict(complete=True,build_manifest=str(build_manifest),build_manifest_sha256=file_sha(build_manifest),
                     runtime_libraries=libraries)
        for name,path in paths.items():binding.update({name:str(path),name+'_sha256':file_sha(path)})
        binding_path=stage/'auxiliary-binding.json';binding_path.write_text(json.dumps(binding))
        args=SimpleNamespace(repo=repo,binary=candidate)
        with patch.object(m,'auxiliary_libraries',return_value=libraries):
            accepted,sha=self.runner.verify_auxiliary(args)
            self.assertEqual((accepted,sha),(binding,file_sha(binding_path)))
            for path in (*paths.values(),obj,build_manifest):
                with self.subTest(changed=path.name):
                    original=path.read_bytes();path.write_bytes(original+b' drift')
                    with self.assertRaises(RuntimeError):self.runner.verify_auxiliary(args)
                    path.write_bytes(original)
            with patch.object(m,'auxiliary_libraries',return_value={}):
                with self.assertRaises(RuntimeError):self.runner.verify_auxiliary(args)
            wrong_build=self.root/'other-build';wrong_build.mkdir()
            manifest=json.loads(build_manifest.read_text());manifest['build_root']=str(wrong_build)
            build_manifest.write_text(json.dumps(manifest))
            binding['build_manifest_sha256']=file_sha(build_manifest)
            binding_path.write_text(json.dumps(binding))
            with self.assertRaisesRegex(RuntimeError,'product objects'):self.runner.verify_auxiliary(args)

    def write_analysis(self):
        # The real supervisor permits an arbitrary fresh run directory. Keep
        # the established layout here while testing analysis data, not paths.
        repo = self.root/'repo'; work = repo/'experiments/cross_version/work/query-task/run'
        work.mkdir(parents=True)
        (repo/'experiments/fixed_matrix').mkdir(parents=True)
        (repo/'experiments/fixed_matrix/catalog.json').write_bytes((REPO/'experiments/fixed_matrix/catalog.json').read_bytes())
        selected = m.selected_groups(REPO,128); qgroups = m.selected_groups(REPO,8)
        bindings,rows,waves = science(selected); qb,qual,qwaves = science(qgroups,True);bindings.update(qb)
        def save(name,items):(work/name).write_text(''.join(json.dumps(x)+'\n' for x in items))
        save('queries.jsonl',rows);save('waves.jsonl',waves);save('qualification.jsonl',qual);save('qualification-waves.jsonl',qwaves)
        (work/'qualification-report.json').write_text(json.dumps(dict(complete=True,query_records=60,
            groups=qgroups,queries_sha256=file_sha(work/'qualification.jsonl'),performance_statistics=False)))
        return work,bindings,selected,rows,waves

    def analyze(self,work,bindings,selected):
        with patch.object(m.run,'validate_coverage',return_value=dict(complete=True)):
            return self.runner.analyze(work/'queries.jsonl',bindings,selected,'resource',CANDIDATE)

    def test_descriptive_analysis_uses_all_waves_and_pairs_each_campaign(self):
        work,b,g,rows,waves = self.write_analysis()
        result = self.analyze(work,b,g)
        self.assertEqual((result['query_rows'],result['wave_rows'],result['qualification_rows']),(2160,648,60))
        self.assertEqual((len(result['cells']),len(result['effects'])),(18,12))
        burst = [c for c in result['cells'] if c['load']=='burst8']
        for cell in burst:
            workers = cell['scheduler_workers']
            self.assertAlmostEqual(cell['throughput_qps'],50*workers)
            self.assertEqual([x['queries'] for x in cell['blocks']],[80,80,80])
            self.assertEqual([x['throughput_qps'] for x in cell['blocks']],
                             [100*workers,50*workers,25*workers])
        for effect in result['effects']:
            self.assertEqual([x['campaign'] for x in effect['blocks']],[0,1,2])
            self.assertAlmostEqual(effect['throughput_ratio'],m.WORKERS[effect['configuration']]
                                   if effect['load']=='burst8' else 1)

    def test_rows_reject_loss_duplicate_wrong_identity_output_budget_clock_and_pool(self):
        groups = m.selected_groups(REPO,128); bindings,rows,_ = science(groups)
        self.runner.validate_rows(rows,bindings,groups,'resource',CANDIDATE)
        changes = [lambda x:x.pop(),lambda x:x.append(deepcopy(x[0])),
            lambda x:x[0].update(case_id='other-case'),lambda x:x[0].update(product_sha256='other-binary'),
            lambda x:x[0].update(resource_id='other-resource'),lambda x:x[0].update(reader_pool_size=1),
            lambda x:x[0].update(scheduler_workers=4),lambda x:x[0].update(owned_output_bytes=40959),
            lambda x:x[0].update(total_ns=1),lambda x:x[0].update(oracle_verified=False),
            lambda x:x[0].update(phase='measurement'),
            lambda x:x[0]['resource_before']['cgroups']['query']['files'].update({'memory.max':'max'})]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                changed=deepcopy(rows);change(changed)
                with self.assertRaises(RuntimeError):self.runner.validate_rows(changed,bindings,groups,'resource',CANDIDATE)

    def test_analysis_rejects_replaced_wave_even_when_cell_and_total_counts_match(self):
        work,b,g,rows,waves = self.write_analysis()
        self.analyze(work,b,g)
        # Repeat 3 is silently lost; repeat 2 is duplicated in the same cell.
        a = next(i for i,w in enumerate(waves) if w['load']=='burst8' and w['repeat']==2)
        z = next(i for i,w in enumerate(waves) if w['load']=='burst8' and w['repeat']==3)
        waves[z]=deepcopy(waves[a]);(work/'waves.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in waves))
        with self.assertRaises(RuntimeError):self.analyze(work,b,g)

    def test_analysis_rejects_fabricated_throughput_and_changed_offered_load(self):
        work,b,g,rows,waves = self.write_analysis()
        self.analyze(work,b,g)
        for changes in (dict(query_count=800),dict(inflight=4),dict(complete=False),dict(makespan_ns=-1)):
            with self.subTest(changes=changes):
                changed=deepcopy(waves)
                next(w for w in changed if w['load']=='burst8' and w['phase']=='measurement').update(changes)
                (work/'waves.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in changed))
                with self.assertRaises(RuntimeError):self.analyze(work,b,g)

    def test_analysis_rejects_durations_that_disagree_with_saved_clock_points(self):
        work,b,g,rows,waves = self.write_analysis()
        self.analyze(work,b,g)
        row = next(r for r in rows if r['load']=='burst8' and r['phase']=='measurement')
        # Keep T=d+q and all wave data unchanged; the raw timestamps expose
        # invented query latency even though the old residual check passes.
        for key in ('total_ns','discovery_ns','value_ns'):row[key]*=100
        (work/'queries.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in rows))
        with self.assertRaises(RuntimeError):self.analyze(work,b,g)

    def test_execution_mean_weights_elapsed_time_instead_of_weighting_short_waves_equally(self):
        work,b,g,rows,waves = self.write_analysis()
        target = next(w for w in waves if w['configuration']=='S4' and w['load']=='burst8'
                      and w['campaign']==0 and w['repeat']==2)
        extension = target['makespan_ns']*9
        target['end_ns'] += extension;target['makespan_ns'] += extension
        target['throughput_qps']=target['query_count']*1e9/target['makespan_ns']
        target['resource_after']=resource(target['end_ns']+1)
        target['intervals']=m.query_task_client.analyze_intervals(target['profiles'],(target['start_ns'],target['end_ns']))
        for row in rows:
            if row['wave_id']==target['wave_id']:
                row['t2_ns']+=extension;row['total_ns']+=extension;row['value_ns']+=extension
                row['resource_after']=target['resource_after']
        (work/'waves.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in waves))
        (work/'queries.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in rows))
        result = self.analyze(work,b,g)
        cell=next(c for c in result['cells'] if c['group_id']==target['group_id']
                  and c['configuration']=='S4' and c['load']=='burst8')
        # Ten waves have 800 ms integrated execution, over 380 ms elapsed.
        self.assertAlmostEqual(cell['blocks'][0]['execution_mean'],40/19)

    def binding_fixture(self):
        source=self.root/'frozen/source';(source/'src').mkdir(parents=True)
        product=source/'src/scheduler.cpp';product.write_text('frozen product source\n')
        manifest=self.root/'source-manifest.json';manifest.write_text(json.dumps({'src/scheduler.cpp':file_sha(product)}))
        build=self.root/'frozen/build';(build/'bin').mkdir(parents=True)
        binary=build/'bin/iotdb_fs';binary.write_bytes(CANDIDATE_BYTES)
        unit=build/'bin/unit_tests';unit.write_bytes(b'accepted unit executable')
        xml=self.root/'unit-tests.xml';root=ET.Element('testsuites',tests='1981',failures='0',errors='0',skipped='0')
        suite=ET.SubElement(root,'testsuite',tests='1981')
        for index in range(1981):
            classname,name=REGRESSION if index==0 else ('Other','Case'+str(index))
            ET.SubElement(suite,'testcase',classname=classname,name=name,status='run',result='completed')
        ET.ElementTree(root).write(xml)
        cache=build/'CMakeCache.txt';cache.write_text('CMAKE_HOME_DIRECTORY:INTERNAL='+str(source)+'\nCMAKE_BUILD_TYPE:STRING=Release\n')
        libraries={'libfrozen.so':dict(path='/same/library',sha256='bytes')}
        binding=dict(complete=True,candidate_binary=str(binary),candidate_sha256=CANDIDATE,
            candidate_source_root=str(source),source_manifest_path=str(manifest),source_manifest_sha256=file_sha(manifest),
            candidate_unit_binary=str(unit),candidate_unit_binary_sha256=file_sha(unit),
            unit_tests_path=str(xml),unit_tests_sha256=file_sha(xml),candidate_build_cache_sha256=file_sha(cache),
            runtime_libraries=libraries)
        path=self.root/'binding.json';path.write_text(json.dumps(binding))
        return SimpleNamespace(binary=binary,baseline_binary=binary,build_binding=path),binding,source,xml,libraries

    def test_one_candidate_gate_rejects_binary_source_xml_and_dependency_drift(self):
        args,binding,source,xml,libraries=self.binding_fixture()
        with patch.object(m,'resolved_libraries',return_value=libraries):
            self.assertTrue(self.runner.verify_candidate(args)['one_candidate'])
            with patch.object(m,'resolved_libraries',return_value={'libdifferent.so':dict(path='/other',sha256='other')}), \
                 self.assertRaisesRegex(RuntimeError,'runtime libraries'):
                self.runner.verify_candidate(args)
            args.baseline_binary=self.root/'different-binary'
            with self.assertRaises(RuntimeError):self.runner.verify_candidate(args)
            args.baseline_binary=args.binary
            product=source/'src/scheduler.cpp';original=product.read_text();product.write_text('changed')
            with self.assertRaises(RuntimeError):self.runner.verify_candidate(args)
            product.write_text(original)
            root=ET.parse(xml);next(root.getroot().iter('testcase')).set('name','regression-removed');root.write(xml)
            binding['unit_tests_sha256']=file_sha(xml);args.build_binding.write_text(json.dumps(binding))
            with self.assertRaisesRegex(RuntimeError,'contention'):self.runner.verify_candidate(args)

    def test_candidate_gate_rejects_extra_unmanifested_source(self):
        args,binding,source,xml,libraries=self.binding_fixture()
        (source/'src/extra.cpp').write_text('unrecorded product input')
        with patch.object(m,'resolved_libraries',return_value=libraries),self.assertRaises(RuntimeError):
            self.runner.verify_candidate(args)


if __name__ == '__main__':unittest.main()
