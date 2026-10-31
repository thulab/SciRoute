"""Sealed FM1 N128 common scheduler 1/2/4 comparison.

One candidate, fixed input concurrency, complete owned-array clock. The inherited
supervisor owns the original database/resource window and restores it on failure.
No scientific publication, native binding, or index rebuild is allowed.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import subprocess
import xml.etree.ElementTree as ET

from . import client, query_task_client, run, telemetry
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, PATH_ARGS
from fs_index.native import NativeContext
from .owner_reader_pool import OwnerReaderPoolRunner, selected_groups, cmake_cache, resolved_libraries
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-query-task-n128/2'
WORK = 'experiments/cross_version/work/query-task-n128-20261025-r2'
CONFIGS = ('S1', 'S2', 'S4')
WORKERS = {'S1': 1, 'S2': 2, 'S4': 4}
ORDERS = (CONFIGS, ('S2', 'S4', 'S1'), ('S4', 'S1', 'S2'))
CAMPAIGNS = (0, 1, 2)
LOADS = ('single', 'burst8', 'FS-index')
WARMUPS, MEASURED, REPEATS = 2, 10, 12
need = run.need


def configuration_order(campaign):
    need(campaign in CAMPAIGNS, 'unknown scheduler campaign')
    return ORDERS[campaign]


def expected_keys(groups, qualification=False):
    return {(g['group_id'], campaign, config, load, repeat, query)
            for g in groups for campaign in ((0,) if qualification else CAMPAIGNS)
            for config in CONFIGS for load in LOADS
            for repeat in range(1 if qualification else REPEATS)
            for query in range(8 if load == 'burst8' else 1)}


def percentile(values, fraction):
    values = sorted(values)
    need(bool(values) and 0 <= fraction <= 1, 'invalid percentile input')
    position = (len(values)-1)*fraction
    low = int(position); high = min(low+1, len(values)-1)
    return values[low] + (values[high]-values[low])*(position-low)


def auxiliary_libraries(binary):
    """The no-IoTDB helper requires Arrow/HDF, not a database client link."""
    reply=subprocess.run(['ldd',str(binary)],check=True,capture_output=True,text=True,timeout=30)
    need('not found' not in reply.stdout,'unresolved auxiliary shared library')
    libraries={}
    for line in reply.stdout.splitlines():
        paths=re.findall(r'(/\S+)',line)
        if not paths: continue
        path=Path(paths[0]).resolve(strict=True)
        libraries[path.name]=dict(path=str(path),sha256=file_sha(path))
    need(all(any(name.startswith(prefix) for name in libraries) for prefix in ('libarrow.so.','libhdf5.so.')),
         'auxiliary Arrow/HDF libraries absent')
    return libraries


class QueryTaskRunner(OwnerReaderPoolRunner):
    def expected_rows(self, args):
        return len(expected_keys(self.groups(args.repo, args.tier)))

    def verify_candidate(self, args):
        binding = read_json(args.build_binding)
        binary = args.binary
        need(binding.get('complete') is True and args.baseline_binary == binary and
             file_sha(binary) == binding['candidate_sha256'] == self.spec.baseline_sha,
             'one accepted candidate binary required for every scheduler configuration')
        source = Path(binding['candidate_source_root']).resolve(strict=True)
        manifest_path = Path(binding['source_manifest_path'])
        need(file_sha(manifest_path) == binding['source_manifest_sha256'], 'source manifest changed')
        sources = read_json(manifest_path)
        for name, sha in sources.items():
            path = source/name
            need(not Path(name).is_absolute() and '..' not in Path(name).parts and
                 path.is_file() and not path.is_symlink() and file_sha(path) == sha,
                 'frozen candidate source changed: '+name)
        need({str(p.relative_to(source)) for p in source.rglob('*') if p.is_file() and
              '__pycache__' not in p.relative_to(source).parts} == set(sources),
             'frozen source manifest must enumerate the exact closure')
        need(Path(binding['candidate_unit_binary']).resolve(strict=True).parent == binary.parent,
             'unit binary must belong to the same candidate build')
        for key, sha_key in (('candidate_unit_binary','candidate_unit_binary_sha256'),
                            ('unit_tests_path','unit_tests_sha256')):
            need(file_sha(Path(binding[key])) == binding[sha_key], 'unit evidence changed: '+key)
        xml = ET.parse(binding['unit_tests_path']).getroot()
        cases = list(xml.iter('testcase'))
        need(len(cases) == int(xml.get('tests', 0)) == 1981 and
             not any(list(xml.iter(tag)) for tag in ('failure','error','skipped')) and
             all(c.get('status') == 'run' and c.get('result') == 'completed' for c in cases) and
             all(int(n.get(k, 0)) == 0 for n in xml.iter() if n.tag in ('testsuite','testsuites')
                 for k in ('failures','errors','disabled','skipped')), '1981 complete tests required')
        for suite, name in self.spec.required_regression_tests:
            need(sum(c.get('classname') == suite and c.get('name') == name for c in cases) == 1,
                 'required contention test absent: '+name)
        cache_path = binary.parents[1]/'CMakeCache.txt'; cache = cmake_cache(cache_path)
        need(file_sha(cache_path) == binding['candidate_build_cache_sha256'] and
             Path(cache['CMAKE_HOME_DIRECTORY']).resolve(strict=True) == source and
             cache['CMAKE_BUILD_TYPE'] == 'Release', 'candidate build binding changed')
        libraries = resolved_libraries(binary)
        need(libraries == binding['runtime_libraries'], 'actual runtime libraries changed')
        return dict(complete=True, candidate_sha256=file_sha(binary), baseline_sha256=file_sha(binary),
                    candidate_source_root=str(source), source_files=len(sources),
                    source_manifest_sha256=file_sha(manifest_path), unit_tests=1981,
                    unit_tests_sha256=file_sha(Path(binding['unit_tests_path'])),
                    runtime_libraries=libraries, one_candidate=True)

    def verify_auxiliary(self, args):
        aux_path = args.repo/WORK/'auxiliary-binding.json'
        auxiliary = read_json(aux_path)
        need(auxiliary.get('complete') is True and
             file_sha(Path(auxiliary['binary'])) == auxiliary['binary_sha256'] and
             file_sha(Path(auxiliary['build_manifest'])) == auxiliary['build_manifest_sha256'] and
             file_sha(Path(auxiliary['hdf5_source'])) == auxiliary['hdf5_source_sha256'] and
             file_sha(Path(auxiliary['probe_source'])) == auxiliary['probe_source_sha256'] and
             auxiliary_libraries(Path(auxiliary['binary'])) == auxiliary['runtime_libraries'],
             'auxiliary probe build/source/smoke binding changed')
        build = read_json(auxiliary['build_manifest'])
        need(Path(build['build_root']).resolve(strict=True) == args.binary.parents[1] and
             build.get('reused_files_unchanged_after_build') is True and
             all(file_sha(Path(item['path'])) == item['sha256'] for item in build['reused_files']),
             'auxiliary reused product objects changed')
        return auxiliary,file_sha(aux_path)

    def source_proof(self, args):
        proof = super().source_proof(args)
        proof.pop('pools', None)
        auxiliary,auxiliary_sha = self.verify_auxiliary(args)
        proof.update(protocol=PROTOCOL, configurations=list(CONFIGS),
                     query_task_workers=WORKERS, reader_pool_size=4, queue_capacity=64,
                     configuration_order=[list(order) for order in ORDERS],
                     arms=list(LOADS), expected_queries=self.expected_rows(args),
                     qualification_queries=len(expected_keys(selected_groups(args.repo, 8), True)),
                     load_concurrency={'single':1, 'burst8':8, 'FS-index':1},
                     client_executor='one asyncio event loop; no query worker threads',
                     clock='first entry to final client-owned values/masks; profiles and oracle outside',
                     auxiliary_mixed_scope='separate no-IoTDB mechanism probe; not an FM1 case',
                     auxiliary_binding=auxiliary,auxiliary_binding_sha256=auxiliary_sha)
        return proof

    def collect_auxiliary(self,args,proof,report):
        binding=proof['auxiliary_binding'];paths=read_json(args.work/'resource-paths.json')
        index=[]
        for campaign in CAMPAIGNS:
            for configuration in configuration_order(campaign):
                for scenario in ('hdf5','arrow','mixed','mixed-held'):
                    root=args.work/'auxiliary'/('campaign-'+str(campaign))/configuration/scenario
                    root.parent.mkdir(parents=True,exist_ok=True)
                    rb=telemetry.sample(paths);run.validate_sample(rb)
                    command=[binding['binary'],binding['hdf5_source'],binding['dataset'],str(root),
                        str(WORKERS[configuration]),scenario,str(WARMUPS),str(MEASURED),str(campaign),'4096']
                    print(json.dumps(dict(event='mixed_probe',campaign=campaign,configuration=configuration,scenario=scenario)),flush=True)
                    with root.with_suffix('.stdout').open('x') as out,root.with_suffix('.stderr').open('x') as err:
                        completed=subprocess.run(command,stdout=out,stderr=err,timeout=180)
                    ra=telemetry.sample(paths);run.validate_sample(ra)
                    need(completed.returncode == 0, 'auxiliary probe failed; preserve evidence, no automatic retry')
                    requests=[json.loads(s) for s in (root/'requests.jsonl').read_text().splitlines()]
                    waves=[json.loads(s) for s in (root/'waves.jsonl').read_text().splitlines()]
                    profiles=[json.loads(s) for s in (root/'profile.jsonl').read_text().splitlines()]
                    manifest=read_json(root/'manifest.json')
                    need(manifest['workers']==WORKERS[configuration] and manifest['scenario']==scenario and
                         manifest['campaign']==str(campaign) and manifest['warm_waves']==WARMUPS and
                         manifest['measured_waves']==MEASURED and manifest['outstanding_per_wave']==8 and
                         manifest['source_count_per_query']==1 and manifest['client_threads']==1,
                         'auxiliary actual parameters differ')
                    need(len(requests)==REPEATS*8 and len(waves)==REPEATS and len(profiles)==REPEATS*8 and
                         all(r['values_and_masks_ok'] is True for r in requests), 'auxiliary output incomplete')
                    need({(r['wave'],r['request']) for r in requests} == {(w,q) for w in range(REPEATS) for q in range(8)},
                         'auxiliary request matrix differs')
                    need({w['wave'] for w in waves}==set(range(REPEATS)) and all(
                        w['warm']==(w['wave']<WARMUPS) and w['after']['completed']-w['before']['completed']==8 and
                        w['after']['failed']==w['before']['failed'] for w in waves), 'auxiliary wave completion differs')
                    need(all(p.get('ok') is True and p['runtime_config']['query_task_workers']==WORKERS[configuration]
                             for p in profiles), 'auxiliary actual execution configuration differs')
                    need(all(w['arrow_owned_before_unlock']==4 for w in waves) if scenario=='mixed-held' else True,
                         'Arrow failed to advance before held native mutex release')
                    receipt=dict(campaign=campaign,configuration=configuration,workers=WORKERS[configuration],scenario=scenario,
                        root=str(root),binary_sha256=binding['binary_sha256'],command=command,requests=len(requests),waves=len(waves),
                        resource_before=rb,resource_after=ra,artifacts={name:file_sha(root/name) for name in
                            ('manifest.json','requests.jsonl','waves.jsonl','scheduler.jsonl','profile.jsonl')})
                    index.append(receipt)
                    atomic_json(args.work/'auxiliary-index.json',dict(complete=False,cells=index))
                    report['auxiliary_cells']=len(index);atomic_json(args.work/'report.json',report)
        need(len(index)==36,'complete auxiliary matrix required')
        atomic_json(args.work/'auxiliary-index.json',dict(complete=True,cells=index,requests=3456,waves=432,
            scope='auxiliary fixture metadata/read-only input; no IoTDB; not an N128 FM1 result'))

    def launcher(self, path, binary, sha, namespace, journal, plan, configuration):
        need(configuration in CONFIGS, 'unknown scheduler configuration')
        receipt = (NativeABRunner.launcher(self, path, binary, sha, namespace, journal, plan)
                   if plan['group']['format'] == 'hdf5' else
                   run.infrastructure.launcher(path, binary, sha, namespace, journal))
        declaration = 'SETTINGS='+repr(receipt['settings'])+'\n'; source = path.read_text()
        need(source.count(declaration) == 1, 'launcher settings layout differs')
        settings = dict(receipt['settings'], **{
            'storage.version_directory.reader_pool_size':'4',
            'query.tasks.worker_threads':str(WORKERS[configuration]),
            'query.tasks.max_queued_tasks':'64',
            'query.tasks.max_batch_size':str(min(4, WORKERS[configuration]))})
        path.write_text(source.replace(declaration, 'SETTINGS='+repr(settings)+'\n', 1))
        receipt.update(settings=settings, sha256=file_sha(path))
        return receipt

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired, *, qualification=False):
        paths = read_json(args.work/'resource-paths.json'); count = 0
        filename = 'qualification.jsonl' if qualification else 'queries.jsonl'
        waves_file = 'qualification-waves.jsonl' if qualification else 'waves.jsonl'
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        for campaign in ((0,) if qualification else CAMPAIGNS):
            plans = sorted((p for p in execution if (p['group']['N'] == 8) == qualification),
                           key=lambda p: ('grib','hdf5').index(p['group']['format']), reverse=campaign % 2 == 1)
            for ordinal, plan in enumerate(plans):
                group = plan['group']; gid = group['group_id']; bound = bindings[gid]
                for configuration in configuration_order(campaign):
                    block = plan['execution_root']/('campaign-qualification' if qualification else 'campaign-'+str(campaign))/configuration
                    block.mkdir(parents=True)
                    journal = block/'owner'; shutil.copytree(plan['journal'], journal)
                    need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'journal copy differs')
                    binary = args.binary; sha = file_sha(binary)
                    launch = self.launcher(block/'launcher', binary, sha, plan['item']['namespace'], journal, plan, configuration)
                    atomic_json(block/'launcher.receipt.json', launch)
                    before, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-before')
                    need(before == plan['scientific_head'], 'head changed before query mount')
                    print(json.dumps(dict(event='scheduler_mount', qualification=qualification,
                        campaign=campaign, group=gid, configuration=configuration)), flush=True)
                    with paired.sf.private_mount(Path(launch['path']), block/'mount', env, writer, profile=True) as context, ExitStack() as stack:
                        need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha, 'running binary differs')
                        actual = read_json(context['root']/'profile.launch.json')
                        need(actual['settings'] == launch['settings'], 'actual scheduler settings differ')
                        native = stack.enter_context(NativeContext(args.library, dict(plan['cfg'], build_index=False)))
                        environment = telemetry.environment(args.repo, artifact_paths=[args.library,binary],
                            process_roles=dict(client=os.getpid(), daemon=context['pid'],
                                datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())), data_path=plan['root'])
                        atomic_json(block/'environment.json', environment)
                        mount = dict(campaign=campaign, configuration=configuration, qualification=qualification,
                            daemon_pid=context['pid'], startup_ns=context['startup_ns'], product_sha256=sha,
                            scheduler_workers=WORKERS[configuration], settings=launch['settings'],
                            environment_sha256=file_sha(block/'environment.json'))
                        plan['item']['mounts'].append(mount)
                        atomic_json(block/'query-plan.json', native.explain('index', bound['native_selection'], client.PREDICATE, bound['scope']))
                        for load in LOADS:
                            model = build_run(registry.get_case(bound['case_id']), execution=str(block), arm=load,
                                implementation=dict(binary_sha256=run.NATIVE_SHA if load == 'FS-index' else sha,
                                    runner_sha256=digest(frozen['sources']), engine='SQLite index + native owned arrays' if load == 'FS-index'
                                        else 'native-hdf5 cache+batch' if group['format'] == 'hdf5' else 'format_catalog + two sequential typed'),
                                clock=dict(kind='client-owned-full-query', d='entry through discovery delivery',
                                    q='discovery through owned Float32/mask', profile_oracle='outside',
                                    throughput='query count / first entry through last owned output'),
                                cache=dict(protocol=PROTOCOL, configuration=configuration, reader_pool_size=4,
                                    native_helper=plan['cfg'] if load == 'FS-index' else None,
                                    daemon_settings=launch['settings'] if load != 'FS-index' else None),
                                environment=dict(resource_id=frozen['resource_id'], process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign, qualification=qualification, warmup=0 if qualification else WARMUPS,
                                    measured=0 if qualification else MEASURED, inflight=8 if load == 'burst8' else 1,
                                    schedule='rotating worker order and per-repeat load order; all fixed rows retained'))
                            registered = registry.register_run(model)
                            bound['run_ids'].setdefault(str(campaign), {}).setdefault(configuration, {})[load] = registered['run_id']
                        atomic_json(args.work/'bindings.json', bindings)
                        for repeat in range(1 if qualification else REPEATS):
                            shift = (campaign+repeat+ordinal+CONFIGS.index(configuration)) % len(LOADS)
                            for load in LOADS[shift:]+LOADS[:shift]:
                                rb = telemetry.sample(paths); run.validate_sample(rb)
                                metadata = dict(group_id=gid, corpus_id=bound['corpus_id'], case_id=bound['case_id'],
                                    oracle_sha256=bound['oracle_sha256'], run_id=bound['run_ids'][str(campaign)][configuration][load],
                                    campaign=campaign, configuration=configuration, scheduler_workers=WORKERS[configuration],
                                    reader_pool_size=4, load=load, arm='FS-index' if load == 'FS-index' else 'IoTDB-FS', repeat=repeat,
                                    phase='qualification' if qualification else 'warmup' if repeat < WARMUPS else 'measurement',
                                    resource_id=frozen['resource_id'], environment_sha256=mount['environment_sha256'], product_sha256=sha)
                                if load == 'FS-index':
                                    row = client.query_native(native, 'FS-index', group, bound['scope'],
                                        bound['native_selection'], bound['expected_matches'])
                                    wave = dict(rows=[dict(row, query_index=0)], profiles=[], complete=True,
                                        makespan_ns=row['total_ns'], throughput_qps=1e9/row['total_ns'], query_count=1,
                                        intervals={}, inflight=1, client_peak_inflight=1)
                                else:
                                    try:
                                        wave = query_task_client.run_wave(context, group, bound['scope'], bound['physical_versions'],
                                            bound['expected_matches'], inflight=8 if load == 'burst8' else 1)
                                    except query_task_client.WaveError as error:
                                        atomic_json(block/('failed-wave-'+load+'-'+str(repeat)+'.json'), error.result)
                                        raise
                                    for profile in wave['profiles']:
                                        need(profile['runtime_config']['query_task_workers'] == WORKERS[configuration], 'profile worker count differs')
                                    need(wave['intervals']['execution']['max_parallel'] <= WORKERS[configuration], 'actual execution exceeds worker bound')
                                ra = telemetry.sample(paths); run.validate_sample(ra)
                                need(wave['complete'] and len(wave['rows']) == (8 if load == 'burst8' else 1), 'incomplete wave')
                                common = dict(metadata, resource_before=rb, resource_after=ra, ok=True)
                                wave_id = digest(dict(group=gid, campaign=campaign, configuration=configuration,
                                                      load=load, repeat=repeat, qualification=qualification))
                                run.append(args.work/waves_file, dict(common, wave_id=wave_id,
                                    **{k:v for k,v in wave.items() if k != 'rows'}))
                                for row in wave['rows']:
                                    row.update(common, wave_id=wave_id)
                                    run.append(args.work/filename, row); count += 1
                                report['qualification_records' if qualification else 'query_records'] = count
                            atomic_json(args.work/'report.json', report)
                    after, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-after')
                    need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'query changed scientific head/journal')
                    need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'], 'query changed sealed FS index')
                    mount.update(complete=True, scientific_head_unchanged=True, journal_unchanged=True)
                    atomic_json(args.work/'report.json', report)
        return count

    def validate_rows(self, rows, bindings, selected, resource_id, candidate_sha, *, qualification=False):
        wanted = expected_keys(selected, qualification)
        keys = {(r['group_id'],r['campaign'],r['configuration'],r['load'],r['repeat'],r['query_index']) for r in rows}
        need(len(rows) == len(wanted) and keys == wanted, 'complete exact query matrix required without replacement')
        groups = {g['group_id']:g for g in selected}
        for row in rows:
            b = bindings[row['group_id']]; g = groups[row['group_id']]
            need(all(row[k] == b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
                 row['run_id'] == b['run_ids'][str(row['campaign'])][row['configuration']][row['load']], 'query identity differs')
            need(row['resource_id'] == resource_id and row['product_sha256'] == candidate_sha and
                 row['scheduler_workers'] == WORKERS[row['configuration']] and row['reader_pool_size'] == 4,
                 'configuration/resource/product identity differs')
            need(all(row.get(k) is True for k in ('ok','oracle_verified','mode_verified')) and
                 row['owned_output_bytes'] == g['owned_output_bytes'] and row['selected_units'] == g['K_units'], 'owned query result differs')
            need(all(type(row[k]) is int and row[k] > 0 for k in ('total_ns','discovery_ns','value_ns')) and
                 row['total_ns'] == row['discovery_ns']+row['value_ns'], 'invalid complete query clock')
            if row['load'] != 'FS-index':
                need(all(type(row[k]) is int for k in ('t0_ns','t1_ns','t2_ns')) and
                     row['t0_ns'] < row['t1_ns'] < row['t2_ns'] and
                     row['total_ns'] == row['t2_ns']-row['t0_ns'] and
                     row['discovery_ns'] == row['t1_ns']-row['t0_ns'] and
                     row['value_ns'] == row['t2_ns']-row['t1_ns'], 'query timestamps and durations differ')
            need(row['phase'] == ('qualification' if qualification else 'warmup' if row['repeat'] < WARMUPS else 'measurement'), 'query phase differs')
            run.validate_sample(row['resource_before']); run.validate_sample(row['resource_after'])
            need(row['resource_before']['monotonic_ns'] < row['resource_after']['monotonic_ns'], 'resource clock reversed')

    def validate_waves(self, waves, rows, bindings, selected, resource_id, candidate_sha, *, qualification=False):
        wanted = {key[:-1] for key in expected_keys(selected, qualification)}
        key_of = lambda row: (row['group_id'],row['campaign'],row['configuration'],row['load'],row['repeat'])
        need(len(waves) == len(wanted) and {key_of(w) for w in waves} == wanted,
             'complete unique wave matrix required')
        grouped = {}
        for row in rows: grouped.setdefault(key_of(row), []).append(row)
        formats = {g['group_id']:g['format'] for g in selected}
        for wave in waves:
            key = key_of(wave); members = grouped[key]
            count = 8 if wave['load'] == 'burst8' else 1
            need(wave.get('complete') is True and wave.get('ok') is True and
                 wave['query_count'] == count and len(members) == count and wave['inflight'] == count and
                 wave['client_peak_inflight'] == count and not wave.get('errors'), 'wave count/complete/load differs')
            need(type(wave['makespan_ns']) is int and wave['makespan_ns'] > 0 and
                 abs(wave['throughput_qps']-count*1e9/wave['makespan_ns']) < 1e-6,
                 'wave throughput/makespan differs')
            expected_id = digest(dict(group=wave['group_id'],campaign=wave['campaign'],configuration=wave['configuration'],
                                     load=wave['load'],repeat=wave['repeat'],qualification=qualification))
            need(wave['wave_id'] == expected_id and all(r['wave_id'] == expected_id for r in members), 'query/wave identity differs')
            need(all(wave[k] == members[0][k] for k in ('group_id','corpus_id','case_id','oracle_sha256','run_id',
                'configuration','scheduler_workers','reader_pool_size','resource_id','product_sha256','phase',
                'resource_before','resource_after','environment_sha256')), 'wave/query provenance differs')
            need(wave['resource_id'] == resource_id and wave['product_sha256'] == candidate_sha,
                 'wave resource/product identity differs')
            profiles = wave['profiles']
            if wave['load'] == 'FS-index':
                need(not profiles and wave['makespan_ns'] == members[0]['total_ns'], 'FS-index wave clock differs')
                continue
            start = min(r['t0_ns'] for r in members); end = max(r['t2_ns'] for r in members)
            need(wave['start_ns'] == start and wave['end_ns'] == end and wave['makespan_ns'] == end-start,
                 'wave must end at final owned output')
            need(len(profiles) == count*(3 if formats[wave['group_id']] == 'grib' else 2) and
                 len({p['sequence'] for p in profiles}) == len(profiles), 'profile count/uniqueness differs')
            for profile in profiles:
                cfg = profile['runtime_config']
                need(profile.get('ok') is True and start <= profile['steady_start_ns'] <= profile['steady_end_ns'] <= end and
                     profile['steady_end_ns']-profile['steady_start_ns'] == profile['server_total_ns'] and
                     cfg['query_task_workers'] == WORKERS[wave['configuration']] and
                     cfg['query_task_max_queued'] == 64 and cfg['query_task_max_batch'] == min(4,WORKERS[wave['configuration']]),
                     'profile clock or actual configuration differs')
            recalculated = query_task_client.analyze_intervals(profiles,(start,end))
            need(wave['intervals'] == recalculated and recalculated['execution']['max_parallel'] <= WORKERS[wave['configuration']],
                 'wave execution overlap differs')

    def analyze(self, path, bindings, selected, resource_id, candidate_sha):
        work = Path(path).parent
        rows = [json.loads(s) for s in Path(path).read_text().splitlines()]
        self.validate_rows(rows, bindings, selected, resource_id, candidate_sha)
        qual = [json.loads(s) for s in (work/'qualification.jsonl').read_text().splitlines()]
        gate = read_json(work/'qualification-report.json')
        need(gate.get('complete') is True and gate.get('query_records') == 60 and
             gate['queries_sha256'] == file_sha(work/'qualification.jsonl'), 'qualification gate incomplete/changed')
        qgroups = gate['groups']
        need(len(qgroups) == 2 and {g['group_id'] for g in qgroups} == {
             'FM1-grib-N8-V1-C8-S0-base-latest','FM1-hdf5-N8-V1-C8-S0-base-latest'}, 'qualification groups differ')
        self.validate_rows(qual, bindings, qgroups, resource_id, candidate_sha, qualification=True)
        waves = [json.loads(s) for s in (work/'waves.jsonl').read_text().splitlines()]
        self.validate_waves(waves,rows,bindings,selected,resource_id,candidate_sha)
        qwaves = [json.loads(s) for s in (work/'qualification-waves.jsonl').read_text().splitlines()]
        self.validate_waves(qwaves,qual,bindings,qgroups,resource_id,candidate_sha,qualification=True)
        cells = []; effects = []
        for group in selected:
            gid = group['group_id']
            for load in LOADS:
                indexed = {}
                for config in CONFIGS:
                    blocks = []
                    for campaign in CAMPAIGNS:
                        rr = [r for r in rows if (r['group_id'],r['configuration'],r['campaign'],r['load'],r['phase']) ==
                              (gid,config,campaign,load,'measurement')]
                        ww = [w for w in waves if (w['group_id'],w['configuration'],w['campaign'],w['load'],w['phase']) ==
                              (gid,config,campaign,load,'measurement')]
                        need(len(rr) == MEASURED*(8 if load == 'burst8' else 1) and len(ww) == MEASURED, 'cell size differs')
                        lat = [r['total_ns']/1e6 for r in rr]
                        block = dict(campaign=campaign, queries=len(rr), waves=len(ww), latency_ms=dict(
                            median=statistics.median(lat), mean=statistics.mean(lat), minimum=min(lat), maximum=max(lat),
                            descriptive_p95=percentile(lat,.95)),
                            discovery_median_ms=statistics.median(r['discovery_ns']/1e6 for r in rr),
                            value_median_ms=statistics.median(r['value_ns']/1e6 for r in rr),
                            throughput_qps=sum(w['query_count'] for w in ww)*1e9/sum(w['makespan_ns'] for w in ww),
                            execution_peak=max((w['intervals'].get('execution',{}).get('max_parallel',0) for w in ww), default=0),
                            execution_mean=sum(w['intervals'].get('execution',{}).get('integral_active_ns',0) for w in ww)/sum(w['makespan_ns'] for w in ww),
                            value_execution_peak=max((w['intervals'].get('operations',{}).get(
                                'typed_batch' if group['format']=='hdf5' else 'typed',{}).get('max_parallel',0) for w in ww),default=0),
                            discovery_execution_peak=max((w['intervals'].get('operations',{}).get(
                                'dataset_catalog' if group['format']=='hdf5' else 'format_catalog',{}).get('max_parallel',0) for w in ww),default=0),
                            queue_median_ms=statistics.median([p['counters'].get('query_tasks.queue_nanoseconds',0)/1e6
                                for w in ww for p in w['profiles']]) if load != 'FS-index' else None)
                        blocks.append(block)
                    cell = dict(group_id=gid, configuration=config, scheduler_workers=WORKERS[config], load=load,
                        blocks=blocks, throughput_qps=statistics.median(b['throughput_qps'] for b in blocks),
                        latency_median_ms=statistics.median(b['latency_ms']['median'] for b in blocks),
                        latency_p95_ms=statistics.median(b['latency_ms']['descriptive_p95'] for b in blocks),
                        execution_peak=max(b['execution_peak'] for b in blocks))
                    cells.append(cell); indexed[config] = cell
                for config in CONFIGS[1:]:
                    ratios = [dict(campaign=c['campaign'],throughput_ratio=c['throughput_qps']/b['throughput_qps'],
                        latency_ratio=c['latency_ms']['median']/b['latency_ms']['median'])
                        for c,b in zip(indexed[config]['blocks'],indexed['S1']['blocks'])]
                    effects.append(dict(group_id=gid,load=load,configuration=config,reference='S1',blocks=ratios,
                        throughput_ratio=statistics.median(r['throughput_ratio'] for r in ratios),
                        latency_ratio=statistics.median(r['latency_ratio'] for r in ratios)))
        return dict(protocol=PROTOCOL,complete=True,query_rows=len(rows),wave_rows=len(waves),qualification_rows=len(qual),
            qualification_resource_coverage=run.validate_coverage(work,qual),cells=cells,effects=effects,
            statistic='All fixed warm samples. Cell throughput=sum successful queries/sum measured wave makespans. Per-campaign latency medians and descriptive p95, then median across campaigns. Effects paired within campaign.',
            limitation='Two CPU total quota; execution overlap is scheduler tasks, not CPU cores or SQL parallelism. Burst8 measures query interleaving, not within-query splitting. Warm process/OS; not cold disk; no tail-latency guarantee.')

    def worker(self, args):
        def stop(sig, frame):
            raise RuntimeError('query task worker interrupted; preserve evidence')
        for sig in (signal.SIGTERM,signal.SIGHUP): signal.signal(sig,stop)
        proof = self.source_proof(args); frozen = read_json(args.work/'prepared.json')
        need(proof == read_json(args.work/'reuse-source.json') and self.closure(args.repo) == frozen['sources'], 'frozen preflight differs')
        paired, _, env, startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL,complete=False,tier=args.tier,query_records=0,qualification_records=0,
            expected_queries=self.expected_rows(args),corpora=[],product_sha256=file_sha(args.binary),
            resource_id=frozen['resource_id'],scientific_publication=False,index_rebuilt=False,new_cases=False,
            location_bindings_created=False,credentials_recorded=False)
        atomic_json(args.work/'report.json',report)
        try:
            registry = Registry(args.work/'registry')
            execution,bindings = self.plans(args,registry,report,proof)
            atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),loads=list(LOADS),
                formal_orders=[list(order) for order in ORDERS],load_order='rotate by repeat+campaign+format ordinal+configuration index',
                qualification_first=True,qualification_queries=60,formal_queries=2160))
            q = self.collect(args,execution,bindings,registry,frozen,report,env,paired,qualification=True)
            need(q == 60, 'N8 qualification count differs')
            self.validate_rows([json.loads(s) for s in (args.work/'qualification.jsonl').read_text().splitlines()],
                bindings,selected_groups(args.repo,8),frozen['resource_id'],frozen['product']['sha256'],qualification=True)
            atomic_json(args.work/'qualification-report.json',dict(complete=True,query_records=q,groups=selected_groups(args.repo,8),
                queries_sha256=file_sha(args.work/'qualification.jsonl'),performance_statistics=False))
            report['qualification_complete'] = True
            count = self.collect(args,execution,bindings,registry,frozen,report,env,paired)
            need(count == self.expected_rows(args), 'formal scheduler query count differs')
            self.collect_auxiliary(args,proof,report)
            need(self.source_proof(args) == proof and self.closure(args.repo) == frozen['sources'], 'final frozen inputs differ')
            for plan in execution:
                plan['item'].update(complete=True,final_raw_validation=run.verify_raw(plan['manifest'],plan['root']/'input',plan['root']/'fs'))
            report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),
                waves_sha256=file_sha(args.work/'waves.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
        except BaseException as error:
            report['failure'] = dict(type=type(error).__name__,message=str(error)[:500]); raise
        finally: atomic_json(args.work/'report.json',report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in PATH_ARGS: parser.add_argument('--'+key.replace('_','-'),type=Path,required=True)
    parser.add_argument('--tier',choices=('n128',),default='n128')
    parser.add_argument('--gate-report',type=Path)
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument('--worker',action='store_true');modes.add_argument('--prepare-only',action='store_true')
    modes.add_argument('--verify-prepared',action='store_true')
    args=parser.parse_args()
    for key in PATH_ARGS: setattr(args,key,getattr(args,key).resolve(strict=key != 'work'))
    for source in (args.source_collection,args.current_collection):
        need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work),'run/source overlap')
    binding=read_json(args.build_binding)
    spec=ProtocolSpec(PROTOCOL,'experiments.fixed_matrix.query_task_scheduler',__doc__,binding['candidate_sha256'],
        frozenset({'src/query/core/QueryTaskScheduler.cpp'}),
        (('SocketFixture','ExternalNativeHdf5MutexLeavesSingleWorkerAvailableForArrow'),
         ('OwnerQueryAdmission','PreparedPublicationRacePreservesOtherUnopenedRoute')),
        ('experiments/fixed_matrix/query_task_scheduler.py','experiments/fixed_matrix/query_task_client.py',
         WORK+'/protocol-binding.json',WORK+'/experiment-plan.json',WORK+'/feishu-4.3-source.json',WORK+'/run_stage.py',
         WORK+'/auxiliary-binding.json',WORK+'/mixed-transfer/query_task_mixed_probe.cpp',
         WORK+'/mixed-transfer/build_query_task_mixed_probe.py',WORK+'/mixed-transfer/README.md'))
    runner=QueryTaskRunner(spec)
    if args.worker: runner.worker(args)
    else:
        proof=runner.source_proof(args)
        if args.prepare_only: runner.prepare_only(args,proof)
        elif args.verify_prepared: runner.verify_prepared(args,proof)
        else: runner.supervise(args,proof)


if __name__ == '__main__': main()
