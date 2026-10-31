"""Four sealed N8/N128 cases: original serial versus current four-query load.

Two reverse-order campaigns; fresh mounts, one warm and three measured waves.
Every wave completes four queries, with owned-array clocks and wave-only audit.
The original product uses FUSE IPC; the current product uses raw result-fetch.
This is an accumulated-configuration comparison, not an isolated pool ablation.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import statistics

from . import control_plane_client, query_task_client, run, telemetry
from .grib_catalog import require_json_roundtrip
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, PATH_ARGS
from .owner_reader_pool import OwnerReaderPoolRunner, selected_groups
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-n8-n128-fourway/1'
WORK = 'experiments/cross_version/work/section13-n8-n128-4way-20261025'
CONFIGS = ('OLD', 'NEW')
CAMPAIGNS = (0, 1)
WARMUPS, MEASURED, REPEATS, QUERY_COUNT = 1, 3, 4, 4
INFLIGHT = {'OLD': 1, 'NEW': 4}
need = run.need


def group_order(groups, campaign):
    need(campaign in CAMPAIGNS, 'unknown campaign')
    ordered = sorted(groups, key=lambda g: (g['N'], ('grib', 'hdf5').index(g['format'])))
    return ordered if campaign == 0 else ordered[::-1]


def configuration_order(campaign):
    need(campaign in CAMPAIGNS, 'unknown campaign')
    return CONFIGS if campaign == 0 else CONFIGS[::-1]


def expected_keys(groups):
    return {(g['group_id'], c, s, r, q) for g in groups for c in CAMPAIGNS
            for s in CONFIGS for r in range(REPEATS) for q in range(QUERY_COUNT)}


def percentile(values, fraction):
    values = sorted(values)
    need(values and 0 <= fraction <= 1, 'invalid percentile')
    position = (len(values)-1)*fraction
    lower = int(position); upper = min(lower+1, len(values)-1)
    return values[lower]+(values[upper]-values[lower])*(position-lower)


def wave_id(group, campaign, configuration, repeat):
    return digest(dict(group=group, campaign=campaign, configuration=configuration,
                       load='four-query-batch', repeat=repeat))


class FourWayRunner(OwnerReaderPoolRunner):
    def groups(self, repo, tier):
        need(tier == 'n128', 'inherited tier token n128 denotes the complete four-case run')
        return selected_groups(repo, 8)+selected_groups(repo, 128)

    def expected_rows(self, args):
        return len(expected_keys(self.groups(args.repo, args.tier)))

    def verify_candidate(self, args):
        path = args.repo/WORK/'verify_product.py'
        spec = importlib.util.spec_from_file_location('fourway_frozen_product_verify', path)
        need(spec is not None and spec.loader is not None, 'product verifier unavailable')
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        proof = module.verify(args)
        need(proof.get('complete') is True and proof['candidate_sha256'] == file_sha(args.binary)
             and proof['baseline_sha256'] == self.spec.baseline_sha == file_sha(args.baseline_binary),
             'product verifier returned incompatible identity')
        return proof

    def source_proof(self, args):
        proof = super().source_proof(args)
        # The accepted parent prepends N8 qualification cases; here N8 is formal.
        unique = {}
        for original in proof['cases']:
            entry = dict(original, qualification=False)
            gid = entry['group_id']
            need(gid not in unique or unique[gid] == entry, 'duplicated case proof differs')
            unique[gid] = entry
        groups = self.groups(args.repo, args.tier)
        need(set(unique) == {g['group_id'] for g in groups}, 'four-case proof differs')
        proof['cases'] = [unique[g['group_id']] for g in groups]
        proof.pop('pools', None)
        proof.update(protocol=PROTOCOL, configurations=list(CONFIGS), arms=['IoTDB-FS'],
            expected_queries=self.expected_rows(args), qualification_queries=0,
            campaigns=list(CAMPAIGNS), warmup_per_arm=WARMUPS, measured_per_arm=MEASURED,
            configuration_order=[list(configuration_order(c)) for c in CAMPAIGNS],
            group_order=[[g['group_id'] for g in group_order(groups,c)] for c in CAMPAIGNS],
            load_concurrency=INFLIGHT, queries_per_wave=QUERY_COUNT,
            query_task_workers={'OLD':None,'NEW':4}, reader_pool_size={'OLD':None,'NEW':4},
            sender_threads={'OLD':None,'NEW':1}, queue_capacity={'OLD':None,'NEW':64},
            client_executor='one asyncio event loop; no client worker/executor',
            clock='each query entry through final owned values/masks; four-query actual makespan',
            first_warm_wave_is_functional_gate=True, separate_qualification=False,
            diagnostic_only=True, server_tracing=False, client_tracing=False, fuse_tracing=False,
            transport={'OLD':'FUSE IPC with streams controls','NEW':'raw_socket result_fetch_v1'})
        return proof

    def launcher(self, path, binary, sha, namespace, journal, plan, configuration):
        need(configuration in CONFIGS, 'unknown configuration')
        receipt = (NativeABRunner.launcher(self,path,binary,sha,namespace,journal,plan)
            if plan['group']['format'] == 'hdf5' else
            run.infrastructure.launcher(path,binary,sha,namespace,journal))
        declaration = 'SETTINGS='+repr(receipt['settings'])+'\n'
        source = path.read_text(); need(source.count(declaration) == 1, 'launcher settings differ')
        settings = dict(receipt['settings'])
        if configuration == 'NEW':
            settings.update({'storage.version_directory.reader_pool_size':'4',
                'query.tasks.worker_threads':'4','query.tasks.max_queued_tasks':'64',
                'query.tasks.max_batch_size':'4','query.result_fetch.sender_threads':'1'})
        else:
            need(not any(k.startswith(('query.tasks.','query.result_fetch.')) or
                         k == 'storage.version_directory.reader_pool_size' for k in settings),
                 'old product must retain its actual original configuration')
        source = source.replace(declaration,'SETTINGS='+repr(settings)+'\n',1)
        marker = 'os.execv(BINARY,[BINARY,*sys.argv[1:]])'
        need(source.count(marker) == 1, 'launcher exec differs')
        clear = ''.join('os.environ.pop('+repr(k)+',None)\n' for k in (
            'IOTDBFS_TAIL_TRACE_FILE','IOTDBFS_TAIL_TRACE_CAPACITY',
            'IOTDBFS_RESULT_FETCH_TRACE_JSONL','IOTDBFS_DIAG_TIMELINE','IOTDBFS_DIAGNOSTIC_TIMELINE_FILE',
            'IOTDBFS_DIAGNOSTIC_TIMELINE_ENABLED'))
        path.write_text(source.replace(marker,clear+marker,1))
        receipt.update(settings=settings,sha256=file_sha(path),configuration=configuration,tracing=False)
        return receipt

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        paths = read_json(args.work/'resource-paths.json'); count = 0
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        by_group = {p['group']['group_id']:p for p in execution}
        for campaign in CAMPAIGNS:
            for group in group_order(self.groups(args.repo,args.tier),campaign):
                gid = group['group_id']; plan = by_group[gid]; bound = bindings[gid]
                for config in configuration_order(campaign):
                    block = plan['execution_root']/('campaign-'+str(campaign))/config
                    block.mkdir(parents=True); journal = block/'owner'
                    shutil.copytree(plan['journal'],journal)
                    need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'],'journal copy differs')
                    binary = args.baseline_binary if config == 'OLD' else args.binary; sha = file_sha(binary)
                    launch = self.launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal,plan,config)
                    atomic_json(block/'launcher.receipt.json',launch)
                    before,_ = run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                    need(before == plan['scientific_head'],'head changed before mount')
                    print(json.dumps(dict(event='fourway_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
                    with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context:
                        need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha,'running product differs')
                        actual = read_json(context['root']/'profile.launch.json')
                        need(actual['settings'] == launch['settings'],'actual settings differ')
                        environment = telemetry.environment(args.repo,artifact_paths=[args.library,binary],
                            process_roles=dict(client=os.getpid(),daemon=context['pid'],
                                datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                        atomic_json(block/'environment.json',environment)
                        mount = dict(group_id=gid,campaign=campaign,configuration=config,qualification=False,
                            diagnostic=False,daemon_pid=context['pid'],
                            startup_ns=context['startup_ns'],product_sha256=sha,settings=launch['settings'],
                            environment_sha256=file_sha(block/'environment.json'),inflight=INFLIGHT[config])
                        plan['item']['mounts'].append(mount)
                        model = build_run(registry.get_case(bound['case_id']),execution=str(block),arm='IoTDB-FS',
                            implementation=dict(binary_sha256=sha,runner_sha256=digest(frozen['sources']),
                                engine='native-hdf5 cache+batch' if group['format']=='hdf5' else 'format_catalog + two sequential typed'),
                            clock=dict(kind='client-owned-full-query',d='query entry through discovery',q='discovery through final owned arrays',
                                throughput='four queries / actual first-entry to final-owned makespan'),
                            cache=dict(protocol=PROTOCOL,configuration=config,daemon_settings=launch['settings']),
                            environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=WARMUPS,measured=MEASURED,inflight=INFLIGHT[config],
                                query_count=QUERY_COUNT,schedule='reverse group and configuration order'))
                        registered = registry.register_run(model)
                        bound['run_ids'].setdefault(str(campaign),{})[config] = registered['run_id']
                        atomic_json(args.work/'bindings.json',bindings)
                        for repeat in range(REPEATS):
                            rb = telemetry.sample(paths); run.validate_sample(rb)
                            metadata = dict(group_id=gid,corpus_id=bound['corpus_id'],case_id=bound['case_id'],
                                oracle_sha256=bound['oracle_sha256'],run_id=registered['run_id'],campaign=campaign,
                                configuration=config,scheduler_workers=4 if config=='NEW' else None,
                                reader_pool_size=4 if config=='NEW' else None,sender_threads=1 if config=='NEW' else None,
                                diagnostic=False,client_backend='raw_socket' if config=='NEW' else 'streams',
                                load='four-query-batch',arm='IoTDB-FS',repeat=repeat,
                                phase='warmup' if repeat < WARMUPS else 'measurement',
                                resource_id=frozen['resource_id'],environment_sha256=mount['environment_sha256'],product_sha256=sha)
                            try:
                                context['control_plane_transport'] = 'raw_socket'
                                transport = control_plane_client if config == 'NEW' else query_task_client
                                wave = transport.run_wave(context,group,bound['scope'],bound['physical_versions'],
                                    bound['expected_matches'],inflight=INFLIGHT[config],query_count=QUERY_COUNT)
                            except query_task_client.WaveError as error:
                                atomic_json(block/('failed-wave-'+str(repeat)+'.json'),error.result); raise
                            wave.setdefault('transport','fuse_ipc')
                            wave.setdefault('client_socket_api','streams')
                            for row in wave['rows']: row.setdefault('transport','fuse_ipc')
                            try:
                                ra = telemetry.sample(paths); run.validate_sample(ra)
                                common = dict(metadata,resource_before=rb,resource_after=ra,ok=True,
                                    wave_id=wave_id(gid,campaign,config,repeat))
                                members = [dict(row,**common) for row in wave['rows']]
                                record = dict(common,**{k:v for k,v in wave.items() if k != 'rows'})
                                # Each warm wave gates the same mount before its measured samples.
                                self.validate_wave(record,members,bound,group,frozen['resource_id'],file_sha(args.binary))
                                run.append(args.work/'waves.jsonl',record)
                                for row in members: run.append(args.work/'queries.jsonl',row); count += 1
                            except BaseException as error:
                                atomic_json(block/('failed-wave-'+str(repeat)+'.json'),dict(
                                    metadata=metadata,resource_before=rb,returned_wave=wave,
                                    failure=dict(type=type(error).__name__,message=str(error)[:500])))
                                raise
                            mount['functional_gate_complete'] = True
                            report['query_records'] = count; atomic_json(args.work/'report.json',report)
                    after,_ = run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                    need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'],
                         'queries changed scientific head/journal')
                    need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'],'sealed index changed')
                    mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
                    atomic_json(args.work/'report.json',report)
        return count

    def validate_query(self,row,bound,group,resource_id,candidate_sha):
        config = row['configuration']; need(config in CONFIGS,'unknown query configuration')
        need(all(row[k] == bound[k] for k in ('corpus_id','case_id','oracle_sha256')) and
             row['run_id'] == bound['run_ids'][str(row['campaign'])][config], 'query identity differs')
        need(row['resource_id'] == resource_id and row['product_sha256'] ==
             (self.spec.baseline_sha if config=='OLD' else candidate_sha), 'query product/resource differs')
        need(row['scheduler_workers'] == (4 if config=='NEW' else None) and
             row['reader_pool_size'] == (4 if config=='NEW' else None) and
             row['sender_threads'] == (1 if config=='NEW' else None) and
             row['client_backend'] == ('raw_socket' if config=='NEW' else 'streams') and
             row['transport'] == ('result_fetch_v1' if config=='NEW' else 'fuse_ipc') and
             row['load'] == 'four-query-batch' and row['arm'] == 'IoTDB-FS' and row['diagnostic'] is False,
             'query configuration differs')
        need(all(row.get(k) is True for k in ('ok','oracle_verified','mode_verified')) and
             row['owned_output_bytes'] == group['owned_output_bytes'] and row['selected_units'] == group['K_units'],
             'owned output verification differs')
        need(row['execution_mode'] == ('native-hdf5' if group['format']=='hdf5' else 'iotdb-indexed'),
             'actual execution mode differs')
        need(all(type(row[k]) is int and row[k] > 0 for k in ('total_ns','discovery_ns','value_ns')) and
             all(type(row[k]) is int for k in ('t0_ns','t1_ns','t2_ns')) and
             row['t0_ns'] < row['t1_ns'] < row['t2_ns'] and
             row['total_ns'] == row['t2_ns']-row['t0_ns'] == row['discovery_ns']+row['value_ns'] and
             row['discovery_ns'] == row['t1_ns']-row['t0_ns'] and row['value_ns'] == row['t2_ns']-row['t1_ns'],
             'full owned clock differs')
        need(row['phase'] == ('warmup' if row['repeat'] < WARMUPS else 'measurement'),'query phase differs')
        calls = row['value_calls']
        need(len(calls) == (1 if group['format']=='hdf5' else 2) and
             row['batch_enabled'] is (group['format']=='hdf5') and
             row['discovery_protocol'] == ('dataset_catalog' if group['format']=='hdf5' else 'format_catalog'),
             'original query shape differs')
        if group['format']=='hdf5': need(calls[0]['batch_items'] == 2,'batch item count differs')
        if config == 'NEW':
            previous = row['t1_ns']
            for call in calls:
                fetches = call['fetches'] if group['format']=='hdf5' else [call]
                need(len(fetches) == (2 if group['format']=='hdf5' else 1),'fetch count differs')
                for fetch in fetches:
                    keys = ('fetch_start_ns','fetch_end_ns','decode_start_ns','decode_end_ns',
                            'append_start_ns','owned_ready_ns','fetch_ns','decode_owned_ns','append_owned_ns')
                    need(all(type(fetch.get(k)) is int for k in keys),'missing integer fetch clock')
                    a,b,c,d,e,f = (fetch[k] for k in keys[:6])
                    need(previous <= a <= b == c <= d <= e <= f <= row['t2_ns'] and
                         fetch['fetch_ns'] == b-a and fetch['append_owned_ns'] == f-e and
                         fetch['decode_owned_ns'] == d-c+f-e,'fetch owned clock differs')
                    previous = f
                if group['format']=='hdf5':
                    need(call['fetch_ns'] == sum(f['fetch_ns'] for f in fetches) and
                         call['decode_owned_ns'] == sum(f['decode_owned_ns'] for f in fetches), 'batch fetch sum differs')

    def validate_wave(self,wave,members,bound,group,resource_id,candidate_sha):
        config = wave['configuration']; need(config in CONFIGS,'unknown wave configuration')
        need(wave.get('complete') is True and wave.get('ok') is True and not wave.get('errors') and
             wave['query_count'] == len(members) == QUERY_COUNT and
             wave['inflight'] == wave['client_peak_inflight'] == INFLIGHT[config] and
             {r['query_index'] for r in members} == set(range(QUERY_COUNT)),'wave matrix/completion differs')
        wid = wave_id(group['group_id'],wave['campaign'],config,wave['repeat'])
        need(wave['wave_id'] == wid and all(r['wave_id'] == wid for r in members),'wave identity differs')
        for member in members:
            self.validate_query(member,bound,group,resource_id,candidate_sha)
            need(all(wave[k] == member[k] for k in ('group_id','corpus_id','case_id','oracle_sha256','run_id',
                'campaign','repeat','configuration','client_backend','scheduler_workers','reader_pool_size',
                'sender_threads','resource_id','product_sha256','phase','resource_before','resource_after',
                'environment_sha256','load','arm','diagnostic')),'wave/query provenance differs')
        start = min(r['t0_ns'] for r in members); end = max(r['t2_ns'] for r in members)
        need(wave['start_ns'] == start and wave['end_ns'] == end and
             type(wave['makespan_ns']) is int and wave['makespan_ns'] == end-start > 0 and
             abs(wave['throughput_qps']-QUERY_COUNT*1e9/(end-start)) < 1e-6,'actual four-query makespan differs')
        need(wave['transport'] == ('result_fetch_v1' if config=='NEW' else 'fuse_ipc') and
             wave['client_socket_api'] == ('raw_socket' if config=='NEW' else 'streams'),'actual transport differs')
        if config == 'OLD':
            ordered = sorted(members,key=lambda r:r['query_index'])
            need(all(a['t2_ns'] <= b['t0_ns'] for a,b in zip(ordered,ordered[1:])),'old baseline is not sequential')
        for side in ('resource_before','resource_after'): run.validate_sample(wave[side])
        need(wave['resource_before']['monotonic_ns'] <= start <= end <= wave['resource_after']['monotonic_ns'],
             'resources do not enclose complete query batch')
        profiles = wave['profiles']; fmt = group['format']
        discovery = 'format_catalog' if fmt=='grib' else 'dataset_catalog'
        value = 'typed' if fmt=='grib' else 'typed_batch'
        count = 12 if fmt=='grib' else 8
        need(len(profiles) == len({p['sequence'] for p in profiles}) == count and
             sum(p['operation']==discovery for p in profiles)==4 and
             sum(p['operation']==value for p in profiles)==(8 if fmt=='grib' else 4),'profile operation matrix differs')
        by_route = {}; catalogs = []
        for profile in profiles:
            need(profile.get('ok') is True and start <= profile['steady_start_ns'] <= profile['steady_end_ns'] <= end and
                 profile['steady_end_ns']-profile['steady_start_ns'] == profile['server_total_ns'],'profile clock differs')
            counters = profile.get('counters',{})
            need(profile.get('server_training_execution',{}).get('read_parallelism') == 1,
                 'within-query read parallelism changed')
            if config == 'NEW':
                cfg = profile['runtime_config']
                need(cfg['query_task_workers']==4 and cfg['query_task_max_queued']==64 and
                     cfg['query_task_max_batch']==4 and cfg['result_sender_threads']==1,'current runtime config differs')
                need(counters.get('control_json.parse_attempts')==1 and counters.get('control_json.reuses')==1 and
                     counters.get('control_json.worker_retry_parse_attempts')==0 and
                     counters.get('control_json.batch_parse_attempts',0)==(2 if profile['operation']=='typed_batch' else 0) and
                     type(counters.get('control_json.transport_parse_ns')) is int and
                     counters['control_json.transport_parse_ns'] >= 0,'parse-once and batch recursion evidence differs')
            else:
                need(not any(k.startswith('control_json.') for k in counters),'old product reports new parse accounting')
            if profile['operation']==discovery:
                catalogs.append(profile)
                if fmt=='hdf5': need(profile.get('server_hdf5_execution',{}).get('mode')=='native-hdf5','catalog mode differs')
            else:
                routes = [profile['path']] if fmt=='grib' else [i['path'] for i in profile['items']]
                if fmt=='hdf5':
                    query_task_client._validate_native_profile(profile)
                    mode = profile['server_hdf5_execution']
                    need(mode.get('native_cache_enabled') is True and all(mode.get(k)==v for k,v in {
                        'read_parallelism':1,'native_cache_files':8,'native_cache_datasets':8,
                        'native_metadata_bytes_per_file':2097152,'native_raw_chunk_bytes_per_dataset':262144,
                        'native_pool_limit_bytes':67108864}.items()), 'native cache configuration differs')
                    need(counters.get('typed_batch_items')==2 and counters.get('registration_attempts')==3,
                         'native batch registration shape differs')
                for route in routes:
                    need(route not in by_route,'duplicate result profile route'); by_route[route] = profile
        need(wave['catalog_profiles'] == catalogs,'anonymous catalog profiles differ')
        used = set()
        for member in members:
            for call in member['value_calls']:
                routes = call['response_paths'] if fmt=='hdf5' else [call['response_path']]
                need(len(routes)==(2 if fmt=='hdf5' else 1) and all(r in by_route and r not in used for r in routes),
                     'missing/shared result routes')
                profile = by_route[routes[0]]
                need(all(by_route[r] is profile for r in routes) and call['server_profile']==profile,'result profile binding differs')
                if fmt=='hdf5': need([i['path'] for i in profile['items']]==routes,'batch route order differs')
                used.update(routes)
        need(used == set(by_route),'unmatched value profile')
        intervals = query_task_client.analyze_intervals(profiles,(start,end))
        need(wave['intervals']==intervals and intervals['execution']['max_parallel'] <= (4 if config=='NEW' else 1),
             'registration overlap differs')

    def validate_rows(self,rows,bindings,selected,resource_id,candidate_sha):
        keys = [(r['group_id'],r['campaign'],r['configuration'],r['repeat'],r['query_index']) for r in rows]
        need(len(keys)==len(expected_keys(selected)) and set(keys)==expected_keys(selected),'exact unique query matrix required')
        groups = {g['group_id']:g for g in selected}
        for row in rows: self.validate_query(row,bindings[row['group_id']],groups[row['group_id']],resource_id,candidate_sha)

    def validate_waves(self,waves,rows,bindings,selected,resource_id,candidate_sha):
        wanted = {k[:-1] for k in expected_keys(selected)}
        key = lambda r:(r['group_id'],r['campaign'],r['configuration'],r['repeat'])
        need(len(waves)==len(wanted) and {key(w) for w in waves}==wanted,'exact unique wave matrix required')
        groups = {g['group_id']:g for g in selected}; grouped = {}
        for row in rows: grouped.setdefault(key(row),[]).append(row)
        for wave in waves:
            self.validate_wave(wave,grouped[key(wave)],bindings[wave['group_id']],groups[wave['group_id']],resource_id,candidate_sha)
        expected = [(g['group_id'],c,s,r) for c in CAMPAIGNS for g in group_order(selected,c)
                    for s in configuration_order(c) for r in range(REPEATS)]
        need([key(w) for w in waves]==expected,'physical group/configuration order differs')
        expected_rows = [(*k,q) for k in expected for q in range(QUERY_COUNT)]
        need([(r['group_id'],r['campaign'],r['configuration'],r['repeat'],r['query_index']) for r in rows] == expected_rows,
             'physical query order differs')

    def analyze(self,path,bindings,selected,resource_id,candidate_sha):
        path = Path(path); work = path.parent
        load = lambda p:[json.loads(s) for s in p.read_text().splitlines()]
        rows = load(path); waves = load(work/'waves.jsonl')
        self.validate_rows(rows,bindings,selected,resource_id,candidate_sha)
        self.validate_waves(waves,rows,bindings,selected,resource_id,candidate_sha)
        cells = []; effects = []
        for group in selected:
            indexed = {}
            for config in CONFIGS:
                blocks = []
                for campaign in CAMPAIGNS:
                    rr = [r for r in rows if (r['group_id'],r['configuration'],r['campaign'],r['phase']) ==
                          (group['group_id'],config,campaign,'measurement')]
                    ww = [w for w in waves if (w['group_id'],w['configuration'],w['campaign'],w['phase']) ==
                          (group['group_id'],config,campaign,'measurement')]
                    need(len(rr)==12 and len(ww)==3,'measured block size differs')
                    lat = [r['total_ns']/1e6 for r in rr]; span = sum(w['makespan_ns'] for w in ww)
                    blocks.append(dict(campaign=campaign,queries=len(rr),waves=len(ww),makespan_sum_ns=span,
                        throughput_qps=len(rr)*1e9/span,mean_makespan_ms=span/len(ww)/1e6,
                        latency_ms=dict(mean=statistics.mean(lat),median=statistics.median(lat),
                            descriptive_p95=percentile(lat,.95),minimum=min(lat),maximum=max(lat)),
                        discovery_mean_ms=statistics.mean(r['discovery_ns']/1e6 for r in rr),
                        value_mean_ms=statistics.mean(r['value_ns']/1e6 for r in rr),
                        execution_peak=max(w['intervals']['execution']['max_parallel'] for w in ww),
                        execution_mean=sum(w['intervals']['execution']['integral_active_ns'] for w in ww)/span))
                rr = [r for r in rows if (r['group_id'],r['configuration'],r['phase'])==(group['group_id'],config,'measurement')]
                total_span = sum(b['makespan_sum_ns'] for b in blocks)
                lat = [r['total_ns']/1e6 for r in rr]
                cell = dict(group_id=group['group_id'],configuration=config,format=group['format'],N=group['N'],
                    corpus_id=bindings[group['group_id']]['corpus_id'],case_id=bindings[group['group_id']]['case_id'],
                    inflight=INFLIGHT[config],blocks=blocks,queries=len(rr),waves=6,
                    pooled_qps=len(rr)*1e9/total_span,pooled_mean_latency_ms=statistics.mean(lat),
                    pooled_median_latency_ms=statistics.median(lat),pooled_descriptive_p95_ms=percentile(lat,.95),
                    pooled_mean_makespan_ms=total_span/6/1e6,
                    execution_peak=max(b['execution_peak'] for b in blocks))
                cells.append(cell); indexed[config]=cell
            old,new = indexed['OLD'],indexed['NEW']
            paired = [dict(campaign=o['campaign'],throughput_ratio=n['throughput_qps']/o['throughput_qps'],
                mean_latency_ratio=n['latency_ms']['mean']/o['latency_ms']['mean'],
                mean_makespan_ratio=n['mean_makespan_ms']/o['mean_makespan_ms'])
                for o,n in zip(old['blocks'],new['blocks'])]
            effects.append(dict(group_id=group['group_id'],reference='OLD',candidate='NEW',blocks=paired,
                pooled_throughput_ratio=new['pooled_qps']/old['pooled_qps'],
                pooled_mean_latency_ratio=new['pooled_mean_latency_ms']/old['pooled_mean_latency_ms'],
                pooled_mean_makespan_ratio=new['pooled_mean_makespan_ms']/old['pooled_mean_makespan_ms']))
        return dict(protocol=PROTOCOL,complete=True,query_rows=len(rows),wave_rows=len(waves),
            measured_queries=sum(r['phase']=='measurement' for r in rows),qualification_rows=0,
            cells=cells,effects=effects,
            statistic='QPS=sum measured query count/sum actual four-query spans; pooled latency and paired campaign effects; every fixed sample retained.',
            limitation='Two-campaign descriptive warm accumulated-configuration probe; differing product, delivery and offered concurrency. Profile overlap is not physical CPU parallelism or full slot occupancy; two-core budget. No isolated pool effect, significance, cold-cache or tail guarantee.')

    def worker(self,args):
        def stop(sig,frame): raise RuntimeError('fourway worker cancelled; preserve evidence')
        for sig in (signal.SIGTERM,signal.SIGHUP): signal.signal(sig,stop)
        proof = self.source_proof(args); frozen = read_json(args.work/'prepared.json')
        need(proof==read_json(args.work/'reuse-source.json') and self.closure(args.repo)==frozen['sources'],'frozen preflight differs')
        paired,_,env,startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL,complete=False,tier=args.tier,query_records=0,qualification_records=0,
            expected_queries=self.expected_rows(args),corpora=[],product_sha256=file_sha(args.binary),
            baseline_sha256=self.spec.baseline_sha,resource_id=frozen['resource_id'],scientific_publication=False,
            index_rebuilt=False,new_cases=False,location_bindings_created=False,credentials_recorded=False)
        atomic_json(args.work/'report.json',report)
        try:
            registry = Registry(args.work/'registry'); execution,bindings = self.plans(args,registry,report,proof)
            atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),
                mounts=[dict(group=g['group_id'],campaign=c,configuration=s)
                    for c in CAMPAIGNS for g in group_order(self.groups(args.repo,args.tier),c)
                    for s in configuration_order(c)],formal_queries=256,formal_waves=64,
                measured_queries=192,measured_waves=48,separate_qualification=False,
                first_warm_wave_is_functional_gate=True,fresh_mount=True,native_helper_context_created=False))
            count = self.collect(args,execution,bindings,registry,frozen,report,env,paired)
            need(count==self.expected_rows(args),'formal row count differs')
            need(self.source_proof(args)==proof and self.closure(args.repo)==frozen['sources'],'frozen inputs changed')
            for plan in execution:
                plan['item'].update(complete=True,final_raw_validation=run.verify_raw(plan['manifest'],plan['root']/'input',plan['root']/'fs'))
            report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),
                waves_sha256=file_sha(args.work/'waves.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
        except BaseException as error:
            report['failure']=dict(type=type(error).__name__,message=str(error)[:500]); raise
        finally: atomic_json(args.work/'report.json',report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in PATH_ARGS: parser.add_argument('--'+key.replace('_','-'),type=Path,required=True)
    parser.add_argument('--tier',choices=('n128',),default='n128'); parser.add_argument('--gate-report',type=Path)
    modes = parser.add_mutually_exclusive_group(); modes.add_argument('--worker',action='store_true')
    modes.add_argument('--prepare-only',action='store_true'); modes.add_argument('--verify-prepared',action='store_true')
    args = parser.parse_args()
    for key in PATH_ARGS: setattr(args,key,getattr(args,key).resolve(strict=key!='work'))
    for source in (args.source_collection,args.current_collection):
        need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work),'source/run paths overlap')
    binding = read_json(args.repo/WORK/'protocol-binding.json')
    need(args.build_binding == (args.repo/WORK/'protocol-binding.json').resolve(strict=True) and
         args.baseline_build_binding == args.build_binding,'one frozen protocol binding required')
    spec = ProtocolSpec(PROTOCOL,'experiments.fixed_matrix.n8_n128_fourway',__doc__,binding['baseline_sha256'],
        frozenset(binding['product_diff']),tuple(tuple(t) for t in binding['required_regression_tests']),
        ('experiments/fixed_matrix/n8_n128_fourway.py','experiments/fixed_matrix/control_plane_client.py',
         'experiments/fixed_matrix/query_task_client.py',*tuple(binding['extra_closure'])))
    runner = FourWayRunner(spec)
    if args.worker: runner.worker(args)
    else:
        proof = runner.source_proof(args); require_json_roundtrip(proof,'fourway proof')
        if args.prepare_only: runner.prepare_only(args,proof)
        elif args.verify_prepared: runner.verify_prepared(args,proof)
        else: runner.supervise(args,proof)


if __name__ == '__main__': main()
