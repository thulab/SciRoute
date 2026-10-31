"""Common GRIB/HDF5 candidate mounts: explicit versus prepared selector handle.

New sealed-case runs only. Preparation and release are separately timed and
profiled. Both modes use identical owned-value, scope and raw-oracle validation.
"""
from __future__ import annotations
from contextlib import ExitStack
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import time

from . import client, run, telemetry
from .hdf5_native_ab import ARMS, CAMPAIGNS, CONFIGS, MEASURED, REPEATS, WARMUPS, NativeABRunner, ProtocolSpec
from fs_index.native import NativeContext
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-cross-format-selector-pair/1'
WORK = 'experiments/cross_version/work/grib-selector-dq-scale-20261024'
MODE = {'A':'explicit', 'B':'handle'}
COUNTERS = {'selector.compiles':0, 'selector.input_path_items':0,
    'selector.handle_queries':1, 'hot_query.selector_short_ref_hits':1,
    'hot_query.selector_long_key_bytes':0}


def load_helper(repo):
    spec=importlib.util.spec_from_file_location('selector_handle_client',repo/WORK/'catalog_handle_client.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def protocol_spec(repo, fmt):
    """A new current-source baseline is bound before preparation; never old A."""
    receipt=read_json(repo/WORK/'protocol-binding.json')
    run.need(receipt.get('complete') is True and receipt.get('baseline_contains_stage_summary') is True,
             'current-source baseline and approved product change must be frozen first')
    return ProtocolSpec(protocol=PROTOCOL,module='experiments.fixed_matrix.'+fmt+'_selector_pair',
        description=__doc__,baseline_sha=receipt['baseline_sha256'],
        product_diff=frozenset(receipt['product_diff']),
        required_regression_tests=tuple(tuple(v) for v in receipt['required_regression_tests']),
        extra_closure=('experiments/fixed_matrix/tests/test_catalog_selector_pair.py',
            'experiments/fixed_matrix/grib_selector_pair.py','experiments/fixed_matrix/hdf5_selector_pair.py',
            WORK+'/catalog_handle_client.py',WORK+'/protocol-binding.json',WORK+'/experiment-plan.json',
            WORK+'/run_control.py',
            WORK+'/test_run_control.py',WORK+'/execution-paths.json',
            WORK+'/README.md',
            WORK+'/instrumentation/pid_cpu_sampler.py',WORK+'/instrumentation/service_gate.py',
            WORK+'/instrumentation/matched-load-plan.json',WORK+'/instrumentation/analyze_matched_load.py',
            WORK+'/instrumentation/analyze_pid_cpu.py',WORK+'/instrumentation/test_service_gate.py',
            WORK+'/instrumentation/test_service_gate_common.py',WORK+'/instrumentation/test_matched_load.py'))


def query_order(campaign, repeat):
    """Per-arm adjacent A/B, ten formal queries balanced five first per mode."""
    modes=CONFIGS if campaign in (0,3) else CONFIGS[::-1]
    arms=ARMS if (repeat+campaign)%2==0 else ARMS[::-1]
    return [(arm,mode) for arm in arms for mode in
        (modes if repeat%2==0 else modes[::-1])]


def clock_stamp():
    return dict(unix_ns=time.time_ns(),monotonic_ns=time.monotonic_ns())


def effects(rows):
    result=[]
    for campaign in CAMPAIGNS:
        modes={}
        for config in CONFIGS:
            arms={arm:{key:statistics.median(r[key]/1e6 for r in rows
                    if r['campaign']==campaign and r['configuration']==config and
                    r['arm']==arm and r['phase']=='measurement')
                for key in ('total_ns','discovery_ns','value_ns')} for arm in ARMS}
            modes[config]=dict(median_ms=arms,**{name:arms['IoTDB-FS'][key]/arms['FS-index'][key]
                for name,key in (('R','total_ns'),('Rd','discovery_ns'),('Rq','value_ns'))})
        result.append(dict(campaign=campaign,modes=modes,**{
            name+'_B_over_A':modes['B'][name]/modes['A'][name] for name in ('R','Rd','Rq')},
            raw_IoTDB_T_B_over_A=modes['B']['median_ms']['IoTDB-FS']['total_ns']/modes['A']['median_ms']['IoTDB-FS']['total_ns'],
            raw_IoTDB_d_B_over_A=modes['B']['median_ms']['IoTDB-FS']['discovery_ns']/modes['A']['median_ms']['IoTDB-FS']['discovery_ns'],
            raw_FS_T_B_over_A=modes['B']['median_ms']['FS-index']['total_ns']/modes['A']['median_ms']['FS-index']['total_ns']))
    return dict(blocks=result,**{key:statistics.median(b[key] for b in result)
        for key in result[0] if key not in ('campaign','modes')})


class CatalogSelectorPairRunner(NativeABRunner):
    def __init__(self,spec,fmt):
        super().__init__(spec)
        run.need(fmt in ('grib','hdf5'),'unknown paired format');self.format=fmt

    def groups(self,repo,tier):
        run.need(tier in ('n8','n128'),'only sealed small-N tiers allowed')
        n=int(tier[1:]);by={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
        return [by[f'FM1-{self.format}-N{n}-V1-C{n}-S0-base-latest']]

    def validate_n8_gate(self,gate,baseline_sha,candidate_sha):
        super().validate_n8_gate(gate,baseline_sha,candidate_sha)
        run.need(gate.get('format')==self.format,'N128 needs same-format new N8 gate')

    def plans(self,args,registry,report,proof):
        # The shared HDF runner's native_root is deliberately HDF-specific.
        original=read_json(args.source_collection/'report.json');oldbind=read_json(args.source_collection/'bindings.json')
        source_registry=Registry(args.source_collection/'registry');selected={g['group_id']:g for g in self.groups(args.repo,args.tier)}
        result=[];bindings={}
        for entry in proof['cases']:
            gid=entry['group_id'];cid=entry['corpus_id'];base=Path(entry['source_root'])
            corpus=registry.register_corpus(source_registry.get_corpus(cid));registry.register_case(source_registry.get_case(entry['case_id']))
            bindings[gid]=deepcopy(oldbind[gid]);bindings[gid]['run_ids']={}
            execution=args.work/'corpora'/entry['corpus_key'];execution.mkdir(parents=True,mode=0o700)
            item=deepcopy(next(v for v in original['corpora'] if v['corpus_id']==cid))
            item.update(complete=False,mounts=[],source_root=str(base),head=entry['head'],journal_digest=entry['journal_digest'])
            report['corpora'].append(item)
            result.append(dict(root=base,execution_root=execution,corpus=corpus,group=selected[gid],item=item,
                journal=Path(entry['journal']),journal_digest=entry['journal_digest'],scientific_head=entry['head'],
                native_root=Path(entry['native_root']) if self.format=='hdf5' else None,
                prep=read_json(base/'preparation.json'),manifest=read_json(base/'input/manifest.json'),
                cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                    cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)))
        return result,bindings

    def launcher(self,path,binary,sha,namespace,journal,plan):
        if self.format=='grib':return run.infrastructure.launcher(path,binary,sha,namespace,journal)
        return super().launcher(path,binary,sha,namespace,journal,plan)

    def source_proof(self,args):
        proof=super().source_proof(args)
        if args.tier=='n128':
            prior=args.gate_report.parent;gate=read_json(args.gate_report)
            run.need(read_json(prior/'prepared.json')['sources']==self.closure(args.repo),
                     'preceding same-format N8 acquisition closure changed')
            for name,key in (('queries.jsonl','queries_sha256'),('bindings.json','bindings_sha256'),
                             ('analysis.json','analysis_sha256'),('selector-lifecycle.jsonl','lifecycle_sha256')):
                run.need(file_sha(prior/name)==gate.get(key),'preceding gate artifact changed: '+name)
        proof.update(comparison='candidate explicit versus candidate selector handle',
            format=self.format,
            baseline_role='fresh current-source build correctness/source gate only',
            modes=MODE,mounts_per_group=4,shared_candidate_mount=True,
            native_contexts_per_mount=2,prepare_release_profiled=True,
            preparation_accounting='first query plus prepare; warm sums plus prepare; no slow-value exclusions')
        return proof

    def validate_row(self,row,group,configuration):
        if self.format=='hdf5':super().validate_row(row,group,configuration)
        else:
            run.need(group['format']=='grib' and configuration in CONFIGS and
                all(row.get(k) is True for k in ('oracle_verified','mode_verified')) and
                row.get('execution_mode')=='iotdb-indexed' and row.get('selected_units')==group['K_units'] and
                row.get('batch_enabled') is False and len(row.get('value_calls',[]))==group['K_units'],
                'GRIB full owned query or unchanged serial q differs')
            profile=row['discovery_call']['server_profile']
            run.need(profile.get('operation')==('format_catalog' if configuration=='A' else 'format_catalog_by_selector') and
                profile['counters'].get('registration_attempts')==1 and 'path' not in profile and
                all(p['server_profile'].get('operation')=='typed' and
                    p['server_profile']['counters'].get('registration_attempts')==1 for p in row['value_calls']),
                'GRIB discovery/serial value profile operations differ')
        if row.get('phase')=='measurement':
            counters=row['discovery_call']['server_profile']['counters']
            for call in [row['discovery_call']]+row['value_calls']:
                c=call['server_profile']['counters']
                run.need(all(type(c.get('request_metadata.'+key)) is int and c['request_metadata.'+key]>=0
                    for key in ('fee_entries_visited','fee_entries_skipped','summary_stages','memo_hits',
                                'overlay_entries_inspected','collector_exits','fallbacks','consumed_bytes')),
                    'actual complete request metadata ledger evidence missing')
            if configuration=='B':run.need(all(type(counters.get(k)) is int and counters[k]==v for k,v in COUNTERS.items()),
                'warm handle mechanism/fee counts differ')
            else:run.need(counters.get('selector.compiles')==1 and counters.get('selector.input_path_items')==group['C_paths'] and
                type(counters.get('hot_query.selector_long_key_bytes')) is int and counters['hot_query.selector_long_key_bytes']>0,
                'explicit warm selector mechanism differs')
            expected=2 if self.format=='hdf5' else 3
            run.need(counters.get('hot_query.proof_hits')==expected,'fresh principal proof hit count differs')
            admissions=[counters[k] for k in ('read_context.admissions','version_context.admissions') if k in counters]
            run.need(admissions and all(type(v) is int and v==expected for v in admissions),
                     'warm discovery actual fresh admissions differ')
            if self.format=='hdf5':
                wanted={'hot_query.fee_summary_stages':4,'hot_query.fee_replay_entries':0,
                    'hot_query.fee_summary_fallbacks':0,'hot_query.fee_summary_collector_exits':0}
                run.need(all(counters.get(k)==v for k,v in wanted.items()),'HDF summary mechanism differs')
            else:
                run.need(counters.get('hot_query.candidate_domain_hits')==1 and
                    counters.get('hot_query.adapter_domain_hits')==1,'GRIB candidate/adapter residency differs')
                run.need(counters.get('request_metadata.collector_exits')==0,
                         'GRIB ordinary ledger unexpectedly exited collection')
                for prefix in ('candidate','adapter'):
                    base='hot_query.'+prefix+'_fee_'
                    run.need(type(counters.get(base+'logical_entries')) is int and counters[base+'logical_entries']>0 and
                        counters.get(base+'entries_visited')==counters[base+'logical_entries'] and
                        all(counters.get(base+k)==0 for k in ('entries_skipped','summary_stages')),
                        'GRIB baseline fee replay evidence differs')
                    # The frozen adapter implementation emits this field only
                    # on its active-collector path. Normal absence remains
                    # missing, rather than becoming fabricated zero evidence.
                    run.need((base+'collector_untracked' not in counters if prefix=='adapter' else
                              counters.get(base+'collector_untracked')==0),
                             'GRIB source-bound collector evidence differs')
        return row

    def worker(self,args):
        def stop(sig,frame):raise RuntimeError('selector worker cancelled; preserve evidence')
        for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
        proof=self.source_proof(args);frozen=read_json(args.work/'prepared.json')
        run.need(proof==read_json(args.work/'reuse-source.json') and self.closure(args.repo)==frozen['sources'],
                 'preflight changed')
        paired,_,env,startup=run.dependencies(args.repo)
        startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
        report=dict(protocol=PROTOCOL,format=self.format,complete=False,tier=args.tier,query_records=0,
            expected_queries=self.expected_rows(args),corpora=[],product_sha256=file_sha(args.binary),
            baseline_sha256=self.spec.baseline_sha,resource_id=frozen['resource_id'],
            scientific_publication=False,index_rebuilt=False,new_cases=False,
            location_bindings_created=False,credentials_recorded=False,comparison='candidate explicit versus handle')
        atomic_json(args.work/'report.json',report)
        try:
            registry=Registry(args.work/'registry');execution,bindings=self.plans(args,registry,report,proof)
            atomic_json(args.work/'execution-order.json',dict(modes=MODE,arms=list(ARMS),
                mounts=[dict(group=g['group_id'],campaign=c,first_mode_order=list(self.configuration_order(c)))
                    for c in CAMPAIGNS for g in self.groups(args.repo,args.tier)],
                shared_candidate_mount=True,fresh_FS_context_per_mode_per_mount=True))
            count=self.collect(args,execution,bindings,registry,frozen,report,env,paired)
            run.need(count==self.expected_rows(args),'selector collection incomplete')
            run.need(self.source_proof(args)==proof and self.closure(args.repo)==frozen['sources'],
                     'source/binary/science changed')
            for plan in execution:
                plan['item'].update(complete=True,final_raw_validation=run.verify_raw(
                    plan['manifest'],plan['root']/'input',plan['root']/'fs'))
            report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),
                          bindings_sha256=file_sha(args.work/'bindings.json'),
                          lifecycle_sha256=file_sha(args.work/'selector-lifecycle.jsonl'))
        except BaseException as error:
            report['failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
        finally:atomic_json(args.work/'report.json',report)

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        helper=load_helper(args.repo);paths=read_json(args.work/'resource-paths.json');count=0
        sha=file_sha(args.binary)
        writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        for campaign in CAMPAIGNS:
            for plan in execution:
                group=plan['group'];gid=group['group_id'];binding=bindings[gid]
                block=plan['execution_root']/('campaign-'+str(campaign))/'paired';block.mkdir(parents=True)
                journal=block/'owner';shutil.copytree(plan['journal'],journal)
                run.need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'journal copy differs')
                launch=self.launcher(block/'launcher',args.binary,sha,plan['item']['namespace'],journal,plan)
                atomic_json(block/'launcher.receipt.json',launch)
                before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                run.need(before==plan['scientific_head'],'head changed before mount')
                print(json.dumps(dict(event='selector_pair_mount',campaign=campaign,group=gid)),flush=True)
                with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                    run.need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha,'running candidate differs')
                    actual=read_json(context['root']/'profile.launch.json')
                    run.need(actual['settings']==launch['settings'],'actual daemon settings differ')
                    natives={mode:stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                             for mode in self.configuration_order(campaign)}
                    environment=telemetry.environment(args.repo,artifact_paths=[args.library,args.binary],
                        process_roles=dict(client=os.getpid(),daemon=context['pid'],
                            datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                    atomic_json(block/'environment.json',environment)
                    observed_client=environment['processes']['client']
                    run.need(observed_client.get('pid')==os.getpid() and type(observed_client.get('start_ticks')) is int,
                             'actual worker identity missing')
                    client_identity={key:observed_client[key] for key in ('pid','start_ticks')}
                    mount=dict(campaign=campaign,configuration='paired',daemon_pid=context['pid'],
                        startup_ns=context['startup_ns'],product_sha256=sha,
                        environment_sha256=file_sha(block/'environment.json'),modes=MODE,client_identity=client_identity,
                        native_contexts={mode:dict(native_context_id=f'{os.getpid()}:{gid}:{campaign}:{mode}',
                            client_pid=os.getpid(),closed=False) for mode in CONFIGS})
                    plan['item']['mounts'].append(mount)
                    for mode in CONFIGS:
                        atomic_json(block/('query-plan-'+mode+'.json'),natives[mode].explain(
                            'index',binding['native_selection'],client.PREDICATE,binding['scope']))
                        for arm in ARMS:
                            model=build_run(registry.get_case(binding['case_id']),execution=str(block),arm=arm,
                                implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                    runner_sha256=digest(frozen['sources']),engine=self.format+' candidate '+MODE[mode]),
                                clock=dict(kind='client-owned-full-query',d='entry through discovery delivery',
                                    q='discovery through owned Float32/mask',profile_oracle='outside',
                                    preparation='separate actual control clock; first and amortized totals include it'),
                                cache=dict(protocol=PROTOCOL,configuration=mode,selector_mode=MODE[mode],
                                    native_helper=plan['cfg'] if arm=='FS-index' else None,
                                    daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=self.format=='hdf5'),
                                environment=dict(resource_id=frozen['resource_id'],
                                    process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign,warmup=WARMUPS,measured=MEASURED,
                                    schedule='shared mount; adjacent modes; AB/BA/BA/AB first-order; five formal first per mode'))
                            bound=registry.register_run(model)
                            binding['run_ids'].setdefault(str(campaign),{}).setdefault(mode,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    response,prepare=helper.lifecycle(context,helper.prepare_request(self.format,binding['scope'],binding['physical_versions']))
                    run.need(response.get('kind')=='prepared_catalog_selector' and
                        isinstance(response.get('selector_id'),str) and bool(response['selector_id']) and
                        type(response.get('idle_ttl_ms')) is int and response['idle_ttl_ms']>0,
                        'prepare response contract differs')
                    selector_id=response['selector_id'];prepare.update(group_id=gid,campaign=campaign,
                        selector_id=selector_id,profile_offset_before=0,**clock_stamp())
                    run.append(args.work/'selector-lifecycle.jsonl',prepare)
                    try:
                        for repeat in range(REPEATS):
                            for arm,mode in query_order(campaign,repeat):
                                rb=telemetry.sample(paths);run.validate_sample(rb)
                                profile_before=context.get('profile_offset',0)
                                call_begin=clock_stamp()
                                if arm=='IoTDB-FS':
                                    row=helper.query(context,group,binding['scope'],binding['physical_versions'],
                                        binding['expected_matches'],batch=self.format=='hdf5',selector_id=selector_id if mode=='B' else None)
                                else:row=client.query_native(natives[mode],arm,group,binding['scope'],
                                    binding['native_selection'],binding['expected_matches'])
                                call_end=clock_stamp();ra=telemetry.sample(paths);run.validate_sample(ra)
                                profile_after=context.get('profile_offset',0)
                                run.need((profile_after>profile_before if arm=='IoTDB-FS' else profile_after==profile_before),
                                         'profile cursor did not advance for exactly IoTDB calls')
                                row.update(group_id=gid,corpus_id=binding['corpus_id'],case_id=binding['case_id'],
                                    oracle_sha256=binding['oracle_sha256'],run_id=binding['run_ids'][str(campaign)][mode][arm],
                                    campaign=campaign,configuration=mode,selector_mode=MODE[mode],arm=arm,repeat=repeat,
                                    phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,resource_id=frozen['resource_id'],
                                    resource_before=rb,resource_after=ra,environment_sha256=mount['environment_sha256'],
                                    product_sha256=sha,client_call_begin=call_begin,client_call_end=call_end,
                                    native_context_id=mount['native_contexts'][mode]['native_context_id'],
                                    client_identity=client_identity,
                                    profile_offset_before=profile_before,profile_offset_after=profile_after)
                                if arm=='IoTDB-FS':self.validate_row(row,group,mode)
                                run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                            atomic_json(args.work/'report.json',report)
                    finally:
                        before_release=context.get('profile_offset',0)
                        response,release=helper.lifecycle(context,dict(kind='catalog_selector_release',selector_id=selector_id))
                        run.need(response.get('kind')=='released_catalog_selector' and response.get('released') is True,
                                 'release response contract differs')
                        release.update(group_id=gid,campaign=campaign,selector_id=selector_id,
                                       profile_offset_before=before_release,**clock_stamp())
                        run.append(args.work/'selector-lifecycle.jsonl',release)
                        mount.update(selector_released=True)
                after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                run.need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],
                         'query changed head/journal')
                run.need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed FS-index')
                mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
                for item in mount['native_contexts'].values():item['closed']=True
                atomic_json(args.work/'report.json',report)
        return count

    def analyze(self,path,bindings,selected,resource_id,candidate_sha):
        rows=[json.loads(line) for line in Path(path).read_text().splitlines()]
        wanted=[(g['group_id'],c,m,a,r) for c in CAMPAIGNS for g in selected
                for r in range(REPEATS) for a,m in query_order(c,r)]
        run.need([(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows]==wanted,
                 'exact complete preregistered order required; no exclusions')
        by={g['group_id']:g for g in selected}
        for row in rows:
            b=bindings[row['group_id']];g=by[row['group_id']];mode=row['configuration']
            run.need(row['resource_id']==resource_id and row['product_sha256']==candidate_sha,
                     'resource/candidate identity differs')
            run.need(all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
                row['run_id']==b['run_ids'][str(row['campaign'])][mode][row['arm']], 'science/run identity differs')
            run.need(all(row.get(k) is True for k in ('ok','oracle_verified','mode_verified')) and
                row['selected_units']==g['K_units'] and row['owned_output_bytes']==g['owned_output_bytes'],
                'owned output not independently verified')
            run.need(row['selector_mode']==MODE[mode] and
                row['phase']==('warmup' if row['repeat']<WARMUPS else 'measurement') and
                all(type(row[k]) is int and row[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
                row['total_ns']==row['discovery_ns']+row['value_ns'],'clock/mode/phase differs')
            for side in ('resource_before','resource_after'):run.validate_sample(row[side])
            run.need(row['resource_before']['monotonic_ns']<=row['client_call_begin']['monotonic_ns']<
                row['client_call_end']['monotonic_ns']<=row['resource_after']['monotonic_ns'] and
                row['client_call_end']['monotonic_ns']-row['client_call_begin']['monotonic_ns']>=row['total_ns'],
                'clock envelope differs')
            if row['arm']=='IoTDB-FS':self.validate_row(row,g,mode)
        lifecycle=[json.loads(line) for line in (Path(path).parent/'selector-lifecycle.jsonl').read_text().splitlines()]
        run.need(len(lifecycle)==len(selected)*8,'four prepare/release pairs required')
        first=[]
        for group in selected:
            for campaign in CAMPAIGNS:
                items=[v for v in lifecycle if v['group_id']==group['group_id'] and v['campaign']==campaign]
                run.need([v['kind'] for v in items]==['catalog_selector_prepare','catalog_selector_release'] and
                    items[0]['selector_id']==items[1]['selector_id'] and
                    all(type(v['total_ns']) is int and v['total_ns']>0 for v in items),'lifecycle differs')
                prepare=items[0]['total_ns'];subset=[r for r in rows if r['group_id']==group['group_id'] and
                    r['campaign']==campaign and r['arm']=='IoTDB-FS']
                initial={m:next(r for r in subset if r['configuration']==m)['total_ns'] for m in CONFIGS}
                first.append(dict(group_id=group['group_id'],campaign=campaign,prepare_ns=prepare,
                    release_ns=items[1]['total_ns'],first_explicit_ns=initial['A'],
                    first_handle_query_ns=initial['B'],first_handle_with_prepare_ns=prepare+initial['B'],
                    first_handle_full_lifecycle_ns=prepare+initial['B']+items[1]['total_ns'],
                    all15_explicit_ns=sum(r['total_ns'] for r in subset if r['configuration']=='A'),
                    all15_handle_with_prepare_ns=prepare+sum(r['total_ns'] for r in subset if r['configuration']=='B'),
                    all15_handle_full_lifecycle_ns=prepare+items[1]['total_ns']+
                        sum(r['total_ns'] for r in subset if r['configuration']=='B'),
                    amortization_note='Interleaved shared domain: first native execution may build domain for either mode; not independent cold comparison.'))
        return dict(protocol=PROTOCOL,complete=True,query_rows=len(rows),lifecycle_rows=len(lifecycle),
            groups=[dict(group_id=g['group_id'],**effects([r for r in rows if r['group_id']==g['group_id']])) for g in selected],
            preparation_and_first=first,statistic='All rows retained; per-block arm medians and paired B/A; means in independent audit.',
            limitation='Warm sequential reuse in shared candidate domain; first execution is order-dependent; no cold-disk/concurrency claim.')



