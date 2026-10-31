"""Public q stage A: ordinary/template requests on one current candidate mount.

Each mount owns a fresh client process and two fresh FS-index contexts. Original
clocks, all scientific rows and background exposure remain visible. Baseline f09
is a source/correctness gate, not a second timed product configuration.
"""
from __future__ import annotations
from contextlib import ExitStack,redirect_stdout
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sys

from . import client,run,telemetry
from .catalog_selector_pair import CatalogSelectorPairRunner,clock_stamp,effects,query_order
from .hdf5_native_ab import NativeABRunner,ProtocolSpec,ARMS,CONFIGS,CAMPAIGNS,REPEATS,WARMUPS
from fs_index.native import NativeContext
from experiments.identity.model import build_run,digest
from experiments.identity.registry import Registry,read_json,file_sha
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL='iotdbfs-fixed-public-q-template-stage-a/1'
WORK='experiments/cross_version/work/public-q-template-stage-a-20261024'
BASELINE_SHA='f09f3742246be3402aaf7dec764f4a7d7d357740a9fb916e30b3b2ca9bed5774'
MODE={'A':'ordinary','B':'template'}
NAMES=('grib8','hdf58','grib128','hdf5128')


def helper(repo,name):
    spec=importlib.util.spec_from_file_location('q_template_'+name,repo/WORK/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def group_order(tier,campaign):
    run.need(tier in ('n8','n128') and campaign in CAMPAIGNS,'unknown stage/campaign')
    if tier=='n8':return ('grib8','hdf58') if campaign in (0,3) else ('hdf58','grib8')
    # Reversal-paired balanced square: every group appears once in each of
    # four sequence positions. C3 reverses C0; C2 reverses C1.
    return (('grib8','hdf58','grib128','hdf5128'),
            ('hdf58','hdf5128','grib8','grib128'),
            ('grib128','grib8','hdf5128','hdf58'),
            ('hdf5128','grib128','hdf58','grib8'))[campaign]


def protocol_spec(repo):
    binding=read_json(repo/WORK/'protocol-binding.json')
    run.need(binding.get('complete') is True and binding.get('baseline_sha256')==BASELINE_SHA,
             'approved product freeze/baseline binding required')
    files=('query_template_client.py','block_transport.py','run_control.py','live_gate.py','pid_cpu_sampler.py',
        'README.md','experiment-plan.json','execution-paths.json','protocol-binding.json','test_offline.py')
    return ProtocolSpec(PROTOCOL,'experiments.fixed_matrix.query_template_pair',__doc__,BASELINE_SHA,
        frozenset(binding['product_diff']),tuple(tuple(v) for v in binding['required_regression_tests']),
        ('experiments/fixed_matrix/tests/test_query_template_pair.py',)+tuple(WORK+'/'+n for n in files))


def child_loop():
    output=sys.stdout;repo=Path(__file__).resolve().parents[2];h=helper(repo,'query_template_client')
    transport=helper(repo,'block_transport');me=transport.identity();state=context=selector=None
    natives={};templates=None
    def reply(value):
        output.write(json.dumps(dict(ok=True,**value),separators=(',',':'),allow_nan=False)+'\n');output.flush()
    def cancel(sig,frame):raise SystemExit('owned query child cancelled')
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):signal.signal(sig,cancel)
    with redirect_stdout(sys.stderr):
        reply(dict(event='ready',client_identity=me))
        try:
            for line in sys.stdin:
                request=json.loads(line);op=request['op']
                if op=='begin':
                    run.need(state is None,'client block already open');state=request
                    context={k:Path(v) if k in ('root','mount','socket') else v for k,v in request['context'].items()}
                    b=request['binding'];fmt=request['group']['format'];templates=h.Templates(fmt)
                    for mode in CONFIGS:natives[mode]=NativeContext(Path(request['library']),dict(request['cfg'],build_index=False))
                    plans={m:n.explain('index',b['native_selection'],client.PREDICATE,b['scope']) for m,n in natives.items()}
                    environment=telemetry.environment(repo,artifact_paths=[Path(request['library']),Path(request['binary'])],
                        process_roles=dict(client=me['pid'],daemon=context['pid'],datanode=request['datanode_pid']),
                        data_path=Path(request['data_root']))
                    response,prep=h.standalone(context,h.d_prepare(fmt,b['scope'],b['physical_versions']))
                    selector=response.get('selector_id');run.need(response.get('kind')=='prepared_catalog_selector' and
                        isinstance(selector,str) and selector,'discovery prepare response differs')
                    prep.update(lifecycle_role='discovery',selector_id=selector)
                    reply(dict(event='block_ready',client_identity=me,environment=environment,query_plans=plans,
                        lifecycle=[prep],profile_offset=context['profile_offset'],native_contexts={m:dict(
                        native_context_id=f'{me["pid"]}:{me["start_ticks"]}:{request["block_ordinal"]}:{m}',closed=False) for m in CONFIGS}))
                elif op=='query':
                    run.need(state is not None,'client block not open');arm=request['arm'];mode=request['configuration']
                    run.need(arm in ARMS and mode in CONFIGS,'unknown arm/mode');b=state['binding'];g=state['group']
                    before=telemetry.sample(state['resource_paths']);run.validate_sample(before);pb=context.get('profile_offset',0)
                    cb=clock_stamp()
                    row=(h.query(context,g,b['scope'],b['physical_versions'],b['expected_matches'],selector,mode,templates)
                        if arm=='IoTDB-FS' else client.query_native(natives[mode],arm,g,b['scope'],b['native_selection'],b['expected_matches']))
                    ce=clock_stamp();after=telemetry.sample(state['resource_paths']);run.validate_sample(after)
                    row.update(resource_before=before,resource_after=after,client_identity=me,
                        client_call_begin=cb,client_call_end=ce,profile_offset_before=pb,
                        profile_offset_after=context.get('profile_offset',0),native_context_id=
                        f'{me["pid"]}:{me["start_ticks"]}:{state["block_ordinal"]}:{mode}')
                    reply(dict(event='query',row=row,profile_offset=context.get('profile_offset',0)))
                elif op=='end':
                    run.need(state is not None,'client block not open');events=templates.release(context)
                    for event in events:event['lifecycle_role']='query'
                    response,release=h.standalone(context,dict(kind='catalog_selector_release',selector_id=selector))
                    run.need(response.get('kind')=='released_catalog_selector' and response.get('released') is True,'discovery release differs')
                    release.update(lifecycle_role='discovery',selector_id=selector);events.append(release)
                    for native in natives.values():native.close()
                    natives={};state=None
                    reply(dict(event='block_closed',client_identity=me,lifecycle=events,profile_offset=context['profile_offset']))
                elif op=='stop':
                    run.need(state is None,'cannot stop active block');reply(dict(event='stopped'));return
                else:raise RuntimeError('unknown child operation')
        finally:
            for native in natives.values():native.close()


class QueryTemplatePairRunner(CatalogSelectorPairRunner):
    def __init__(self,spec):super().__init__(spec,'grib')
    worker=NativeABRunner.worker

    def groups(self,repo,tier):
        run.need(tier in ('n8','n128'),'unknown stage')
        by={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
        return [by[f'FM1-{fmt}-N{n}-V1-C{n}-S0-base-latest'] for fmt,n in
                ([('grib',8),('hdf5',8)] if tier=='n8' else [('grib',8),('hdf5',8),('grib',128),('hdf5',128)])]

    def validate_n8_gate(self,gate,baseline_sha,candidate_sha):
        run.need(gate.get('protocol')==PROTOCOL and gate.get('tier')=='n8' and gate.get('query_records')==480 and
            gate.get('product_sha256')==candidate_sha and gate.get('baseline_sha256')==baseline_sha and
            all(gate.get(k) is True for k in ('complete','scientific_complete','ready_for_analysis',
                'original_service_restored','resources_restored','controller_restoration_verified')),
            'new joint-format N8 qualification/restoration required')

    def source_proof(self,args):
        proof=NativeABRunner.source_proof(self,args)
        if args.tier=='n128':
            prior=args.gate_report.parent;gate=read_json(args.gate_report)
            run.need(read_json(prior/'prepared.json')['sources']==self.closure(args.repo),'qualification closure changed')
            for name,key in (('queries.jsonl','queries_sha256'),('bindings.json','bindings_sha256'),
                ('analysis.json','analysis_sha256'),('selector-lifecycle.jsonl','lifecycle_sha256')):
                run.need(file_sha(prior/name)==gate[key],'qualification artifact changed: '+name)
        proof.update(phase='qualification' if args.tier=='n8' else 'joint-main',formats=['grib','hdf5'],
            comparison='same candidate ordinary/template mechanism; no cross-binary performance attribution',
            modes=MODE,group_order=[list(group_order(args.tier,c)) for c in CAMPAIGNS],
            fresh_client_per_mount=True,lifecycle_includes_prepare_release=True,all_rows_retained=True)
        return proof

    def plans(self,args,registry,report,proof):
        result,bindings=super().plans(args,registry,report,proof)
        by={e['group_id']:e for e in proof['cases']}
        for p in result:
            p['native_root']=Path(by[p['group']['group_id']]['native_root']) if p['group']['format']=='hdf5' else None
        return result,bindings

    def launcher(self,path,binary,sha,namespace,journal,plan):
        return (NativeABRunner.launcher(self,path,binary,sha,namespace,journal,plan) if plan['group']['format']=='hdf5'
                else run.infrastructure.launcher(path,binary,sha,namespace,journal))

    def validate_row(self,row,group,configuration):
        fmt=group['format'];run.need(configuration in CONFIGS and
            all(row.get(k) is True for k in ('oracle_verified','mode_verified')) and
            row.get('selected_units')==2 and row.get('owned_output_bytes')==40960 and
            row.get('execution_mode')==('native-hdf5' if fmt=='hdf5' else 'iotdb-indexed') and
            row.get('batch_enabled')==(fmt=='hdf5') and len(row['value_calls'])==(1 if fmt=='hdf5' else 2),
            'unchanged K2 owned query required')
        d=row['discovery_call']['server_profile'];q=row['value_calls'];want='typed_batch' if fmt=='hdf5' else 'typed'
        run.need(d.get('operation')==('dataset_catalog_by_selector' if fmt=='hdf5' else 'format_catalog_by_selector') and
            all(v['server_profile'].get('operation')==(want if configuration=='A' else 'query_template_execute') for v in q),
            'ordinary/template operation differs')
        for v in q:
            c=v['server_profile']['counters'];run.need(c.get('registration_attempts')==(3 if fmt=='hdf5' else 1),
                'original registration attempts differ')
            if fmt=='hdf5':run.need(c.get('typed_batch_items')==2,'original batch item count differs')
            if configuration=='B':
                expected={'query_template.executes':1,'query_template.declaration_reuses':2 if fmt=='hdf5' else 1,
                    'query_template.batch_executes':1 if fmt=='hdf5' else 0,'query_template.single_executes':0 if fmt=='hdf5' else 1,
                    'selector.compiles':0,'selector.input_path_items':0}
                run.need(all(type(c.get(k)) is int and c[k]==n for k,n in expected.items()),'template reuse mechanism differs')
        run.need(row['outer_query_wall_ns']==row['total_ns']+row['management_control_ns'] and
            row['total_ns']==row['discovery_ns']+row['value_ns'],'management/query clocks overlap')
        if row.get('phase')=='measurement':run.need(not row['lifecycle_events'],'warm templates unexpectedly rebuilt')
        return row

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        transport=helper(args.repo,'block_transport');paths=read_json(args.work/'resource-paths.json');count=0
        sha=file_sha(args.binary);writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        plans={p['group']['format']+str(p['group']['N']):p for p in execution}
        order=[(c,pos,plans[key]) for c in CAMPAIGNS for pos,key in enumerate(group_order(args.tier,c))]
        atomic_json(args.work/'execution-order.json',dict(mounts=[dict(campaign=c,sequence_position=pos,
            group=p['group']['group_id']) for c,pos,p in order],fresh_client_per_mount=True,modes=MODE))
        def event(kind,**value):run.append(args.work/'client-events.jsonl',dict(event=kind,**value,**clock_stamp()))
        for block_ordinal,(campaign,position,plan) in enumerate(order):
            g=plan['group'];gid=g['group_id'];b=bindings[gid]
            block=plan['execution_root']/('campaign-'+str(campaign))/'paired';block.mkdir(parents=True)
            journal=block/'owner';shutil.copytree(plan['journal'],journal)
            run.need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'journal copy differs')
            launch=self.launcher(block/'launcher',args.binary,sha,plan['item']['namespace'],journal,plan)
            atomic_json(block/'launcher.receipt.json',launch)
            before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
            run.need(before==plan['scientific_head'],'head changed before mount')
            print(json.dumps(dict(event='query_template_mount',campaign=campaign,sequence_position=position,group=gid)),flush=True)
            with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context:
                run.need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha and
                    read_json(context['root']/'profile.launch.json')['settings']==launch['settings'],'actual candidate/settings differ')
                child=transport.QueryChild(args,'block-'+str(block_ordinal),event)
                try:
                    ready=child.request(dict(op='begin',block_ordinal=block_ordinal,group=g,binding=b,
                        context={k:str(v) if isinstance(v,Path) else v for k,v in context.items() if k in ('root','mount','socket','pid')},
                        library=str(args.library),binary=str(args.binary),cfg=plan['cfg'],resource_paths=paths,
                        data_root=str(plan['root']),datanode_pid=int((args.repo.parent/'runtime/run/datanode.pid').read_text())))
                    run.need(ready['event']=='block_ready' and ready['client_identity']==child.identity,'child identity differs')
                    atomic_json(block/'environment.json',ready['environment'])
                    mount=dict(campaign=campaign,configuration='paired',sequence_position=position,block_ordinal=block_ordinal,
                        daemon_pid=context['pid'],startup_ns=context['startup_ns'],product_sha256=sha,
                        environment_sha256=file_sha(block/'environment.json'),client_identity=child.identity,
                        native_contexts=ready['native_contexts'],client_closed=False)
                    plan['item']['mounts'].append(mount)
                    def lifecycle(items,repeat=None):
                        for item in items:
                            item.update(group_id=gid,campaign=campaign,block_ordinal=block_ordinal,
                                sequence_position=position,client_identity=child.identity,configuration='B' if
                                item['kind'].startswith('query_template_') else 'shared',repeat=repeat)
                            run.append(args.work/'selector-lifecycle.jsonl',item)
                    lifecycle(ready['lifecycle'])
                    for mode in CONFIGS:
                        atomic_json(block/('query-plan-'+mode+'.json'),ready['query_plans'][mode])
                        for arm in ARMS:
                            bound=registry.register_run(build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                                implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                    runner_sha256=digest(frozen['sources']),engine=g['format']+' '+MODE[mode]),
                                clock=dict(kind='client-owned-full-query',d='actual selector discovery delivery',
                                    q='original serial/batch control through owned arrays; management controls separately timed',
                                    lifecycle='prepare+all actual queries+release, each clock once',profile_oracle='outside'),
                                cache=dict(protocol=PROTOCOL,configuration=mode,batch=g['format']=='hdf5',
                                    native_helper=plan['cfg'] if arm=='FS-index' else None,
                                    daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None),
                                environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign,warmup=5,measured=10,group_sequence_position=position,
                                    schedule='fresh client/mount; adjacent modes; AB/BA/BA/AB initial; formal5/5 first')))
                            b['run_ids'].setdefault(str(campaign),{}).setdefault(mode,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    for repeat in range(REPEATS):
                        for query_position,(arm,mode) in enumerate(query_order(campaign,repeat)):
                            response=child.request(dict(op='query',arm=arm,configuration=mode));row=response['row']
                            run.need(row['client_identity']==child.identity,'query executed in wrong client')
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][mode][arm],campaign=campaign,configuration=mode,arm=arm,repeat=repeat,
                                phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,resource_id=frozen['resource_id'],
                                environment_sha256=mount['environment_sha256'],product_sha256=sha,block_ordinal=block_ordinal,
                                sequence_position=position,query_position_in_repeat=query_position)
                            if arm=='IoTDB-FS':self.validate_row(row,g,mode);lifecycle(row['lifecycle_events'],repeat)
                            run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                        atomic_json(args.work/'report.json',report)
                    closed=child.request(dict(op='end'),60);run.need(closed['event']=='block_closed','block failed to close')
                    lifecycle(closed['lifecycle']);mount.update(selector_released=True,templates_released=True)
                    for n in mount['native_contexts'].values():n['closed']=True
                    child.close();mount['client_closed']=True
                except BaseException:
                    child.close(False);raise
            after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
            run.need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'] and
                file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed sealed inputs')
            mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
            atomic_json(args.work/'report.json',report)
        report.update(lifecycle_sha256=file_sha(args.work/'selector-lifecycle.jsonl'),client_events_sha256=file_sha(args.work/'client-events.jsonl'))
        return count

    def analyze(self,path,bindings,selected,resource_id,candidate_sha):
        rows=[json.loads(line) for line in Path(path).read_text().splitlines()]
        tier='n8' if len(selected)==2 else 'n128';lookup={g['format']+str(g['N']):g['group_id'] for g in selected}
        wanted=[(lookup[key],c,m,a,r) for c in CAMPAIGNS for key in group_order(tier,c)
                for r in range(REPEATS) for a,m in query_order(c,r)]
        run.need([(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows]==wanted,
                 'complete exact joint schedule required; never exclude slow rows')
        gm={g['group_id']:g for g in selected};identities={};groups=[]
        for row in rows:
            b=bindings[row['group_id']];g=gm[row['group_id']];mode=row['configuration'];who=row['client_identity']
            run.need(row['resource_id']==resource_id and row['product_sha256']==candidate_sha and
                all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
                row['run_id']==b['run_ids'][str(row['campaign'])][mode][row['arm']],'science/run/candidate differs')
            run.need(row['total_ns']==row['discovery_ns']+row['value_ns'] and
                all(type(row[k]) is int and row[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
                all(row.get(k) is True for k in ('ok','oracle_verified','mode_verified')) and
                row['phase']==('warmup' if row['repeat']<5 else 'measurement'),'owned query/clock/phase differs')
            for side in ('resource_before','resource_after'):
                sample=row[side];run.validate_sample(sample)
                run.need(sample['sampler']['pid']==who['pid'] and sample['processes'][str(who['pid'])]['start_ticks']==who['start_ticks'],
                    'actual independent client resource identity differs')
            run.need(row['resource_before']['monotonic_ns']<=row['client_call_begin']['monotonic_ns']<
                row['client_call_end']['monotonic_ns']<=row['resource_after']['monotonic_ns'],'resource/query envelope differs')
            run.need(row['client_call_end']['monotonic_ns']-row['client_call_begin']['monotonic_ns']>=
                row.get('outer_query_wall_ns',row['total_ns']),'owned query exceeds observed client wall envelope')
            identities.setdefault((row['group_id'],row['campaign']),set()).add((who['pid'],who['start_ticks']))
            if row['arm']=='IoTDB-FS':self.validate_row(row,g,mode)
        run.need(all(len(v)==1 for v in identities.values()) and len(set.union(*identities.values()))==len(selected)*4,
            'one distinct query client per mount required')
        for g in selected:
            subset=[r for r in rows if r['group_id']==g['group_id']]
            for campaign in CAMPAIGNS:
                for repeat in range(WARMUPS,REPEATS):
                    pair={r['configuration']:r for r in subset if r['campaign']==campaign and
                        r['repeat']==repeat and r['arm']=='IoTDB-FS'}
                    charges={m:[call['server_profile']['counters'].get('request_metadata.consumed_bytes')
                        for call in [r['discovery_call']]+r['value_calls']] for m,r in pair.items()}
                    run.need(set(charges)==set(CONFIGS) and all(type(v) is int and v>0
                        for values in charges.values() for v in values) and charges['A']==charges['B'],
                        'ordinary/template exact formal d/q logical charges differ')
            groups.append(dict(group_id=g['group_id'],**effects(subset)))
        lifecycle=[json.loads(line) for line in (Path(path).parent/'selector-lifecycle.jsonl').read_text().splitlines()]
        costs=[]
        for g in selected:
            for campaign in CAMPAIGNS:
                lc=[e for e in lifecycle if (e['group_id'],e['campaign'])==(g['group_id'],campaign)]
                query=[r for r in rows if (r['group_id'],r['campaign'],r['arm'])==(g['group_id'],campaign,'IoTDB-FS')]
                singles=2 if g['format']=='grib' else 1
                kinds=[e['kind'] for e in lc]
                run.need(kinds==['catalog_selector_prepare']+['query_template_prepare']*singles+
                    ['query_template_release']*singles+['catalog_selector_release'],
                    'complete d/q prepare/release lifecycle required')
                prepare=[e for e in lc if e['kind']=='query_template_prepare'];release=[e for e in lc if e['kind']=='query_template_release']
                run.need([e['template_id'] for e in prepare]==[e['template_id'] for e in release] and
                    lc[0]['selector_id']==lc[-1]['selector_id'] and all(e['server_profile']['operation']==e['kind'] for e in lc),
                    'lifecycle identity/operation differs')
                sequences=[lc[0]['server_profile']['sequence']]
                for r in query:
                    calls=[r['discovery_call']]+r['lifecycle_events']+r['value_calls']
                    sequences.extend(call['server_profile']['sequence'] for call in calls)
                sequences.extend(e['server_profile']['sequence'] for e in lc if e['kind'] in ('query_template_release','catalog_selector_release'))
                run.need(sequences==list(range(1,(96 if g['format']=='grib' else 64)+1)),
                    'every actual profile must appear once in complete ordered stream')
                qprepare=sum(e['total_ns'] for e in prepare);qrelease=sum(e['total_ns'] for e in release)
                run.need(sum(r['management_control_ns'] for r in query)==qprepare,'management prepare clocks differ')
                costs.append(dict(group_id=g['group_id'],campaign=campaign,query_prepare_ns=qprepare,
                    query_release_ns=qrelease,discovery_prepare_ns=lc[0]['total_ns'],discovery_release_ns=lc[-1]['total_ns'],
                    ordinary_queries_ns=sum(r['total_ns'] for r in query if r['configuration']=='A'),
                    template_queries_ns=sum(r['total_ns'] for r in query if r['configuration']=='B'),
                    template_q_lifecycle_ns=qprepare+qrelease+sum(r['value_ns'] for r in query if r['configuration']=='B'),
                    limitation='shared d/cache order; not independent cold startup comparison'))
        return dict(protocol=PROTOCOL,complete=True,query_rows=len(rows),groups=groups,
            independent_query_clients=len(identities),all_rows_retained=True,lifecycle=costs,
            limitation='same candidate warm mechanism comparison; first executions share cache and are order dependent; lifecycle separately counted')


if __name__=='__main__':
    if sys.argv[1:]==['--query-child']:child_loop()
    else:QueryTemplatePairRunner(protocol_spec(Path(__file__).resolve().parents[2])).main()
