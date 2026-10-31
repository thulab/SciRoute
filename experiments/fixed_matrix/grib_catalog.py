"""Query-only GRIB location-hint/catalog ablation on two sealed FM1 cases.

N8 smoke (72 rows) must pass before the independent N128 formal run (216).
No publication, generation, index construction, source mutation or implicit retry.
"""
from __future__ import annotations
import argparse
from contextlib import ExitStack
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import time

from . import run, telemetry, client, grib_catalog_client
from fs_index.native import NativeContext
from .replay import SOURCE_PINS
from .hdf5_cache_batch import verify_build_binding, finish_children
from experiments.identity.model import build_run,digest
from experiments.identity.registry import Registry,file_sha,read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.resource_limited.resources import CgroupV2Resources,atomic_json

need=run.need
PROTOCOL='iotdbfs-fixed-grib-catalog/1'
BASELINE_SHA='b7e7d9955580c5f9401912dc5948685d4208c7fde9d6d34e6160e2364d236589'
GROUPS=('FM1-grib-N8-V1-C8-S0-base-latest','FM1-grib-N128-V1-C128-S0-base-latest')
CONFIGS=('A','B','C')
ARMS=('IoTDB-FS','FS-index')


def groups(repo,smoke=False):
    catalog={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    return [catalog[GROUPS[0 if smoke else 1]]]


def campaigns(args):return (0,) if args.smoke else (0,1,2)
def expected_rows(args):return len(campaigns(args))*len(CONFIGS)*len(ARMS)*12

def configuration_order(campaign):
    return tuple(CONFIGS[(i+campaign)%3] for i in range(3))


def require_json_roundtrip(value,label):
    # Reject tuples, integer object keys, etc. Do not normalize away the
    # mismatch: the worker must reproduce exactly the frozen JSON proof.
    need(value==json.loads(json.dumps(value,allow_nan=False)),label+' is not JSON round-trip stable')


def closure(repo):
    result=run.closure(repo)
    for relative in ('experiments/fixed_matrix/tests/test_grib_catalog.py',
                     'experiments/cross_version/grib_discovery/run.sh',
                     'experiments/cross_version/grib_discovery/README.md'):
        result[relative]=file_sha(repo/relative)
    return result


def source_proof(args):
    """Read-only checks on selected sealed scientific inputs, before service work."""
    source=args.source_collection
    need(source.name=='collection-r2','only original FM1 collection-r2 publications are accepted')
    for name,sha in SOURCE_PINS.items():need(file_sha(source/name)==sha,'sealed source changed: '+name)
    need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json')==run.CATALOG_SHA,'fixed catalog changed')
    old=read_json(source/'prepared.json')
    need(old['native_library_sha256']==run.NATIVE_SHA and file_sha(args.library)==run.NATIVE_SHA,'FS helper changed')
    need(file_sha(args.inspector)==run.infrastructure.INSPECTOR_SHA,'head inspector changed')
    for name in ('experiments/fixed_matrix/client.py','experiments/fixed_matrix/native.py','experiments/fixed_matrix/catalog.json'):
        need(file_sha(source/'source'/name)==old['sources'][name]==file_sha(args.repo/('fs_index/native.py' if name=='experiments/fixed_matrix/native.py' else name)),'sealed scientific client changed: '+name)
    need(file_sha(args.baseline_binary)==BASELINE_SHA,'approved A binary changed')
    build_gate=verify_build_binding(args)
    need(file_sha(args.binary)!=BASELINE_SHA,'A/B must be distinct pinned products')
    registry=Registry(source/'registry');bindings=read_json(source/'bindings.json');report=read_json(source/'report.json')
    entries=[]
    for group in groups(args.repo,args.smoke):
        gid=group['group_id'];bound=bindings[gid];corpus=registry.get_corpus(bound['corpus_id']);case=registry.get_case(bound['case_id'])
        need(case['corpus_id']==corpus['corpus_id'] and case['expected_matches']==bound['expected_matches'] and
             digest(case['expected_matches'])==bound['oracle_sha256'],'case/oracle binding changed')
        matches=[c for c in report['corpora'] if c['corpus_id']==bound['corpus_id']]
        need(len(matches)==1,'independent corpus is not uniquely bound')
        item=matches[0];base=source/'corpora'/item['corpus_key'];prep=read_json(base/'preparation.json')
        manifest=read_json(base/'input/manifest.json')
        need(prep['complete'] and file_sha(base/'index.sqlite')==item['index_sha256']==prep['index_sha256'],'FS index changed')
        need(file_sha(base/'input/manifest.json')==item['raw_manifest_sha256']==corpus['source_evidence']['manifest_sha256'],'manifest changed')
        need(file_sha(base/'publication.json')==prep['publication_receipt_sha256']==corpus['publication_evidence']['receipt_sha256'],'publication changed')
        need(read_json(base/'publication.json')['head']==item['head'],'published head differs')
        need(run.infrastructure.journal_snapshot(base/'owner')==item['journal_digest'],'archived owner journal changed')
        entries.append(dict(group_id=gid,case_id=case['case_id'],corpus_id=corpus['corpus_id'],corpus_key=item['corpus_key'],
            oracle_sha256=bound['oracle_sha256'],source_root=str(base),head=item['head'],journal_digest=item['journal_digest'],
            artifacts={n:file_sha(base/n) for n in ('input/manifest.json','catalog.json','index.sqlite','publication.json','preparation.json')},
            raw_validation=run.verify_raw(manifest,base/'input',base/'fs')))
    gate=None
    if not args.smoke:
        need(args.smoke_report is not None,'N128 requires accepted N8 smoke report, no automatic scale-up')
        gate=read_json(args.smoke_report)
        need(gate.get('complete') is True and gate.get('smoke') is True and gate.get('query_records')==72 and
             gate.get('original_service_restored') is True and gate.get('controller_restoration_verified') is True and
             gate.get('resources_restored') is True and gate.get('product_sha256')==file_sha(args.binary) and
             gate.get('baseline_sha256')==BASELINE_SHA,'N8 gate incomplete, not restored, or different product')
    return dict(protocol=PROTOCOL,complete=True,cases=entries,source_collection=str(source),source_pins=SOURCE_PINS,
        configurations=list(CONFIGS),configuration_meaning=dict(A='approved baseline typed',B='candidate typed/hint',C='candidate format_catalog/hint'),
        campaigns=list(campaigns(args)),orders=[dict(campaign=c,order=list(configuration_order(c))) for c in campaigns(args)],
        arms=list(ARMS),expected_queries=expected_rows(args),warmup_per_arm=2,measured_per_arm=10,
        build_gate=build_gate,baseline=dict(path=str(args.baseline_binary),sha256=BASELINE_SHA),
        product=dict(path=str(args.binary),sha256=file_sha(args.binary)),smoke=args.smoke,
        smoke_report_sha256=file_sha(args.smoke_report) if gate else None,scientific_publication=False,index_rebuilt=False,new_cases=False,
        mutation='query only on archived-head journal copies; scientific source remains unchanged')


def plans(args,registry,report,proof):
    prior=read_json(args.source_collection/'report.json');oldbind=read_json(args.source_collection/'bindings.json')
    selected={g['group_id']:g for g in groups(args.repo,args.smoke)};source_registry=Registry(args.source_collection/'registry')
    result=[];bindings={}
    for entry in proof['cases']:
        gid=entry['group_id'];cid=entry['corpus_id'];base=Path(entry['source_root'])
        corpus=registry.register_corpus(source_registry.get_corpus(cid));registry.register_case(source_registry.get_case(entry['case_id']))
        bindings[gid]=deepcopy(oldbind[gid]);bindings[gid]['run_ids']={}
        execution=args.work/'corpora'/entry['corpus_key'];execution.mkdir(parents=True,mode=0o700)
        item=deepcopy(next(x for x in prior['corpora'] if x['corpus_id']==cid))
        item.update(complete=False,mounts=[],source_root=str(base));report['corpora'].append(item)
        result.append(dict(root=base,execution_root=execution,corpus=corpus,group=selected[gid],item=item,
            journal_digest=entry['journal_digest'],scientific_head=entry['head'],prep=read_json(base/'preparation.json'),
            manifest=read_json(base/'input/manifest.json'),cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)))
    return result,bindings


def validate_row(row,configuration,expected_units):
    need(row.get('oracle_verified') is True and row.get('mode_verified') is True and
         row.get('execution_mode')=='iotdb-indexed' and row.get('selected_units')==expected_units,'GRIB result not verified')
    discovery=row.get('discovery_call',{});profile=discovery.get('server_profile',{})
    need(len(row.get('value_calls',[]))==expected_units,'typed value count differs')
    if configuration=='C':
        need(row.get('discovery_protocol')=='format_catalog' and profile.get('operation')=='format_catalog' and
             discovery.get('response_path') is None and discovery.get('ipc_bytes')==0 and 'path' not in profile,
             'lightweight discovery used typed/FUSE result route')
    else:need(row.get('discovery_protocol')=='typed' and discovery.get('response_path'),'A/B must use original typed discovery')
    return row


def collect(args,execution,bindings,registry,frozen,report,env,paired):
    paths=read_json(args.work/'resource-paths.json');count=0
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    for campaign in campaigns(args):
        for plan in execution:
            group=plan['group'];gid=group['group_id'];b=bindings[gid]
            for config in configuration_order(campaign):
                block=plan['execution_root']/('campaign-'+str(campaign))/config;block.mkdir(parents=True)
                journal=block/'owner';shutil.copytree(plan['root']/'owner',journal)
                need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'private journal copy differs')
                binary=args.baseline_binary if config=='A' else args.binary;sha=file_sha(binary)
                launch=run.infrastructure.launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal)
                atomic_json(block/'launcher.receipt.json',launch)
                before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                need(before==plan['scientific_head'],'head changed before query')
                print(json.dumps(dict(event='grib_discovery_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
                with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                    need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha,'running product differs')
                    actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual settings differ')
                    native=stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                    environment=telemetry.environment(args.repo,artifact_paths=[args.library,binary],process_roles=dict(client=os.getpid(),
                        daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                    atomic_json(block/'environment.json',environment)
                    mount=dict(campaign=campaign,configuration=config,daemon_pid=context['pid'],startup_ns=context['startup_ns'],
                        product_sha256=sha,environment_sha256=file_sha(block/'environment.json'));plan['item']['mounts'].append(mount)
                    atomic_json(block/'query-plan.json',native.explain('index',b['native_selection'],client.PREDICATE,b['scope']))
                    for arm in ARMS:
                        bound=registry.register_run(build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                            implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                runner_sha256=digest(frozen['sources']),engine=('GRIB native field; '+('format_catalog' if config=='C' else 'typed')+' discovery')
                                    if arm=='IoTDB-FS' else 'SQLite cross-file + native ecCodes owned values'),
                            clock=dict(kind='client-owned-full-query',d='entry through frozen discovery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                            cache=dict(protocol=PROTOCOL,configuration=config,native_helper=plan['cfg'] if arm=='FS-index' else None,
                                daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None),
                            environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=2,measured=10,schedule='paired alternating arms; parity campaign+configuration+repeat')))
                        b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    for repeat in range(12):
                        order=ARMS if (repeat+campaign+CONFIGS.index(config))%2==0 else tuple(reversed(ARMS))
                        for arm in order:
                            rb=telemetry.sample(paths);run.validate_sample(rb)
                            if arm=='IoTDB-FS':
                                row=grib_catalog_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],lightweight=config=='C')
                                validate_row(row,config,group['K_units'])
                            else:row=client.query_native(native,arm,group,b['scope'],b['native_selection'],b['expected_matches'])
                            ra=telemetry.sample(paths);run.validate_sample(ra)
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][config][arm],campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                phase='warmup' if repeat<2 else 'measurement',ok=True,resource_id=frozen['resource_id'],resource_before=rb,
                                resource_after=ra,environment_sha256=mount['environment_sha256'],product_sha256=sha)
                            run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                        atomic_json(args.work/'report.json',report)
                after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'query changed head/journal')
                need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed FS-index')
                mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True);atomic_json(args.work/'report.json',report)
    return count


def analyze(path,bindings,selected,campaign_ids):
    rows=[json.loads(s) for s in Path(path).read_text().splitlines()]
    wanted={(g['group_id'],c,f,a,r) for g in selected for c in campaign_ids for f in CONFIGS for a in ARMS for r in range(12)}
    keys={(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows}
    need(len(rows)==len(wanted) and keys==wanted,'all preregistered rows required; no latency exclusions')
    gm={g['group_id']:g for g in selected}
    resource_ids={r.get('resource_id') for r in rows}
    need(len(resource_ids)==1 and None not in resource_ids and '' not in resource_ids,'mixed or absent resource identity')
    candidate_shas={r.get('product_sha256') for r in rows if r['configuration'] in ('B','C')}
    need(len(candidate_shas)==1 and None not in candidate_shas and BASELINE_SHA not in candidate_shas,'B/C product differs or is baseline')
    need(all(r.get('product_sha256')==BASELINE_SHA for r in rows if r['configuration']=='A'),'A is not pinned baseline')
    for row in rows:
        b=bindings[row['group_id']];config=row['configuration']
        need(all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')),'row science identity differs')
        need(row['run_id']==b['run_ids'][str(row['campaign'])][config][row['arm']],'run identity differs')
        need(row['ok'] is True and row['oracle_verified'] is True and row['mode_verified'] is True,'unverified row')
        need(row['selected_units']==gm[row['group_id']]['K_units'] and row['owned_output_bytes']==gm[row['group_id']]['owned_output_bytes'],'owned output boundary differs')
        need(row['phase']==('warmup' if row['repeat']<2 else 'measurement'),'phase differs')
        need(all(type(row[k]) is int and row[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
             row['total_ns']==row['discovery_ns']+row['value_ns'],'full query clock differs')
        for side in ('resource_before','resource_after'):run.validate_sample(row[side])
        need(row['resource_after']['monotonic_ns']>row['resource_before']['monotonic_ns'],'resource clock reversed')
        if row['arm']=='IoTDB-FS':validate_row(row,config,gm[row['group_id']]['K_units'])
    cells=[]
    for group in selected:
        for config in CONFIGS:
            blocks=[]
            for campaign in campaign_ids:
                arms={arm:{k:statistics.median(r[k] for r in rows if r['group_id']==group['group_id'] and r['campaign']==campaign and
                    r['configuration']==config and r['arm']==arm and r['phase']=='measurement')/1e6 for k in ('total_ns','discovery_ns','value_ns')} for arm in ARMS}
                blocks.append(dict(campaign=campaign,warm_median_ms=arms,**{ratio:arms['IoTDB-FS'][key]/arms['FS-index'][key]
                    for ratio,key in (('R','total_ns'),('Rd','discovery_ns'),('Rq','value_ns'))}))
            cells.append(dict(group_id=group['group_id'],table_id=group['table_id'],configuration=config,blocks=blocks,
                warm_median_ms={a:{k:statistics.median(b['warm_median_ms'][a][k] for b in blocks) for k in ('total_ns','discovery_ns','value_ns')} for a in ARMS},
                **{k:statistics.median(b[k] for b in blocks) for k in ('R','Rd','Rq')},R_range=[min(b['R'] for b in blocks),max(b['R'] for b in blocks)]))
    effects=[]
    for group in selected:
        by={c['configuration']:c for c in cells if c['group_id']==group['group_id']}
        for left,right in (('B','A'),('C','B'),('C','A')):
            blocks=[dict(campaign=a['campaign'],**{metric:a[metric]/b[metric] for metric in ('R','Rd','Rq')},
                         raw_IoTDB_T_ratio=a['warm_median_ms']['IoTDB-FS']['total_ns']/b['warm_median_ms']['IoTDB-FS']['total_ns'])
                    for a,b in zip(by[left]['blocks'],by[right]['blocks'])]
            effects.append(dict(group_id=group['group_id'],numerator=left,denominator=right,blocks=blocks,
                **{k:statistics.median(b[k] for b in blocks) for k in ('R','Rd','Rq','raw_IoTDB_T_ratio')},
                R_range=[min(b['R'] for b in blocks),max(b['R'] for b in blocks)]))
    return dict(complete=True,protocol=PROTOCOL,query_rows=len(rows),campaigns=list(campaign_ids),cells=cells,effects=effects,
        statistic='Median of campaign warm medians; R/Rd/Rq formed against same-block FS-index; all rows retained.',
        limitation='Sequential warm-query ablation with independent fresh mounts/FS contexts; no parallelism or cold-disk claim.')


def worker(args):
    def stop(sig,frame):raise RuntimeError('GRIB catalog worker cancelled; preserve evidence')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
    proof=source_proof(args);frozen=read_json(args.work/'prepared.json')
    need(proof==read_json(args.work/'reuse-source.json') and closure(args.repo)==frozen['sources'],'frozen preflight changed')
    paired,_,env,startup=run.dependencies(args.repo);startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    report=dict(protocol=PROTOCOL,complete=False,query_records=0,expected_queries=expected_rows(args),corpora=[],
        product_sha256=file_sha(args.binary),baseline_sha256=BASELINE_SHA,resource_id=frozen['resource_id'],
        scientific_publication=False,index_rebuilt=False,new_cases=False,smoke=args.smoke,credentials_recorded=False)
    atomic_json(args.work/'report.json',report)
    try:
        registry=Registry(args.work/'registry');execution,bindings=plans(args,registry,report,proof)
        atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),arms=list(ARMS),
            mounts=[dict(campaign=c,order=configuration_order(c)) for c in campaigns(args)],fresh_mount_and_FS_context=True))
        count=collect(args,execution,bindings,registry,frozen,report,env,paired)
        need(count==expected_rows(args),'GRIB catalog collection incomplete')
        need(source_proof(args)==proof and closure(args.repo)==frozen['sources'],'source or binary changed')
        for p in execution:p['item'].update(complete=True,final_raw_validation=run.verify_raw(p['manifest'],p['root']/'input',p['root']/'fs'))
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
    except BaseException as error:
        report['failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:atomic_json(args.work/'report.json',report)


def supervise(args,proof):
    """Existing resource/service primitives; supervisor never enters query leaf."""
    require_json_roundtrip(proof,'preflight proof')
    args.work.mkdir(parents=True,mode=0o700,exist_ok=False)
    atomic_json(args.work/'reuse-source.json',proof)
    run.bootstrap(args)  # Import/client ABI gate before any service/resource change.
    sources=closure(args.repo)
    require_json_roundtrip(sources,'source closure')
    for name,sha in sources.items():
        target=args.work/'source'/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(args.repo/name,target);need(file_sha(target)==sha,'source snapshot differs')
    _,_,args.env,_=run.dependencies(args.repo)
    args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text());args.report=args.work/'service-report.json'
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(x in argv for x in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')),'database memory differs')
    software=run.software_inventory(args.repo);atomic_json(args.work/'software.json',software)
    environment=telemetry.environment(args.repo,artifact_paths=[args.library,args.binary,args.baseline_binary,args.inspector],
        process_roles=software['process_roles'],data_path=args.work)
    atomic_json(args.work/'environment.json',environment)
    atomic_json(args.work/'support-roles.json',{k:v for k,v in software['process_roles'].items() if k!='datanode'})
    prepared=dict(protocol=PROTOCOL,sources=sources,product=dict(path=str(args.binary),sha256=file_sha(args.binary)),
        baseline=dict(path=str(args.baseline_binary),sha256=BASELINE_SHA),native_library_sha256=run.NATIVE_SHA,
        inspector=str(args.inspector),resource_id=digest(dict(resources=run.infrastructure.resource_identity([2,3]),
            environment_sha256=file_sha(args.work/'environment.json'))))
    atomic_json(args.work/'prepared.json',prepared)
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    process=sampler=None;service={};failure=None;handlers={}
    def cancelled(sig,frame):
        if process is not None and process.poll() is None:process.terminate()
        raise RuntimeError('GRIB catalog supervisor cancelled; restore original service/resources')
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):handlers[sig]=signal.signal(sig,cancelled)
    try:
        manager.create();atomic_json(args.work/'resource-before.json',manager.snapshot())
        with (args.work/'sampler.private.stderr').open('x') as err:
            sampler=subprocess.Popen([sys.executable,'-m','experiments.fixed_matrix.telemetry','--paths',str(args.work/'resource-paths.json'),
                '--roles',str(args.work/'support-roles.json'),'--output',str(args.work/'resource-samples.jsonl'),
                '--stop-file',str(args.work/'sampler.stop'),'--interval','1','--max-seconds','14000'],
                cwd=args.repo,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=err)
        resources_script=Path(run.infrastructure.__file__).with_name('resources.py')
        with run.infrastructure.database_start_in_leaf(resources_script,manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service):
                try:
                    command=[sys.executable,str(resources_script),'enter-leaf-exec','--leaf',manager.paths['query'],
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.grib_catalog','--worker']
                    for key in ('repo','work','binary','baseline_binary','build_binding','library','inspector','source_collection'):
                        command.extend(['--'+key.replace('_','-'),str(getattr(args,key))])
                    if args.smoke:command.append('--smoke')
                    elif args.smoke_report:command.extend(['--smoke-report',str(args.smoke_report)])
                    with (args.work/'worker.stdout').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=args.repo,env=run.worker_environment(),stdin=subprocess.DEVNULL,
                            stdout=out,stderr=err,start_new_session=True)
                        manager.attach_query(process.pid);started=time.monotonic()
                        while process.poll() is None:
                            need(sampler.poll() is None,'resource sampler stopped')
                            run.infrastructure.observe(manager.paths)
                            need(time.monotonic()-started<=12600,'GRIB worker hard timeout')
                            time.sleep(5)
                    need(process.returncode==0,'GRIB worker failed; preserve evidence, no automatic retry')
                finally:finish_children(process,manager,args.work)
    except BaseException as error:
        failure=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:
        try:manager.close();atomic_json(args.work/'resources-restored.json',manager.state)
        finally:
            (args.work/'sampler.stop').touch()
            if sampler is not None:
                try:sampler.wait(timeout=10)
                except subprocess.TimeoutExpired:sampler.terminate();sampler.wait(timeout=10)
            report=read_json(args.work/'report.json') if (args.work/'report.json').exists() else dict(complete=False,query_records=0)
            science=report.get('complete') is True
            report.update(service,scientific_complete=science,resources_restored=manager.state.get('restoration_complete',False))
            if failure:report['controller_failure']=failure
            report['ready_for_analysis']=bool(science and not failure and sampler is not None and sampler.returncode==0 and
                report.get('original_service_restored') is True and report.get('controller_restoration_verified') is True and report['resources_restored'])
            report['complete']=False;atomic_json(args.work/'report.json',report)
            for sig,handler in handlers.items():signal.signal(sig,handler)
    need(report['ready_for_analysis'],'scientific run, sampler or restoration incomplete')
    try:
        rows=[json.loads(s) for s in (args.work/'queries.jsonl').read_text().splitlines()]
        coverage=run.validate_coverage(args.work,rows)
        summary=analyze(args.work/'queries.jsonl',read_json(args.work/'bindings.json'),groups(args.repo,args.smoke),campaigns(args))
        atomic_json(args.work/'analysis.json',summary)
        report.update(complete=True,resource_coverage=coverage,analysis_sha256=file_sha(args.work/'analysis.json'))
    except BaseException as error:
        report['analysis_failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=True,queries=expected_rows(args),report=str(args.work/'report.json'))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('repo','work','binary','baseline-binary','build-binding','library','inspector','source-collection'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--smoke-report',type=Path)
    parser.add_argument('--smoke',action='store_true',help='N8 gate only; no automatic formal N128 collection')
    parser.add_argument('--worker',action='store_true');parser.add_argument('--prepare-only',action='store_true')
    args=parser.parse_args()
    for key in ('repo','binary','baseline_binary','build_binding','library','inspector','source_collection'):
        setattr(args,key,getattr(args,key).resolve(strict=True))
    if args.smoke_report:args.smoke_report=args.smoke_report.resolve(strict=True)
    args.work=args.work.resolve()
    need(not args.work.is_relative_to(args.source_collection),'new run cannot overwrite scientific source')
    need(not args.source_collection.is_relative_to(args.work),'new run cannot contain scientific source')
    if args.worker:worker(args)
    else:
        proof=source_proof(args)
        require_json_roundtrip(proof,'preflight proof')
        if args.prepare_only:
            args.work.mkdir(parents=True,mode=0o700,exist_ok=False);atomic_json(args.work/'preflight.json',proof)
            print(json.dumps(dict(complete=True,scientific_queries=0,expected_queries=expected_rows(args),smoke=args.smoke)))
        else:supervise(args,proof)


if __name__=='__main__':main()
