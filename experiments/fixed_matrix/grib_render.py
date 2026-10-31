"""Query-only GRIB render allocation/geometry ablation on two sealed FM1 cases.

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
from . import grib_catalog as catalog_run
from fs_index.native import NativeContext
from .replay import SOURCE_PINS
from .hdf5_cache_batch import verify_build_binding, finish_children
from experiments.identity.model import build_run,digest
from experiments.identity.registry import Registry,file_sha,read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.resource_limited.resources import CgroupV2Resources,atomic_json

need=run.need
PROTOCOL='iotdbfs-fixed-grib-render/1'
BASELINE_SHA='f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
BASELINE_AUDIT_SHA='2187f0254e4fb45e9cc590e4394691dcfb4c1e657fec445e90ee0f6933154d6b'
GEOMETRY_SETTING='query.grib.geometry_cache.enabled'
FROZEN_HELPERS={
 'experiments/fixed_matrix/client.py':'c163cadfdbee49076c24a84e3e61b5226f6155b592c180260cf6acf52533c563',
 'experiments/fixed_matrix/native.py':'ce10863a7a93fa756b4f5c7f78960dd44df3ff52fa0e447bb9b1349bbbb4320b',
 'experiments/fixed_matrix/grib_catalog.py':'036d1f3f8e45845b57b64438066f1bb8bb6b7f9aa4173a88d2074738aa0d4d2b',
 'experiments/fixed_matrix/grib_catalog_client.py':'bdb12ca35341a5ffad26580a5f7ddbdea3d513cc2b5b53dcbb47f281a68d1d24'}
GROUPS=('FM1-grib-N8-V1-C8-S0-base-latest','FM1-grib-N128-V1-C128-S0-base-latest')
CONFIGS=('A','B','C')
ARMS=('IoTDB-FS','FS-index')


# These helpers depend only on the same approved case ids, matrix shape and
# JSON invariants. No runtime rebinding of the frozen module's globals.
groups=catalog_run.groups
campaigns=catalog_run.campaigns
expected_rows=catalog_run.expected_rows
configuration_order=catalog_run.configuration_order
require_json_roundtrip=catalog_run.require_json_roundtrip
plans=catalog_run.plans


def closure(repo):
    result=run.closure(repo)
    for relative in ('experiments/fixed_matrix/tests/test_grib_render.py',
                     'experiments/cross_version/grib_render/run.sh',
                     'experiments/cross_version/grib_render/README.md'):
        result[relative]=file_sha(repo/relative)
    return result


def accepted_baseline(args,entries):
    path=args.repo/'experiments/cross_version/work/grib-discovery-20261022/independent-audit.json'
    need(file_sha(path)==BASELINE_AUDIT_SHA,'accepted format_catalog baseline audit changed')
    audit=read_json(path)
    need(audit.get('complete') is True and audit.get('formal_complete') is True and audit.get('rows')==288 and
         audit.get('build_gate',{}).get('binary_sha256')==BASELINE_SHA,'baseline is not the accepted previous product')
    prior={segment['science']['group_id']:segment for segment in audit['segments']}
    for entry in entries:
        old=prior[entry['group_id']]
        need(old['candidate_sha256']==BASELINE_SHA and all(old['science'][key]==entry[key] for key in ('corpus_id','case_id','oracle_sha256')) and
             old['science']['raw_and_FS_inventory_sha256']==entry['raw_validation']['hashes_sha256'],
             'baseline used another scientific corpus/case')
    return dict(path=str(path),sha256=BASELINE_AUDIT_SHA,rows=288,product_sha256=BASELINE_SHA)


def geometry_settings(configuration):
    need(configuration in CONFIGS,'unknown render configuration')
    return {} if configuration=='A' else {GEOMETRY_SETTING:'false' if configuration=='B' else 'true'}


def launcher(path,binary,sha,namespace,journal,configuration):
    receipt=run.infrastructure.launcher(path,binary,sha,namespace,journal)
    additions=geometry_settings(configuration)
    if additions:
        original='SETTINGS='+repr(receipt['settings'])+'\n'
        settings=dict(receipt['settings'],**additions);text=path.read_text()
        need(text.count(original)==1,'launcher declaration changed')
        path.write_text(text.replace(original,'SETTINGS='+repr(settings)+'\n',1))
        receipt.update(settings=settings,sha256=file_sha(path))
    receipt['render_configuration']=configuration
    return receipt


def source_proof(args):
    """Read-only checks on selected sealed scientific inputs, before service work."""
    source=args.source_collection
    need(source.name=='collection-r2','only original FM1 collection-r2 publications are accepted')
    for name,sha in SOURCE_PINS.items():need(file_sha(source/name)==sha,'sealed source changed: '+name)
    need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json')==run.CATALOG_SHA,'fixed catalog changed')
    for relative,sha in FROZEN_HELPERS.items():need(file_sha(args.repo/('fs_index/native.py' if relative=='experiments/fixed_matrix/native.py' else relative))==sha,'frozen helper changed: '+relative)
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
        need(gate.get('protocol')==PROTOCOL and gate.get('complete') is True and gate.get('smoke') is True and gate.get('query_records')==72 and
             gate.get('original_service_restored') is True and gate.get('controller_restoration_verified') is True and
             gate.get('resources_restored') is True and gate.get('product_sha256')==file_sha(args.binary) and
             gate.get('baseline_sha256')==BASELINE_SHA,'N8 gate incomplete, not restored, or different product')
    return dict(protocol=PROTOCOL,complete=True,cases=entries,source_collection=str(source),source_pins=SOURCE_PINS,
        configurations=list(CONFIGS),configuration_meaning=dict(A='accepted format_catalog and original render',B='candidate direct render; geometry cache disabled',C='candidate direct render; geometry cache enabled'),
        campaigns=list(campaigns(args)),orders=[dict(campaign=c,order=list(configuration_order(c))) for c in campaigns(args)],
        arms=list(ARMS),expected_queries=expected_rows(args),warmup_per_arm=2,measured_per_arm=10,
        build_gate=build_gate,baseline=dict(path=str(args.baseline_binary),sha256=BASELINE_SHA),
        baseline_acceptance=accepted_baseline(args,entries),render_settings={c:geometry_settings(c) for c in CONFIGS},
        product=dict(path=str(args.binary),sha256=file_sha(args.binary)),smoke=args.smoke,
        smoke_report_sha256=file_sha(args.smoke_report) if gate else None,scientific_publication=False,index_rebuilt=False,new_cases=False,
        fixed_case_geometry='Complete 64x64 non-interpolated grids; B/C both use inline full-grid path, no ROI cache benefit claim',
        mutation='query only on archived-head journal copies; scientific source remains unchanged')


def validate_row(row,configuration,expected_units):
    need(configuration in CONFIGS and row.get('oracle_verified') is True and row.get('mode_verified') is True and
         row.get('execution_mode')=='iotdb-indexed' and row.get('selected_units')==expected_units,'GRIB result not verified')
    discovery=row.get('discovery_call',{});profile=discovery.get('server_profile',{})
    need(row.get('discovery_protocol')=='format_catalog' and profile.get('operation')=='format_catalog' and
         discovery.get('response_path') is None and discovery.get('ipc_bytes')==0 and 'path' not in profile,
         'all configurations must retain the accepted format_catalog discovery')
    values=row.get('value_calls',[]);need(len(values)==expected_units,'per-hit typed value control count differs')
    for call in values:
        p=call['server_profile'];need(p.get('operation')=='typed' and call.get('response_path')==p.get('path') and
             isinstance(p.get('path'),str) and call.get('ipc_bytes',0)>0,'value route differs from original typed path')
        if configuration=='A':continue  # Old A has no new render instrumentation.
        counters=p['counters']
        expected={'grib.render.value_elements':4096,'grib.render.latitude_elements':0,'grib.render.longitude_elements':0,
            'grib.render.double_intermediate_elements':0,'grib.render.full_grid_fast_paths':1,
            'grib.render.value_moved_elements':4096,'grib.render.value_copied_elements':0,
            'grib.geometry_cache.enabled':1 if configuration=='C' else 0}
        # Event-only copied-elements has no entry on the move branch. The
        # applied switch gauge must exist even when disabled (zero).
        observed={key:counters.get(key,0) if value==0 and key!='grib.geometry_cache.enabled' else counters.get(key)
                  for key,value in expected.items()}
        need(all(type(observed[key]) is int and observed[key]==value for key,value in expected.items()),
             'actual render path/geometry switch did not match the full-grid ablation')
        need(all(counters.get('grib.geometry_cache.'+key,0)==0 for key in ('hits','misses','builds','replacements')),
             'full-grid query unexpectedly looked up/built a geometry map')
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
                launch=launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal,config)
                atomic_json(block/'launcher.receipt.json',launch)
                before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                need(before==plan['scientific_head'],'head changed before query')
                print(json.dumps(dict(event='grib_render_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
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
                                runner_sha256=digest(frozen['sources']),engine='format_catalog + GRIB native render ('+config+')'
                                    if arm=='IoTDB-FS' else 'SQLite cross-file + native ecCodes owned values'),
                            clock=dict(kind='client-owned-full-query',d='entry through frozen discovery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                            cache=dict(protocol=PROTOCOL,configuration=config,geometry_settings=geometry_settings(config),native_helper=plan['cfg'] if arm=='FS-index' else None,
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
                                row=grib_catalog_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],lightweight=True)
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
    def stop(sig,frame):raise RuntimeError('GRIB render worker cancelled; preserve evidence')
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
        need(count==expected_rows(args),'GRIB render collection incomplete')
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
        raise RuntimeError('GRIB render supervisor cancelled; restore original service/resources')
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
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.grib_render','--worker']
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


def prepare_only(args,proof):
    require_json_roundtrip(proof,'preflight proof')
    args.work.mkdir(parents=True,mode=0o700,exist_ok=False)
    atomic_json(args.work/'preflight.json',proof)
    run.bootstrap(args)
    sources=closure(args.repo);require_json_roundtrip(sources,'source closure')
    atomic_json(args.work/'preflight-source-closure.json',sources)
    print(json.dumps(dict(complete=True,scientific_queries=0,service_modified=False,resource_modified=False,
        expected_queries=expected_rows(args),source_files=len(sources),smoke=args.smoke)))


def verify_prepared(args,proof):
    require_json_roundtrip(proof,'preflight proof')
    need(proof==read_json(args.work/'preflight.json'),'cross-process preflight proof changed')
    sources=closure(args.repo);require_json_roundtrip(sources,'source closure')
    need(sources==read_json(args.work/'preflight-source-closure.json'),'cross-process source closure changed')
    bootstrap=read_json(args.work/'bootstrap.json')
    need(bootstrap.get('complete') is True and bootstrap.get('native_loaded') is True and
         bootstrap.get('service_modified') is False and bootstrap.get('query_executed') is False,'client dependency gate failed')
    print(json.dumps(dict(complete=True,scientific_queries=0,service_modified=False,resource_modified=False,
        proof_equal=True,closure_equal=True,bootstrap=True,source_files=len(sources))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('repo','work','binary','baseline-binary','build-binding','library','inspector','source-collection'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--smoke-report',type=Path)
    parser.add_argument('--smoke',action='store_true',help='N8 gate only; no automatic formal N128 collection')
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument('--worker',action='store_true');modes.add_argument('--prepare-only',action='store_true')
    modes.add_argument('--verify-prepared',action='store_true',help='Read-only second-process proof/closure/dependency check')
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
        if args.prepare_only:prepare_only(args,proof)
        elif args.verify_prepared:verify_prepared(args,proof)
        else:supervise(args,proof)


if __name__=='__main__':main()
