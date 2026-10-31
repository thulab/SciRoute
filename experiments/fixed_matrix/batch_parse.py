"""Independent HDF5 batch-child parsing A/B on two existing FM1 cases.

Four campaigns AB/BA/BA/AB, five warmups and ten measured pairs per block.
No generation, binding, publication, index rebuild, automatic retry or scale-up.
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
import xml.etree.ElementTree as ET
from types import SimpleNamespace

from . import run, telemetry, client, hdf5_batch_client
from fs_index.native import NativeContext
from .replay import SOURCE_PINS
from .hdf5_cache_batch import verify_build_binding, finish_children, validate_row as validate_hdf5
from .grib_catalog import require_json_roundtrip
from .shared_candidates import native_binding, compare_build_environment
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.hdf5_route3.native_filesystem.fixed_probe import filesystem_launcher
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json

need=run.need
PROTOCOL='iotdbfs-fixed-batch-parse/1'
BASELINE_SHA='f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
CONFIGS=('A','B')
ARMS=('IoTDB-FS','FS-index')
CAMPAIGNS=(0,1,2,3)
WARMUPS=5
MEASURED=10
REPEATS=WARMUPS+MEASURED
PRODUCT_DIFF={'src/query/frontend/QueryRegistrationSocket.cpp'}
REQUIRED_REGRESSION_TESTS=(
    'TypedBatchKeepsStickyBudgetStatusWhenAnExecutorThrows',
    'TypedEntryPointsPreserveTheirValidationErrorOrder',
    'TypedBatchPredeclarationErrorsPrecedeEarlierBindingSemantics',
    'TypedBindingSemanticsRemainAfterAuthorizationInBothEntryPoints',
)
PATH_ARGS=('repo','work','binary','baseline_binary','build_binding','baseline_build_binding',
           'library','inspector','source_collection','current_collection')


def groups(repo,tier):
    need(tier in ('n8','n128'),'unknown fixed tier')
    size=8 if tier=='n8' else 128
    by={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    return [by[f'FM1-hdf5-N{size}-V1-C{size}-S0-base-latest']]


def campaigns(group):return CAMPAIGNS

def configuration_order(campaign):
    need(campaign in CAMPAIGNS,'unknown campaign')
    return CONFIGS if campaign in (0,3) else tuple(reversed(CONFIGS))

def expected_rows(args):return len(groups(args.repo,args.tier))*len(CAMPAIGNS)*len(CONFIGS)*len(ARMS)*REPEATS


def closure(repo):
    result=run.closure(repo)
    for relative in ('experiments/fixed_matrix/tests/test_batch_parse.py',
        'experiments/cross_version/batch_parse/run.sh','experiments/cross_version/batch_parse/README.md'):
        result[relative]=file_sha(repo/relative)
    return result


def verify_candidate(args):
    gates={}
    for label,binary,binding in (('A',args.baseline_binary,args.baseline_build_binding),('B',args.binary,args.build_binding)):
        gate=verify_build_binding(SimpleNamespace(binary=binary,build_binding=binding))
        receipt=read_json(binding)
        for key in ('focused_tests','all_unit_tests'):
            root=ET.parse(receipt[key]).getroot();cases=list(root.iter('testcase'))
            need(len(cases)==int(root.attrib.get('tests',0)) and cases and
                 all(int(n.attrib.get(k,0))==0 for n in root.iter() if n.tag in ('testsuite','testsuites')
                     for k in ('failures','errors','disabled','skipped')) and
                 not any(list(root.iter(k)) for k in ('failure','error','skipped')) and
                 all(n.get('status')=='run' and n.get('result')=='completed' for n in cases),
                 'A/B focused/full XML gate contains skipped or incomplete cases')
            if label=='B':
                for name in REQUIRED_REGRESSION_TESTS:
                    found=[n for n in cases if n.get('classname')=='SocketFixture' and n.get('name')==name]
                    need(len(found)==1 and found[0].get('status')=='run' and found[0].get('result')=='completed',
                         'candidate '+key+' requires one completed SocketFixture.'+name)
        gates[label]=gate
    a,b=read_json(args.baseline_build_binding),read_json(args.build_binding)
    a_sha,b_sha=file_sha(args.baseline_binary),file_sha(args.binary)
    need(a_sha==BASELINE_SHA and b_sha!=a_sha,'baseline differs or candidate is identical')
    product=lambda n:n.startswith(('src/','include/','cmake/')) or n=='CMakeLists.txt'
    names={n for n in a['source_files'] if product(n)}|{n for n in b['source_files'] if product(n)}
    changed={n for n in names if a['source_files'].get(n)!=b['source_files'].get(n)}
    need(changed==PRODUCT_DIFF,'A/B product differs outside approved batch parser change')
    return dict(candidate_sha256=b_sha,baseline_sha256=a_sha,gates=gates,product_files=len(names),
        changed_product_files=sorted(changed),build_environment=compare_build_environment(args.baseline_binary,args.binary))


def validate_n8_gate(gate,baseline_sha,candidate_sha):
    need(gate.get('protocol')==PROTOCOL and gate.get('tier')=='n8' and gate.get('query_records')==240 and
         all(gate.get(k) is True for k in ('complete','scientific_complete','ready_for_analysis','original_service_restored',
             'resources_restored','controller_restoration_verified')) and
         gate.get('product_sha256')==candidate_sha and gate.get('baseline_sha256')==baseline_sha,
         'new N8 qualification incomplete, not restored or different A/B')


def validate_row(row,group,configuration):
    need(configuration in CONFIGS and group['format']=='hdf5','unknown configuration or format')
    validate_hdf5(row,group['K_units'],'C1B1')
    d=row['discovery_call']['server_profile']['counters']
    q=row['value_calls'][0]['server_profile']['counters']
    # The batch frame registers once before the K item registrations.
    need(d.get('registration_attempts')==1 and q.get('registration_attempts')==1+group['K_units'] and
         q.get('typed_batch_items')==group['K_units'] and
         d['registration_attempts']+q['registration_attempts']==2+group['K_units'],
         'registration attempts or typed batch item count differs')
    return row


def source_proof(args):
    source=args.source_collection
    need(source.name=='collection-r2','sealed FM1 collection-r2 required')
    for name,sha in SOURCE_PINS.items():need(file_sha(source/name)==sha,'scientific source changed: '+name)
    need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json')==run.CATALOG_SHA,'fixed catalog changed')
    need(file_sha(args.library)==run.NATIVE_SHA and file_sha(args.inspector)==run.infrastructure.INSPECTOR_SHA,
         'FS helper or head inspector changed')
    build=verify_candidate(args);registry=Registry(source/'registry')
    bindings=read_json(source/'bindings.json');report=read_json(source/'report.json');entries=[]
    for group in groups(args.repo,args.tier):
        gid=group['group_id'];bound=bindings[gid];corpus=registry.get_corpus(bound['corpus_id']);case=registry.get_case(bound['case_id'])
        need(case['corpus_id']==corpus['corpus_id'] and case['expected_matches']==bound['expected_matches'] and
             digest(case['expected_matches'])==bound['oracle_sha256'],'case/oracle identity changed')
        matched=[v for v in report['corpora'] if v['corpus_id']==corpus['corpus_id']]
        need(len(matched)==1,'independent corpus not uniquely bound')
        item=matched[0];base=source/'corpora'/item['corpus_key'];prep=read_json(base/'preparation.json')
        manifest=read_json(base/'input/manifest.json')
        need(prep['complete'] and file_sha(base/'index.sqlite')==item['index_sha256']==prep['index_sha256'] and
             file_sha(base/'input/manifest.json')==item['raw_manifest_sha256']==corpus['source_evidence']['manifest_sha256'] and
             file_sha(base/'publication.json')==prep['publication_receipt_sha256']==corpus['publication_evidence']['receipt_sha256'],
             'index/manifest/publication changed')
        need(read_json(base/'publication.json')['head']==item['head'] and
             run.infrastructure.journal_snapshot(base/'owner')==item['journal_digest'],'archived publication/journal changed')
        entry=dict(group_id=gid,format=group['format'],case_id=case['case_id'],corpus_id=corpus['corpus_id'],
            corpus_key=item['corpus_key'],source_root=str(base),head=item['head'],journal=str(base/'owner'),
            journal_digest=item['journal_digest'],namespace=item['namespace'],
            artifacts={n:file_sha(base/n) for n in ('input/manifest.json','catalog.json','index.sqlite','publication.json','preparation.json')},
            raw_validation=run.verify_raw(manifest,base/'input',base/'fs'))
        if group['format']=='hdf5':entry.update(native_binding(args,corpus['corpus_id'],manifest))
        entries.append(entry)
    gate=None
    if args.tier=='n128':
        need(args.gate_report is not None,'N128 requires completed new N8 gate')
        gate=read_json(args.gate_report)
        validate_n8_gate(gate,BASELINE_SHA,build['candidate_sha256'])
    return dict(protocol=PROTOCOL,complete=True,tier=args.tier,cases=entries,build_gate=build,
        source_collection=str(source),source_pins=SOURCE_PINS,current_collection=str(args.current_collection),
        current_location_files={n:file_sha(args.current_collection/n) for n in ('current-locations.json','report.json')},
        expected_queries=expected_rows(args),configurations=list(CONFIGS),arms=list(ARMS),
        warmup_per_arm=WARMUPS,measured_per_arm=MEASURED,campaigns={g['group_id']:list(campaigns(g)) for g in groups(args.repo,args.tier)},
        gate_report_sha256=file_sha(args.gate_report) if gate else None,
        product=dict(path=str(args.binary),sha256=build['candidate_sha256']),baseline=dict(path=str(args.baseline_binary),sha256=BASELINE_SHA),
        resource_identity=run.infrastructure.resource_identity([2,3]),
        scientific_publication=False,index_rebuilt=False,new_cases=False,location_bindings_created=False)



def plans(args,registry,report,proof):
    original=read_json(args.source_collection/'report.json');oldbind=read_json(args.source_collection/'bindings.json')
    source_registry=Registry(args.source_collection/'registry');selected={g['group_id']:g for g in groups(args.repo,args.tier)}
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
            native_root=Path(entry['native_root']),
            prep=read_json(base/'preparation.json'),manifest=read_json(base/'input/manifest.json'),
            cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)))
    return result,bindings



def launcher(path,binary,sha,namespace,journal,plan):
    receipt=filesystem_launcher(path,binary,namespace,journal,False,native_root=plan['native_root'],expected_sha=sha)
    declaration='SETTINGS='+repr(receipt['settings'])+'\n';text=path.read_text()
    need(text.count(declaration)==1,'launcher settings layout differs')
    settings=dict(receipt['settings'],**{'query.hdf5.native_cache':'true'})
    path.write_text(text.replace(declaration,'SETTINGS='+repr(settings)+'\n',1))
    receipt.update(settings=settings,sha256=file_sha(path));return receipt



def collect(args,execution,bindings,registry,frozen,report,env,paired):
    paths=read_json(args.work/'resource-paths.json');count=0
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    for campaign in CAMPAIGNS:
        for ordinal,plan in enumerate(execution):
            group=plan['group'];gid=group['group_id'];b=bindings[gid]
            if campaign not in campaigns(group):continue
            for config in configuration_order(campaign):
                block=plan['execution_root']/('campaign-'+str(campaign))/config;block.mkdir(parents=True)
                journal=block/'owner';shutil.copytree(plan['journal'],journal)
                need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'private journal copy differs')
                binary=args.baseline_binary if config=='A' else args.binary;sha=file_sha(binary)
                launch=launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal,plan)
                atomic_json(block/'launcher.receipt.json',launch)
                before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                need(before==plan['scientific_head'],'head changed before mount')
                print(json.dumps(dict(event='batch_parse_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
                with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                    need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha,'running binary differs')
                    actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual settings differ')
                    native=stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                    environment=telemetry.environment(args.repo,artifact_paths=[args.library,binary],process_roles=dict(client=os.getpid(),
                        daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                    atomic_json(block/'environment.json',environment)
                    mount=dict(campaign=campaign,configuration=config,daemon_pid=context['pid'],startup_ns=context['startup_ns'],
                        product_sha256=sha,environment_sha256=file_sha(block/'environment.json'));plan['item']['mounts'].append(mount)
                    atomic_json(block/'query-plan.json',native.explain('index',b['native_selection'],client.PREDICATE,b['scope']))
                    for arm in ARMS:
                        model=build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                            implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                runner_sha256=digest(frozen['sources']),engine='SQLite index + native HDF5 owned values' if arm=='FS-index' else 'native-hdf5 cache+batch'),
                            clock=dict(kind='client-owned-full-query',d='entry through discovery delivery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                            cache=dict(protocol=PROTOCOL,configuration=config,native_helper=plan['cfg'] if arm=='FS-index' else None,
                                daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=True),
                            environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=WARMUPS,measured=MEASURED,schedule='AB/BA/BA/AB; paired alternating arms; parity repeat+campaign+configuration'))
                        bound=registry.register_run(model);b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    for repeat in range(REPEATS):
                        order=ARMS if (repeat+campaign+ordinal+CONFIGS.index(config))%2==0 else tuple(reversed(ARMS))
                        for arm in order:
                            rb=telemetry.sample(paths);run.validate_sample(rb)
                            if arm=='IoTDB-FS':
                                row=hdf5_batch_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],batch=True)
                                validate_row(row,group,config)
                            else:row=client.query_native(native,arm,group,b['scope'],b['native_selection'],b['expected_matches'])
                            ra=telemetry.sample(paths);run.validate_sample(ra)
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][config][arm],campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,resource_id=frozen['resource_id'],resource_before=rb,
                                resource_after=ra,environment_sha256=mount['environment_sha256'],product_sha256=sha)
                            run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                        atomic_json(args.work/'report.json',report)
                after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'query changed head/journal')
                need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed FS-index')
                mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True);atomic_json(args.work/'report.json',report)
    return count



def analyze(path,bindings,selected,resource_id,candidate_sha):
    rows=[json.loads(line) for line in Path(path).read_text().splitlines()]
    wanted={(g['group_id'],c,f,a,r) for g in selected for c in campaigns(g) for f in CONFIGS for a in ARMS for r in range(REPEATS)}
    keys={(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows}
    need(len(rows)==len(wanted) and keys==wanted,'all preregistered rows required; no exclusions')
    gm={g['group_id']:g for g in selected};resources={r.get('resource_id') for r in rows}
    need(bool(resource_id) and resources=={resource_id},'mixed or unbound resource identity')
    for row in rows:
        b=bindings[row['group_id']];group=gm[row['group_id']];config=row['configuration']
        need(all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
             row['run_id']==b['run_ids'][str(row['campaign'])][config][row['arm']],'scientific or run identity differs')
        need(row.get('product_sha256')==(BASELINE_SHA if config=='A' else candidate_sha),'A/B product differs')
        need(row.get('ok') is True and row.get('oracle_verified') is True and row.get('mode_verified') is True and
             row.get('selected_units')==group['K_units'] and row.get('owned_output_bytes')==group['owned_output_bytes'],
             'unverified owned query output')
        need(row['phase']==('warmup' if row['repeat']<WARMUPS else 'measurement'),'phase differs')
        need(all(type(row[k]) is int and row[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
             row['total_ns']==row['discovery_ns']+row['value_ns'],'full query clock differs')
        for side in ('resource_before','resource_after'):run.validate_sample(row[side])
        need(row['resource_after']['monotonic_ns']>row['resource_before']['monotonic_ns'],'resource clock reversed')
        if row['arm']=='IoTDB-FS':validate_row(row,group,config)
    cells=[];effects=[]
    for group in selected:
        gid=group['group_id'];by_config={}
        for config in CONFIGS:
            blocks=[]
            for campaign in campaigns(group):
                arms={a:{k:statistics.median(r[k]/1e6 for r in rows if r['group_id']==gid and r['configuration']==config and
                    r['campaign']==campaign and r['arm']==a and r['phase']=='measurement')
                    for k in ('total_ns','discovery_ns','value_ns')} for a in ARMS}
                blocks.append(dict(campaign=campaign,warm_median_ms=arms,**{
                    name:arms['IoTDB-FS'][key]/arms['FS-index'][key] for name,key in
                    (('R','total_ns'),('Rd','discovery_ns'),('Rq','value_ns'))}))
            cell=dict(group_id=gid,configuration=config,corpus_id=bindings[gid]['corpus_id'],case_id=bindings[gid]['case_id'],
                blocks=blocks,**{k:statistics.median(b[k] for b in blocks) for k in ('R','Rd','Rq')})
            cells.append(cell);by_config[config]=cell
        comparisons=[]
        for a,b in zip(by_config['A']['blocks'],by_config['B']['blocks']):
            comparisons.append(dict(campaign=a['campaign'],**{k+'_B_over_A':b[k]/a[k] for k in ('R','Rd','Rq')},
                raw_IoTDB_T_B_over_A=b['warm_median_ms']['IoTDB-FS']['total_ns']/a['warm_median_ms']['IoTDB-FS']['total_ns'],
                raw_FS_T_B_over_A=b['warm_median_ms']['FS-index']['total_ns']/a['warm_median_ms']['FS-index']['total_ns']))
        effects.append(dict(group_id=gid,blocks=comparisons,**{
            k:statistics.median(v[k] for v in comparisons) for k in comparisons[0] if k!='campaign'}))
    return dict(protocol=PROTOCOL,complete=True,query_rows=len(rows),cells=cells,effects=effects,
        statistic='Within-block arm medians; paired B/A per campaign; no historical substitution or slow-value exclusions.',
        limitation='Sequential warm queries only; no cold-disk or concurrency claim.')



def worker(args):
    def stop(sig,frame):raise RuntimeError('batch-parse worker cancelled; preserve evidence')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
    proof=source_proof(args);frozen=read_json(args.work/'prepared.json')
    need(proof==read_json(args.work/'reuse-source.json') and closure(args.repo)==frozen['sources'],'preflight changed')
    paired,_,env,startup=run.dependencies(args.repo);startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    report=dict(protocol=PROTOCOL,complete=False,tier=args.tier,query_records=0,expected_queries=expected_rows(args),corpora=[],
        product_sha256=file_sha(args.binary),baseline_sha256=BASELINE_SHA,resource_id=frozen['resource_id'],
        scientific_publication=False,index_rebuilt=False,new_cases=False,location_bindings_created=False,credentials_recorded=False)
    atomic_json(args.work/'report.json',report)
    try:
        registry=Registry(args.work/'registry');execution,bindings=plans(args,registry,report,proof)
        atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),arms=list(ARMS),
            mounts=[dict(group=g['group_id'],campaign=c,order=list(configuration_order(c)))
                for c in CAMPAIGNS for g in groups(args.repo,args.tier) if c in campaigns(g)],fresh_mount_and_FS_context=True))
        count=collect(args,execution,bindings,registry,frozen,report,env,paired)
        need(count==expected_rows(args),'batch-parse A/B collection incomplete')
        need(source_proof(args)==proof and closure(args.repo)==frozen['sources'],'source/binary/scientific inputs changed')
        for plan in execution:plan['item'].update(complete=True,final_raw_validation=run.verify_raw(plan['manifest'],plan['root']/'input',plan['root']/'fs'))
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
        resource_policy=proof['resource_identity'],
        inspector=str(args.inspector),resource_id=digest(dict(resources=run.infrastructure.resource_identity([2,3]),
            environment_sha256=file_sha(args.work/'environment.json'))))
    atomic_json(args.work/'prepared.json',prepared)
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    process=sampler=None;service={};failure=None;handlers={}
    def cancelled(sig,frame):
        if process is not None and process.poll() is None:process.terminate()
        raise RuntimeError('batch-parse supervisor cancelled; restore original service/resources')
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
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.batch_parse','--worker']
                    for key in PATH_ARGS:
                        command.extend(['--'+key.replace('_','-'),str(getattr(args,key))])
                    command.extend(['--tier',args.tier])
                    if args.gate_report:command.extend(['--gate-report',str(args.gate_report)])
                    with (args.work/'worker.stdout').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=args.repo,env=run.worker_environment(),stdin=subprocess.DEVNULL,
                            stdout=out,stderr=err,start_new_session=True)
                        manager.attach_query(process.pid);started=time.monotonic()
                        while process.poll() is None:
                            need(sampler.poll() is None,'resource sampler stopped')
                            run.infrastructure.observe(manager.paths)
                            need(time.monotonic()-started<=12600,'batch-parse worker hard timeout')
                            time.sleep(5)
                    need(process.returncode==0,'batch-parse worker failed; preserve evidence, no automatic retry')
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
        summary=analyze(args.work/'queries.jsonl',read_json(args.work/'bindings.json'),groups(args.repo,args.tier),prepared['resource_id'],prepared['product']['sha256'])
        atomic_json(args.work/'analysis.json',summary)
        report.update(complete=True,resource_coverage=coverage,analysis_sha256=file_sha(args.work/'analysis.json'))
    except BaseException as error:
        report['analysis_failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=True,queries=expected_rows(args),report=str(args.work/'report.json'))))



def prepare_only(args,proof):
    require_json_roundtrip(proof,'preflight proof')
    args.work.mkdir(parents=True,mode=0o700,exist_ok=False);atomic_json(args.work/'preflight.json',proof)
    run.bootstrap(args)
    sources=closure(args.repo);require_json_roundtrip(sources,'source closure')
    atomic_json(args.work/'preflight-source-closure.json',sources)
    print(json.dumps(dict(complete=True,scientific_queries=0,service_modified=False,resource_modified=False,
        expected_queries=expected_rows(args),source_files=len(sources),tier=args.tier)))



def verify_prepared(args,proof):
    require_json_roundtrip(proof,'preflight proof')
    need(proof==read_json(args.work/'preflight.json'),'cross-process scientific/build proof changed')
    sources=closure(args.repo);require_json_roundtrip(sources,'source closure')
    need(sources==read_json(args.work/'preflight-source-closure.json'),'cross-process execution closure changed')
    bootstrap=read_json(args.work/'bootstrap.json')
    need(all(bootstrap.get(k) is True for k in ('complete','native_loaded')) and
         all(bootstrap.get(k) is False for k in ('service_modified','query_executed')),'client bootstrap failed')
    print(json.dumps(dict(complete=True,scientific_queries=0,service_modified=False,resource_modified=False,
        proof_equal=True,closure_equal=True,bootstrap=True,source_files=len(sources))))



def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in PATH_ARGS:parser.add_argument('--'+key.replace('_','-'),type=Path,required=True)
    parser.add_argument('--tier',choices=('n8','n128'),required=True)
    parser.add_argument('--gate-report',type=Path)
    modes=parser.add_mutually_exclusive_group();modes.add_argument('--worker',action='store_true')
    modes.add_argument('--prepare-only',action='store_true');modes.add_argument('--verify-prepared',action='store_true')
    args=parser.parse_args()
    for key in PATH_ARGS:
        if key!='work':setattr(args,key,getattr(args,key).resolve(strict=True))
    args.work=args.work.resolve()
    if args.gate_report:args.gate_report=args.gate_report.resolve(strict=True)
    for source in (args.source_collection,args.current_collection):
        need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work),'run/source paths overlap')
    if args.worker:worker(args)
    else:
        proof=source_proof(args);require_json_roundtrip(proof,'preflight proof')
        if args.prepare_only:prepare_only(args,proof)
        elif args.verify_prepared:verify_prepared(args,proof)
        else:supervise(args,proof)



if __name__=='__main__':main()
