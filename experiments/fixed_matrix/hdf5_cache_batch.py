"""Approved native HDF5 2x2 ablation on three sealed FM1 cases.

No generation, scientific publication, new location binding or index rebuild.
The smoke run is a separate N8 campaign; formal collection always has 864 rows.
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

from . import run, telemetry, hdf5_batch_client
from fs_index.native import NativeContext
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.hdf5_route3.native_filesystem import fixed_probe
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json

need = run.need
PROTOCOL = 'iotdbfs-fixed-hdf5-cache-batch/1'
GROUPS = ('FM1-hdf5-N8-V1-C8-S0-base-latest', 'FM1-hdf5-N128-V1-C128-S0-base-latest',
          'FM1-hdf5-N8-V8-C8-S0-identical-all-filtered')
CONFIGS = ('C0B0', 'C1B0', 'C0B1', 'C1B1')
ARMS = ('IoTDB-FS', 'FS-index')
CURRENT_PINS = {'prepared.json':'ac73808d23d86700f6f76ce8374ff13b931978617c3a1b07822328779e23098c',
    'current-locations.json':'21c3669fa6162c79f8aaeba7a44ad118f407c7508f02f220c046d9d7268d16e6',
    'report.json':'b0db602350e4f4b83448d01424b97806a03d7aace08e44c95985551ba0fdb526',
    'analysis.json':'aa9f5b2c3b7d788307be5fac6376b0b48ed84a11f965a7411a5b848c6fa99ed3'}


def groups(repo, smoke=False):
    catalog = {g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    return [catalog[g] for g in (GROUPS[:1] if smoke else GROUPS)]


def campaigns(args): return (0,) if args.smoke else (0, 1, 2)
def expected_rows(args): return len(groups(args.repo, args.smoke))*len(campaigns(args))*len(CONFIGS)*len(ARMS)*12


def configuration_order(campaign, corpus_index):
    # Fixed before measurements: rotations balance early/late positions over the
    # three corpora and campaigns, with a preregistered Latin-square traversal.
    offset = (campaign + corpus_index) % 4
    return tuple(CONFIGS[(i+offset)%4] for i in (0,1,3,2))


def closure(repo):
    result = run.closure(repo)
    relative = 'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py'
    result[relative] = file_sha(repo/relative)
    return result


def verify_build_binding(args):
    """Verify the independently built source snapshot and both XML gates."""
    receipt=read_json(args.build_binding)
    need(receipt.get('complete') is True and receipt.get('binary_sha256')==file_sha(args.binary),
         'candidate build is not complete or binary changed')
    need(Path(receipt.get('binary','')).is_absolute() and Path(receipt['binary']).resolve(strict=True)==args.binary,
         'candidate binary path differs')
    source_root=Path(receipt.get('source_root',''))
    need(source_root.is_absolute() and source_root.is_dir() and not source_root.is_symlink(),'independent build source root missing')
    source_root=source_root.resolve(strict=True);sources=receipt.get('source_files',{})
    need(isinstance(sources,dict) and sources,'build source closure missing')
    need({str(p.relative_to(source_root)) for p in source_root.rglob('*') if p.is_file()}==set(sources),
         'build source manifest does not enumerate exact file closure')
    for name,sha in sources.items():
        relative=Path(name)
        need(not relative.is_absolute() and '..' not in relative.parts,'unsafe build source path')
        source=source_root/relative
        need(not source.is_symlink() and source.resolve(strict=True).is_relative_to(source_root)
             and file_sha(source)==sha,'build source changed: '+name)
    gates={}
    for label in ('focused_tests','all_unit_tests'):
        path=Path(receipt.get(label,''))
        need(path.is_absolute() and not path.is_symlink() and file_sha(path)==receipt.get(label+'_sha256'),
             'test artifact changed: '+label)
        tree=ET.parse(path);root=tree.getroot()
        need(root.tag in ('testsuites','testsuite') and int(root.attrib.get('tests','0'))>0,'test gate has no tests')
        need(all(int(node.attrib.get(key,'0'))==0 for node in root.iter() if node.tag in ('testsuites','testsuite')
                 for key in ('failures','errors')) and not list(root.iter('failure')) and not list(root.iter('error')),
             'test gate has failures/errors: '+label)
        gates[label]=dict(path=str(path),sha256=file_sha(path),tests=int(root.attrib['tests']))
    unit=Path(receipt.get('unit_binary',''))
    need(unit.is_absolute() and not unit.is_symlink() and file_sha(unit)==receipt.get('unit_binary_sha256'),
         'unit binary changed or path missing')
    return dict(complete=True,receipt_sha256=file_sha(args.build_binding),source_root=str(source_root),
                source_files=len(sources),tests=gates,unit_binary_sha256=file_sha(unit))


def preflight(args):
    build_gate=verify_build_binding(args)
    proof = fixed_probe.source_proof(args, group_ids=[g['group_id'] for g in groups(args.repo,args.smoke)], campaigns=campaigns(args))
    for name, sha in CURRENT_PINS.items():
        need(file_sha(args.current_collection/name) == sha, 'accepted current evidence changed: '+name)
    report = read_json(args.current_collection/'report.json')
    need(report['complete'] and report['original_service_restored'] and report['resources_restored'], 'previous collection incomplete')
    current = read_json(args.current_collection/'current-locations.json')
    locations = {x['corpus_id']:x for x in current['corpora']}
    old = {x['corpus_id']:x for x in report['corpora']}
    bound = []
    for entry in proof['cases']:
        cid = entry['corpus_id']; location = locations[cid]; item = old[cid]
        journal = Path(location['active_owner_journal']); root = Path(location['native_file_root'])
        need(location['namespace'] == item['namespace'] and str(journal)==item['active_owner_journal']
             and str(root)==item['native_file_root'] and root.is_dir() and not root.is_symlink(), 'current location missing')
        need(run.infrastructure.journal_snapshot(journal) == item['journal_digest'], 'current journal changed')
        receipt_path = Path(location['location_receipt'])
        need(str(receipt_path)==item['file_binding_receipt'] and file_sha(receipt_path)==item['file_binding_receipt_sha256'],
             'accepted native location receipt changed')
        receipt = read_json(receipt_path)
        need(receipt['complete'] and receipt['scientific_identities_unchanged'] and receipt['head_after'] == item['head']
             and receipt['active_journal_digest']==item['journal_digest'], 'location proof differs')
        bound.append(dict(corpus_id=cid, namespace=item['namespace'], journal=str(journal),
            journal_digest=item['journal_digest'], head=item['head'], native_root=str(root),
            receipt=str(receipt_path),receipt_sha256=file_sha(receipt_path)))
    proof.update(protocol=PROTOCOL, expected_queries=expected_rows(args), configurations=list(CONFIGS), arms=list(ARMS),
        current_collection=str(args.current_collection), current_pins=CURRENT_PINS, existing_bindings=bound,
        source_files=closure(args.repo), build_gate=build_gate, location_bindings_created=False, smoke=args.smoke,
        mutation='query only; no native-file binding, publication or index rebuild')
    return proof


def plans(args, registry, report, proof):
    old = {x['corpus_id']:x for x in read_json(args.source_collection/'report.json')['corpora']}
    old_bindings = read_json(args.source_collection/'bindings.json'); prior = {x['corpus_id']:x for x in proof['existing_bindings']}
    source_registry = Registry(args.source_collection/'registry'); selected = {g['group_id']:g for g in groups(args.repo,args.smoke)}
    result=[]; bindings={}
    for entry in proof['cases']:
        gid=entry['group_id']; cid=entry['corpus_id']; base=Path(entry['source_root']); current=prior[cid]
        corpus=registry.register_corpus(source_registry.get_corpus(cid)); registry.register_case(source_registry.get_case(entry['case_id']))
        bindings[gid]=deepcopy(old_bindings[gid]);bindings[gid]['run_ids']={}
        execution=args.work/'corpora'/entry['corpus_key']; execution.mkdir(parents=True,mode=0o700)
        journal=execution/'owner-active';shutil.copytree(current['journal'],journal)
        need(run.infrastructure.journal_snapshot(journal)==current['journal_digest'],'current journal copy differs')
        item=deepcopy(old[cid]);item.update(complete=False,mounts=[],source_root=str(base),
            active_owner_journal=str(journal),head=current['head'],journal_digest=current['journal_digest'],
            native_file_root=current['native_root'],reused_binding_receipt=current['receipt'])
        report['corpora'].append(item)
        result.append(dict(spec=corpus['parameters']['generation']['specification'],root=base,execution_root=execution,
            manifest=read_json(base/'input/manifest.json'),fsroot=base/'fs',journal=journal,item=item,
            scientific_head=current['head'],journal_digest=current['journal_digest'],corpus=corpus,groups=[selected[gid]],
            native_root=Path(current['native_root']),cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144),
            prep=read_json(base/'preparation.json')))
    return result,bindings


def launcher(path,args,plan,config):
    receipt=fixed_probe.filesystem_launcher(path,args.binary,plan['item']['namespace'],plan['journal'],False,
        native_root=plan['native_root'],expected_sha=file_sha(args.binary))
    old='SETTINGS='+repr(receipt['settings'])+'\n';settings=dict(receipt['settings'],**{'query.hdf5.native_cache':'true' if config[1]=='1' else 'false'})
    text=path.read_text();need(text.count(old)==1,'launcher layout changed');path.write_text(text.replace(old,'SETTINGS='+repr(settings)+'\n',1))
    receipt.update(settings=settings,sha256=file_sha(path),ablation=config)
    atomic_json(path.with_name(path.name+'.receipt.json'),receipt);return receipt


def validate_row(row, expected_units, config):
    need(row.get('selected_units')==expected_units and row.get('oracle_verified') is True
        and row.get('mode_verified') is True and row.get('execution_mode')=='native-hdf5','result not verified')
    batch=config[3]=='1';cached=config[1]=='1';calls=row.get('value_calls',[])
    need(len(calls)==(1 if batch else expected_units) and row.get('batch_enabled')==batch,'value control count differs')
    for call in calls:
        p=call['server_profile'];c=p['counters'];mode=p.get('server_hdf5_execution',{})
        need(mode.get('mode')=='native-hdf5' and mode.get('payload_backend')=='filesystem','wrong native backend')
        need(type(mode.get('native_cache_enabled')) is bool and mode['native_cache_enabled']==cached
             and bool(c.get('hdf5.native.session.cache.enabled',0))==cached,'actual native session policy differs')
        need(c.get('hdf5.native.slabs',0)>0 and not p['iotdb_delta'].get('payload_calls',0),'no native slab or database payload fallback')
        for key in ('payload_map.block_reads','payload_map.data_batches','payload_map.hash_bytes','hdf5.selection.decoder_compiles','hdf5.layout.pages.calls'):
            need(not c.get(key,0),'forbidden native work: '+key)
        local=c.get('hdf5.native.filesystem.calls',0);source=c.get('hdf5.native.source.calls',0)
        need(local==source and c.get('hdf5.native.filesystem.bytes',0)==c.get('hdf5.native.source.bytes',0),'source not exclusively filesystem')
        if not local:
            need(cached and c.get('hdf5.native.session.hits',0)>0 and c.get('hdf5.native.session.rebinds',0)>0
                and c.get('hdf5.native.session.validations',0)>=2
                and c.get('hdf5.native.logical.calls',0)>0 and c.get('hdf5.native.logical.bytes',0)>0
                and c.get('hdf5.native.logical.replays',0)>0,'zero IO without fresh validated and budgeted cache hit')
        else:need(c.get('hdf5.native.filesystem.bytes',0)>0,'filesystem read has no bytes')
        if batch:need(p.get('operation')=='typed_batch' and call.get('batch_items')==expected_units,'batch profile/item count differs')
    return row


def collect(args,execution,bindings,registry,frozen,report,env,paired):
    from . import client
    paths=read_json(args.work/'resource-paths.json');count=0;binary_sha=file_sha(args.binary)
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    for campaign in campaigns(args):
        for corpus_index,plan in enumerate(execution):
            for config in configuration_order(campaign,corpus_index):
                group=plan['groups'][0];gid=group['group_id'];b=bindings[gid]
                block=plan['execution_root']/('campaign-'+str(campaign))/config;block.mkdir(parents=True)
                launch=launcher(block/'launcher',args,plan,config)
                before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                need(before==plan['scientific_head'],'head changed before mount')
                print(json.dumps(dict(event='ablation_mount',campaign=campaign,group=gid,config=config)),flush=True)
                with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                    need(file_sha(Path('/proc')/str(context['pid'])/'exe')==binary_sha,'running product differs')
                    actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual cache settings differ')
                    native=stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                    environment=telemetry.environment(args.repo,artifact_paths=[args.library,args.binary],
                        process_roles=dict(client=os.getpid(),daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                    atomic_json(block/'environment.json',environment)
                    mount=dict(campaign=campaign,configuration=config,daemon_pid=context['pid'],startup_ns=context['startup_ns'],environment_sha256=file_sha(block/'environment.json'))
                    plan['item']['mounts'].append(mount)
                    atomic_json(block/'query-plan.json',native.explain('index',b['native_selection'],client.PREDICATE,b['scope']))
                    for arm in ARMS:
                        bound=registry.register_run(build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                            implementation=dict(binary_sha256=binary_sha if arm=='IoTDB-FS' else frozen['native_library_sha256'],runner_sha256=digest(frozen['sources']),engine='native-hdf5'),
                            clock=dict(kind='client-owned-full-query',d='entry through frozen discovery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                            cache=dict(protocol=PROTOCOL,configuration=config,native_helper=plan['cfg'] if arm=='FS-index' else None,
                                daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=config[3]=='1',
                                C0_policy='original metadata 8 MiB/raw 0',C1_policy='8 files/8 datasets, metadata 2 MiB/raw 256 KiB, 64 MiB pool'),
                            environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=2,measured=10,schedule='alternating paired arms; parity campaign+corpus+config')))
                        b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    for repeat in range(12):
                        order=ARMS if (repeat+campaign+corpus_index+CONFIGS.index(config))%2==0 else tuple(reversed(ARMS))
                        for arm in order:
                            rb=telemetry.sample(paths);run.validate_sample(rb)
                            if arm=='IoTDB-FS':
                                row=hdf5_batch_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],batch=config[3]=='1')
                                validate_row(row,group['K_units'],config)
                            else:row=client.query_native(native,arm,group,b['scope'],b['native_selection'],b['expected_matches'])
                            ra=telemetry.sample(paths);run.validate_sample(ra)
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][config][arm],campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                phase='warmup' if repeat<2 else 'measurement',ok=True,resource_id=frozen['resource_id'],
                                resource_before=rb,resource_after=ra,environment_sha256=mount['environment_sha256'])
                            run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                        atomic_json(args.work/'report.json',report)
                after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                need(after==before and run.infrastructure.journal_snapshot(plan['journal'])==plan['journal_digest'],'query changed head/journal')
                need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed FS index')
                mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
                atomic_json(args.work/'report.json',report)
    return count


def analyze(path,bindings,selected,campaign_ids):
    rows=[json.loads(s) for s in Path(path).read_text().splitlines()]
    wanted={(g['group_id'],c,f,a,r) for g in selected for c in campaign_ids for f in CONFIGS for a in ARMS for r in range(12)}
    keys={(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows}
    need(len(rows)==len(wanted) and keys==wanted,'complete ablation rows required; no latency exclusions')
    by_group={g['group_id']:g for g in selected}
    for row in rows:
        b=bindings[row['group_id']];config=row['configuration']
        need(all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')),'case identity differs')
        need(row['run_id']==b['run_ids'][str(row['campaign'])][config][row['arm']],'run identity differs')
        need(row['ok'] is True and row['oracle_verified'] is True and row['mode_verified'] is True,'unverified query')
        need(row['phase']==('warmup' if row['repeat']<2 else 'measurement'),'phase differs')
        need(all(type(row[k]) is int and row[k]>0 for k in ('total_ns','discovery_ns','value_ns'))
            and row['total_ns']==row['discovery_ns']+row['value_ns'],'full query clock differs')
        for side in ('resource_before','resource_after'):run.validate_sample(row[side])
        if row['arm']=='IoTDB-FS':validate_row(row,by_group[row['group_id']]['K_units'],config)
    cells=[]
    for group in selected:
        for config in CONFIGS:
            blocks=[]
            for campaign in campaign_ids:
                arms={arm:{k:statistics.median(r[k] for r in rows if r['group_id']==group['group_id'] and r['campaign']==campaign
                    and r['configuration']==config and r['arm']==arm and r['phase']=='measurement')/1e6
                    for k in ('total_ns','discovery_ns','value_ns')} for arm in ARMS}
                blocks.append(dict(campaign=campaign,warm_median_ms=arms,**{ratio:arms['IoTDB-FS'][key]/arms['FS-index'][key]
                    for ratio,key in (('R','total_ns'),('Rd','discovery_ns'),('Rq','value_ns'))}))
            cells.append(dict(group_id=group['group_id'],table_id=group['table_id'],configuration=config,blocks=blocks,
                warm_median_ms={a:{k:statistics.median(b['warm_median_ms'][a][k] for b in blocks)
                    for k in ('total_ns','discovery_ns','value_ns')} for a in ARMS},
                **{k:statistics.median(b[k] for b in blocks) for k in ('R','Rd','Rq')},
                R_range=[min(b['R'] for b in blocks),max(b['R'] for b in blocks)]))
    effects=[]
    cell_map={(c['group_id'],c['configuration']):c for c in cells}
    for group in selected:
        gid=group['group_id']
        for label,left,right in (('cache_at_B0','C1B0','C0B0'),('cache_at_B1','C1B1','C0B1'),
                                 ('batch_at_C0','C0B1','C0B0'),('batch_at_C1','C1B1','C1B0'),
                                 ('combined','C1B1','C0B0')):
            blocks=[]
            for l,r in zip(cell_map[(gid,left)]['blocks'],cell_map[(gid,right)]['blocks']):
                blocks.append(dict(campaign=l['campaign'],normalized_R_ratio=l['R']/r['R'],
                    raw_IoTDB_T_ratio=l['warm_median_ms']['IoTDB-FS']['total_ns']/r['warm_median_ms']['IoTDB-FS']['total_ns']))
            effects.append(dict(group_id=gid,effect=label,numerator=left,denominator=right,blocks=blocks,
                normalized_R_ratio=statistics.median(b['normalized_R_ratio'] for b in blocks),
                range=[min(b['normalized_R_ratio'] for b in blocks),max(b['normalized_R_ratio'] for b in blocks)]))
    return dict(complete=True,protocol=PROTOCOL,query_rows=len(rows),campaigns=list(campaign_ids),cells=cells,effects=effects,
        statistic='Median of campaign warm medians; ratios formed against same-block FS-index. All samples retained.',
        limitation='Descriptive serial warm-query ablation; no concurrency or cold-disk claim.')


def worker(args):
    def stop(sig,frame):raise RuntimeError('ablation cancelled; preserve evidence')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
    proof=preflight(args);frozen=read_json(args.work/'prepared.json')
    need(proof==read_json(args.work/'reuse-source.json') and closure(args.repo)==frozen['sources'],'frozen preflight changed')
    paired,_,env,startup=run.dependencies(args.repo);startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    report=dict(protocol=PROTOCOL,complete=False,query_records=0,expected_queries=expected_rows(args),corpora=[],
        product_sha256=file_sha(args.binary),resource_id=frozen['resource_id'],scientific_publication=False,
        index_rebuilt=False,new_cases=False,location_bindings_created=False,smoke=args.smoke,credentials_recorded=False)
    atomic_json(args.work/'report.json',report)
    try:
        registry=Registry(args.work/'registry');execution,bindings=plans(args,registry,report,proof)
        atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),arms=list(ARMS),
            mounts=[dict(campaign=c,corpus=p['spec']['corpus_key'],order=configuration_order(c,i))
                for c in campaigns(args) for i,p in enumerate(execution)],fresh_mount_and_FS_context_per_configuration=True))
        count=collect(args,execution,bindings,registry,frozen,report,env,paired)
        need(count==expected_rows(args),'ablation incomplete')
        need(preflight(args)==proof and closure(args.repo)==frozen['sources'],'frozen source changed')
        for p in execution:p['item'].update(complete=True,final_raw_validation=run.verify_raw(p['manifest'],p['root']/'input',p['fsroot']))
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
    except BaseException as error:
        report['failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:atomic_json(args.work/'report.json',report)


def finish_children(process,manager,work):
    # Configurations add one level beneath campaign; the original cleanup glob
    # predates it. Stop the exact worker first, then unmount only owned roots.
    if process is not None and process.poll() is None:
        process.terminate()
        try:process.wait(timeout=120)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
    try:
        for path in work.glob('corpora/*/campaign-*/*/mount/mount'):
            if os.path.ismount(path):
                subprocess.run(['fusermount','-uz',str(path)],stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=30)
    finally:run.finish_children(process,manager,work)


def supervise(args, proof):
    args.work.mkdir(parents=True, mode=0o700, exist_ok=False)
    atomic_json(args.work / 'reuse-source.json', proof)
    run.bootstrap(args)  # Client/product Arrow environments stay separate.
    sources = closure(args.repo)
    for relative, expected in sources.items():
        target = args.work/'source'/relative; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.repo/relative, target); need(file_sha(target) == expected, 'source copy differs')
    paired, _, args.env, startup = run.dependencies(args.repo)
    args.runtime = args.repo.parent/'runtime'; args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text()); args.report=args.work/'service-report.json'
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(x in argv for x in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')), 'database memory differs')
    software=run.software_inventory(args.repo); atomic_json(args.work/'software.json', software)
    environment=telemetry.environment(args.repo, artifact_paths=[args.binary,args.library,args.inspector],
        process_roles=software['process_roles'],data_path=args.work)
    atomic_json(args.work/'environment.json', environment)
    atomic_json(args.work/'support-roles.json',{k:v for k,v in software['process_roles'].items() if k!='datanode'})
    prepared=dict(protocol=PROTOCOL, sources=sources, product=dict(path=str(args.binary),sha256=file_sha(args.binary)),
        native_library_sha256=run.NATIVE_SHA, inspector=str(args.inspector),
        resource_id=digest(dict(resources=run.infrastructure.resource_identity([2,3]),environment_sha256=file_sha(args.work/'environment.json'))))
    atomic_json(args.work/'prepared.json',prepared)
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    process=sampler=None; service={}; failure=None; handlers={}
    def cancelled(sig,frame):
        if process and process.poll() is None: process.terminate()
        raise RuntimeError('fixed HDF5 supervisor cancelled; restore service/resources')
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):handlers[sig]=signal.signal(sig,cancelled)
    try:
        manager.create(); atomic_json(args.work/'resource-before.json',manager.snapshot())
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
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.hdf5_cache_batch','--worker']
                    for key in ('repo','work','binary','build_binding','library','inspector','source_collection','current_collection'):
                        command.extend(['--'+key.replace('_','-'),str(getattr(args,key))])
                    if args.smoke:command.append('--smoke')
                    with (args.work/'worker.stdout').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=args.repo,env=run.worker_environment(),stdin=subprocess.DEVNULL,
                            stdout=out,stderr=err,start_new_session=True)
                        manager.attach_query(process.pid); started=time.monotonic()
                        while process.poll() is None:
                            run.need(sampler.poll() is None,'resource sampler stopped')
                            run.infrastructure.observe(manager.paths)
                            run.need(time.monotonic()-started <= 12600,'fixed HDF5 worker hard timeout')
                            time.sleep(5)
                    need(process.returncode==0,'HDF5 worker failed; preserve evidence, no automatic retry')
                finally:finish_children(process,manager,args.work)
    except BaseException as error:
        failure=dict(type=type(error).__name__,message=str(error)[:500])
        raise
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
            report['ready_for_analysis']=bool(science and not failure and sampler is not None and sampler.returncode==0
                and report.get('original_service_restored') is True and report.get('controller_restoration_verified') is True and report['resources_restored'])
            report['complete']=False;atomic_json(args.work/'report.json',report)
            for sig,handler in handlers.items():signal.signal(sig,handler)
    need(report['ready_for_analysis'],'science, sampler or restoration incomplete')
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
    for name in ('repo','work','binary','build-binding','library','inspector','source-collection','current-collection'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--smoke',action='store_true',help='N8 only, one campaign; never pooled into formal collection')
    parser.add_argument('--worker',action='store_true');parser.add_argument('--prepare-only',action='store_true')
    args=parser.parse_args()
    for key in ('repo','binary','build_binding','library','inspector','source_collection','current_collection'):
        setattr(args,key,getattr(args,key).resolve(strict=True))
    args.work=args.work.resolve()
    need(not args.work.is_relative_to(args.source_collection) and not args.work.is_relative_to(args.current_collection),
         'new run cannot overwrite scientific evidence')
    if args.worker:worker(args)
    else:
        proof=preflight(args)
        if args.prepare_only:
            args.work.mkdir(parents=True,mode=0o700,exist_ok=False);atomic_json(args.work/'preflight.json',proof)
            print(json.dumps(dict(complete=True,scientific_queries=0,expected_queries=expected_rows(args),smoke=args.smoke)))
        else:supervise(args,proof)


if __name__=='__main__':main()
