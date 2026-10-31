"""Public-layer A/B on four existing FM1 cases; query-only, no implicit retry."""
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

from . import run, telemetry, client, grib_catalog_client, hdf5_batch_client
from fs_index.native import NativeContext
from .replay import SOURCE_PINS
from .hdf5_cache_batch import verify_build_binding, finish_children, validate_row as validate_hdf5
from .grib_catalog import validate_row as validate_grib, require_json_roundtrip
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.hdf5_route3.native_filesystem.fixed_probe import filesystem_launcher
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json

need=run.need
PROTOCOL='iotdbfs-fixed-shared-candidates/1'
BASELINE_SHA='f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
CANDIDATE_SHA='d52c9b1edfb618efa27834e4464248995bbe0976a8b4d4298ad8a434d82bdd20'
CONFIGS=('A','B')
ARMS=('IoTDB-FS','FS-index')
PRODUCT_DIFF={
 'include/iotdbfs/query/core/QueryModule.h','include/iotdbfs/query/frontend/TypedQueryService.h',
 'src/query/core/QueryModule.cpp','src/query/frontend/FuseTypedQueryBridge.cpp',
 'src/query/frontend/QueryRegistrationSocket.cpp','src/query/frontend/TypedQueryService.cpp'}
NEW_TESTS={('QueryModule','BorrowedSelectionCanDieBeforePreparedQueryExecutes'),
 ('FormatCatalogSocket','BorrowsFullOwnerIdentityViewForEveryFreshAuthorization'),
 ('FormatCatalogSocket','StrictAuthorizationKeepsOwnedExactIdentityList'),
 ('OwnerQueryAdmission','MaterializedSourceViewPinsCompleteContextAcrossSharedPinsAndMoves'),
 ('OwnerQueryAdmission','StrictMaterializedSourceViewOwnsItsOriginalIdentities'),
 ('SocketFixture','StrictLazyMembershipDoesNotAuthorizeAChangedSourceIdentity'),
 ('SocketFixture','OwnerRegistrationBorrowsFrozenSourceViewAndSkipsUnusedMembership'),
 ('SocketFixture','BorrowedOwnerAuthorizationStillRejectsNonmatchingFrozenCandidate')}
PATH_ARGS=('repo','work','binary','baseline_binary','build_binding','baseline_build_binding',
           'library','inspector','source_collection','current_collection')


def groups(repo,tier):
    need(tier in ('n8','n128'),'unknown fixed tier')
    size=8 if tier=='n8' else 128
    by={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    return [by[f'FM1-{fmt}-N{size}-V1-C{size}-S0-base-latest'] for fmt in ('grib','hdf5')]


def campaigns(group):return (0,) if group['format']=='grib' and group['N']==8 else (0,1,2)
def configuration_order(campaign):return CONFIGS if campaign%2==0 else tuple(reversed(CONFIGS))
def expected_rows(args):return sum(len(campaigns(g))*len(CONFIGS)*len(ARMS)*12 for g in groups(args.repo,args.tier))


def closure(repo):
    result=run.closure(repo)
    for relative in ('experiments/fixed_matrix/tests/test_shared_candidates.py',
        'experiments/cross_version/shared_candidates/run.sh','experiments/cross_version/shared_candidates/README.md'):
        result[relative]=file_sha(repo/relative)
    return result


def verify_candidate(args):
    """The new build has one full XML gate, never a fabricated focused gate."""
    stage=args.binary.parents[2];receipt=read_json(args.build_binding)
    manifest_path=stage/'source-manifest.json';sources=read_json(manifest_path);source_root=stage/'source'
    need(receipt.get('complete') is True and receipt.get('full_clean_build') is True and
         receipt.get('binary_sha256')==file_sha(args.binary)==CANDIDATE_SHA and
         receipt.get('source_manifest_sha256')==file_sha(manifest_path),'candidate build receipt differs')
    need(source_root.is_dir() and not source_root.is_symlink() and
         {str(p.relative_to(source_root)) for p in source_root.rglob('*') if p.is_file()}==set(sources),
         'candidate exact source closure differs')
    for name,sha in sources.items():
        relative=Path(name);path=source_root/relative
        need(not relative.is_absolute() and '..' not in relative.parts and not path.is_symlink() and
             path.resolve(strict=True).is_relative_to(source_root.resolve()) and file_sha(path)==sha,'candidate source changed')
    tests=stage/'all-unit-tests.xml';unit=args.binary.with_name('unit_tests')
    need(file_sha(tests)==receipt.get('tests_sha256') and file_sha(unit)==receipt.get('unit_binary_sha256'),
         'candidate test artifacts changed')
    validate_unit_gate(tests)
    a=SimpleNamespace(binary=args.baseline_binary,build_binding=args.baseline_build_binding)
    baseline_gate=verify_build_binding(a);baseline=read_json(args.baseline_build_binding)
    need(file_sha(args.baseline_binary)==BASELINE_SHA,'latest accepted baseline differs')
    product=lambda n:n.startswith(('src/','include/','cmake/')) or n=='CMakeLists.txt'
    names={n for n in baseline['source_files'] if product(n)}|{n for n in sources if product(n)}
    changed={n for n in names if baseline['source_files'].get(n)!=sources.get(n)}
    need(changed==PRODUCT_DIFF,'A/B product difference exceeds approved six common-layer files')
    return dict(candidate_sha256=CANDIDATE_SHA,baseline_sha256=BASELINE_SHA,
        verification_sha256=file_sha(args.build_binding),source_manifest_sha256=file_sha(manifest_path),
        candidate_unit_tests=1833,baseline_gate=baseline_gate,product_files=len(names),changed_product_files=sorted(changed),
        build_environment=compare_build_environment(args.baseline_binary,args.binary))


def validate_unit_gate(path):
    root=ET.parse(path).getroot();cases=list(root.iter('testcase'))
    need(root.tag in ('testsuite','testsuites') and int(root.attrib.get('tests',0))==1833 and len(cases)==1833 and
         all(int(n.attrib.get(k,0))==0 for n in root.iter() if n.tag in ('testsuite','testsuites')
             for k in ('failures','errors','disabled','skipped')) and
         not any(list(root.iter(k)) for k in ('failure','error','skipped')) and
         all(n.get('status')=='run' and n.get('result')=='completed' for n in cases),
         'candidate unit gate must contain 1833 completed cases without skips')
    for suite,name in NEW_TESTS:
        need(sum(n.get('classname')==suite and n.get('name')==name for n in cases)==1,
             'new candidate test missing or duplicated: '+suite+'.'+name)


def compare_build_environment(baseline,binary):
    keys=('CMAKE_BUILD_TYPE','CMAKE_CXX_COMPILER','CMAKE_CXX_FLAGS','CMAKE_CXX_FLAGS_RELEASE',
          'Arrow_DIR','HDF5_DIR','CMAKE_PREFIX_PATH','IOTDB_ROOT')
    records={}
    for label,path in (('A',baseline),('B',binary)):
        cache=path.parents[1]/'CMakeCache.txt';values={}
        for line in cache.read_text().splitlines():
            if line and not line.startswith(('#','//')) and ':' in line and '=' in line:
                values[line.split(':',1)[0]]=line.split('=',1)[1]
        need(all(values.get(k) for k in ('CMAKE_BUILD_TYPE','CMAKE_CXX_COMPILER','Arrow_DIR','HDF5_DIR')),
             'build cache lacks compiler/dependency configuration')
        selected={k:values.get(k) for k in keys}
        need(selected['CMAKE_BUILD_TYPE']=='Release','A/B must use Release builds')
        compiler=Path(selected['CMAKE_CXX_COMPILER']).resolve(strict=True)
        records[label]=dict(cache_sha256=file_sha(cache),configuration=selected,
            compiler_path=str(compiler),compiler_sha256=file_sha(compiler))
    need(records['A']['configuration']==records['B']['configuration'] and
         records['A']['compiler_sha256']==records['B']['compiler_sha256'],
         'A/B compiler, Release flags or Arrow/HDF5 dependency configuration differs')
    return records


def native_binding(args,cid,manifest):
    current=read_json(args.current_collection/'current-locations.json')
    report=read_json(args.current_collection/'report.json')
    need(report.get('complete') is True and report.get('original_service_restored') is True and
         report.get('resources_restored') is True,'native location collection incomplete')
    locations=[v for v in current['corpora'] if v['corpus_id']==cid]
    items=[v for v in report['corpora'] if v['corpus_id']==cid]
    need(len(locations)==len(items)==1,'native location is not unique')
    loc,item=locations[0],items[0];journal=Path(loc['active_owner_journal']);native=Path(loc['native_file_root'])
    path=Path(loc['location_receipt'])
    need(loc['namespace']==item['namespace'] and str(journal)==item['active_owner_journal'] and
         str(native)==item['native_file_root'] and native.is_dir() and not native.is_symlink() and
         str(path)==item['file_binding_receipt'] and file_sha(path)==item['file_binding_receipt_sha256'],
         'native location reference differs')
    receipt=read_json(path);journal_digest=run.infrastructure.journal_snapshot(journal)
    need(receipt.get('complete') is True and receipt.get('scientific_identities_unchanged') is True and
         receipt['head_after']==item['head'] and journal_digest==item['journal_digest']==receipt['active_journal_digest'],
         'native location scientific identity/head/journal differs')
    for entry in manifest['entries']:
        blob=native/(entry['sha256']+'.blob')
        need(blob.is_file() and not blob.is_symlink() and blob.stat().st_size==entry['bytes'] and
             file_sha(blob)==entry['sha256'],'managed native HDF5 bytes changed')
    return dict(journal=str(journal),journal_digest=journal_digest,head=item['head'],namespace=item['namespace'],
        native_root=str(native),receipt=str(path),receipt_sha256=file_sha(path),
        native_inventory_sha256=digest([(v['sha256'],v['bytes']) for v in manifest['entries']]))


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
        need(gate.get('protocol')==PROTOCOL and gate.get('tier')=='n8' and gate.get('query_records')==192 and
             all(gate.get(k) is True for k in ('complete','scientific_complete','ready_for_analysis','original_service_restored',
                  'resources_restored','controller_restoration_verified')) and
             gate.get('product_sha256')==CANDIDATE_SHA and gate.get('baseline_sha256')==BASELINE_SHA,
             'new N8 qualification incomplete or different A/B')
    return dict(protocol=PROTOCOL,complete=True,tier=args.tier,cases=entries,build_gate=build,
        source_collection=str(source),source_pins=SOURCE_PINS,current_collection=str(args.current_collection),
        current_location_files={n:file_sha(args.current_collection/n) for n in ('current-locations.json','report.json')},
        expected_queries=expected_rows(args),configurations=list(CONFIGS),arms=list(ARMS),
        warmup_per_arm=2,measured_per_arm=10,campaigns={g['group_id']:list(campaigns(g)) for g in groups(args.repo,args.tier)},
        gate_report_sha256=file_sha(args.gate_report) if gate else None,
        product=dict(path=str(args.binary),sha256=CANDIDATE_SHA),baseline=dict(path=str(args.baseline_binary),sha256=BASELINE_SHA),
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
            native_root=Path(entry['native_root']) if entry['format']=='hdf5' else None,
            prep=read_json(base/'preparation.json'),manifest=read_json(base/'input/manifest.json'),
            cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)))
    return result,bindings


def launcher(path,binary,sha,namespace,journal,plan):
    if plan['group']['format']=='grib':return run.infrastructure.launcher(path,binary,sha,namespace,journal)
    receipt=filesystem_launcher(path,binary,namespace,journal,False,native_root=plan['native_root'],expected_sha=sha)
    declaration='SETTINGS='+repr(receipt['settings'])+'\n';text=path.read_text()
    need(text.count(declaration)==1,'launcher settings layout differs')
    settings=dict(receipt['settings'],**{'query.hdf5.native_cache':'true'})
    path.write_text(text.replace(declaration,'SETTINGS='+repr(settings)+'\n',1))
    receipt.update(settings=settings,sha256=file_sha(path));return receipt


def validate_row(row,group,configuration):
    need(configuration in CONFIGS,'unknown configuration')
    count=group['K_units']
    if group['format']=='hdf5':validate_hdf5(row,count,'C1B1')
    else:validate_grib(row,'C',count)  # Old validator's C means format_catalog, not render C.
    if configuration=='A':return row  # Missing new A counters are never zero-work evidence.
    borrowed='registration_source_refs_borrowed'
    if group['format']=='grib':
        d=row['discovery_call']['server_profile']['counters']
        need(d.get(borrowed)==group['C_paths'] and d.get('registration_source_refs_constructed',0)==0,
             'GRIB borrowed discovery source membership mechanism differs')
        for call in row['value_calls']:
            c=call['server_profile']['counters']
            need(c.get(borrowed)==1 and c.get('registration_authorization_membership_builds',0)==0 and
                 c.get('registration_authorization_membership_entries',0)==0,'exact GRIB value borrowed membership mechanism differs')
    else:
        p=row['value_calls'][0]['server_profile'];c=p['counters']
        need(c.get(borrowed)==count and c.get('typed_batch_items')==count and
             c.get('registration_authorization_membership_builds',0)==0 and
             c.get('registration_authorization_membership_entries',0)==0,
             'HDF5 top-level batch borrowed membership mechanism differs')
    return row


def collect(args,execution,bindings,registry,frozen,report,env,paired):
    paths=read_json(args.work/'resource-paths.json');count=0
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    for campaign in (0,1,2):
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
                print(json.dumps(dict(event='shared_candidates_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
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
                                runner_sha256=digest(frozen['sources']),engine=('SQLite index + native owned values' if arm=='FS-index' else
                                    'native-hdf5 cache+batch' if group['format']=='hdf5' else 'format_catalog + unchanged GRIB typed values')),
                            clock=dict(kind='client-owned-full-query',d='entry through discovery delivery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                            cache=dict(protocol=PROTOCOL,configuration=config,native_helper=plan['cfg'] if arm=='FS-index' else None,
                                daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=group['format']=='hdf5'),
                            environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=2,measured=10,schedule='AB/BA/AB; paired alternating arms; parity repeat+campaign+format+configuration'))
                        bound=registry.register_run(model);b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    for repeat in range(12):
                        order=ARMS if (repeat+campaign+ordinal+CONFIGS.index(config))%2==0 else tuple(reversed(ARMS))
                        for arm in order:
                            rb=telemetry.sample(paths);run.validate_sample(rb)
                            if arm=='IoTDB-FS':
                                if group['format']=='hdf5':row=hdf5_batch_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],batch=True)
                                else:row=grib_catalog_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],lightweight=True)
                                validate_row(row,group,config)
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


def analyze(path,bindings,selected,resource_id):
    rows=[json.loads(line) for line in Path(path).read_text().splitlines()]
    wanted={(g['group_id'],c,f,a,r) for g in selected for c in campaigns(g) for f in CONFIGS for a in ARMS for r in range(12)}
    keys={(r['group_id'],r['campaign'],r['configuration'],r['arm'],r['repeat']) for r in rows}
    need(len(rows)==len(wanted) and keys==wanted,'all preregistered rows required; no exclusions')
    gm={g['group_id']:g for g in selected};resources={r.get('resource_id') for r in rows}
    need(bool(resource_id) and resources=={resource_id},'mixed or unbound resource identity')
    for row in rows:
        b=bindings[row['group_id']];group=gm[row['group_id']];config=row['configuration']
        need(all(row[k]==b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
             row['run_id']==b['run_ids'][str(row['campaign'])][config][row['arm']],'scientific or run identity differs')
        need(row.get('product_sha256')==(BASELINE_SHA if config=='A' else CANDIDATE_SHA),'A/B product differs')
        need(row.get('ok') is True and row.get('oracle_verified') is True and row.get('mode_verified') is True and
             row.get('selected_units')==group['K_units'] and row.get('owned_output_bytes')==group['owned_output_bytes'],
             'unverified owned query output')
        need(row['phase']==('warmup' if row['repeat']<2 else 'measurement'),'phase differs')
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
        limitation='Sequential warm queries only; A missing new counters is not zero work; no cold-disk or concurrency claim.')


def worker(args):
    def stop(sig,frame):raise RuntimeError('shared-candidates worker cancelled; preserve evidence')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
    proof=source_proof(args);frozen=read_json(args.work/'prepared.json')
    need(proof==read_json(args.work/'reuse-source.json') and closure(args.repo)==frozen['sources'],'preflight changed')
    paired,_,env,startup=run.dependencies(args.repo);startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    report=dict(protocol=PROTOCOL,complete=False,tier=args.tier,query_records=0,expected_queries=expected_rows(args),corpora=[],
        product_sha256=CANDIDATE_SHA,baseline_sha256=BASELINE_SHA,resource_id=frozen['resource_id'],
        scientific_publication=False,index_rebuilt=False,new_cases=False,location_bindings_created=False,credentials_recorded=False)
    atomic_json(args.work/'report.json',report)
    try:
        registry=Registry(args.work/'registry');execution,bindings=plans(args,registry,report,proof)
        atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),arms=list(ARMS),
            mounts=[dict(group=g['group_id'],campaign=c,order=list(configuration_order(c)))
                for c in (0,1,2) for g in groups(args.repo,args.tier) if c in campaigns(g)],fresh_mount_and_FS_context=True))
        count=collect(args,execution,bindings,registry,frozen,report,env,paired)
        need(count==expected_rows(args),'shared A/B collection incomplete')
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
        raise RuntimeError('shared-candidates supervisor cancelled; restore original service/resources')
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
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.shared_candidates','--worker']
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
                            need(time.monotonic()-started<=12600,'shared-candidates worker hard timeout')
                            time.sleep(5)
                    need(process.returncode==0,'shared-candidates worker failed; preserve evidence, no automatic retry')
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
        summary=analyze(args.work/'queries.jsonl',read_json(args.work/'bindings.json'),groups(args.repo,args.tier),prepared['resource_id'])
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


