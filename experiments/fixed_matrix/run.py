"""Approved FM1 primary collection; no arbitrary experiment parameter overrides.

Original database, account and license only. The supervisor owns restoration
outside the measured group. Every scientific fixture and timing is newly bound.
"""
from __future__ import annotations
import argparse
from contextlib import ExitStack
import itertools
import json
import os
from pathlib import Path
import random
import shutil
import signal
import subprocess
import sys
import time
import uuid

from experiments.identity.model import build_case,build_corpus,build_run,digest
from experiments.identity.registry import Registry,file_sha,read_json
from experiments.cross_version.resource_limited import run as infrastructure
from experiments.cross_version.resource_limited.resources import CgroupV2Resources,atomic_json

CATALOG_SHA='41eff9c7aa824ed62df6c025531c74e731a94e75270f3e936e7b71934bcab47d'
PRODUCT_SHA='36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9'
NATIVE_SHA='e1530ee8cc3a41d842851adede200810b9aea1b6dccdb8cbd6158ec4e367e1de'
NATIVE_RECEIPT_SHA='247dc88cb02b8dbfa9769d4ec2dd6337a5d128483dec6f4593bc66ff38bd9808'
ARMS=('FS-scan','FS-index','IoTDB-FS')
need=infrastructure.need
append=infrastructure.append


def dependencies(repo):
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    paired=startup.load_runner(repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner']=paired
    from experiments.cross_version.pilot import publish
    need(file_sha(paired.sf.__file__)==startup.HELPER_SHA,'frozen infrastructure import differs')
    _,_,env=publish.runtime(repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publish.DATABASE)
    need(env['IOTDB_HOST'] in ('127.0.0.1','localhost') and env['IOTDB_PORT']=='33667','original database endpoint differs')
    env.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    for key in infrastructure.DIAGNOSTIC:env.pop(key,None)
    return paired,publish,env,startup


def source_paths(repo):
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    frozen=repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments'
    return sorted(p for folder in ('experiments/fixed_matrix','experiments/identity','experiments/multi_file')
        for p in (repo/folder).glob('*.py')) + sorted(p for p in (repo/'fs_index').rglob('*') if p.is_file() and '__pycache__' not in p.parts) + [
        repo/'experiments/cross_version/resource_limited/resources.py',repo/'experiments/cross_version/resource_limited/run.py',
        repo/'experiments/cross_version/resource_limited/native_hdf_run.py',
        repo/'experiments/cross_version/hdf5_route3/route3_control.py',repo/'experiments/cross_version/version_directory/v00_head_probe.py',
        *sorted((repo/'experiments/cross_version/pilot').glob('*.py')),
        *sorted((repo/'experiments/cross_version/hdf5_route3').glob('*.py')),
        *sorted((repo/'experiments/cross_version/version_matrix').glob('*.py')),
        *sorted(frozen.rglob('*.py')),*[frozen/relative for relative in startup.DEPENDENCIES],
        *[repo/relative for relative in infrastructure.source_closure(repo)],
        repo/'experiments/cross_version/version_directory/v03_startup_wrapper.py',repo/'experiments/fixed_matrix/catalog.json']


def closure(repo):return {str(p.relative_to(repo)):file_sha(p) for p in source_paths(repo) if p.is_file()}


def check_registration(args):
    need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json')==CATALOG_SHA,'approved fixed catalog changed')
    doc=read_json(args.registration)
    need(doc.get('complete') is True and doc.get('before_any_experiment') is True and
        doc.get('primary_cases')==32 and doc.get('catalog_sha256')==CATALOG_SHA,'table registration not verified')
    binding=read_json(args.native_binding)
    need(file_sha(args.native_binding)==NATIVE_RECEIPT_SHA and
        binding.get('protocol')=='iotdbfs-fixed-native-build/1' and
        binding.get('tests',{}).get('passed') is True and binding['tests']['count']==7,
        'native build receipt incomplete or changed')
    need(str(args.library)==binding['library'] and file_sha(args.library)==binding['library_sha256']==NATIVE_SHA,
        'native library identity differs')
    for source in binding['sources']:
        need(file_sha(args.repo/source['path'])==source['sha256'],'native source changed since verified build')
    for lib in binding['linked_libraries']:
        need(Path(lib['path']).resolve()==Path(lib['resolved']) and file_sha(lib['resolved'])==lib['sha256'],
            'native linked library differs')
    for artifact in binding['artifacts']:
        need(file_sha(args.native_binding.parent/artifact['path'])==artifact['sha256'],'native build evidence differs')
    return doc,binding


def launcher(path,binary,prefix,journal,initialize):
    from experiments.cross_version.resource_limited.native_hdf_run import launcher as native_launcher
    receipt=native_launcher(path,binary,PRODUCT_SHA,prefix,journal)
    if initialize:
        old='SETTINGS='+repr(receipt['settings'])+'\n';settings=dict(receipt['settings'],**{'storage.version_directory.initialize':'true'})
        source=path.read_text();need(source.count(old)==1,'launcher structure changed')
        path.write_text(source.replace(old,'SETTINGS='+repr(settings)+'\n',1));receipt.update(settings=settings,sha256=file_sha(path))
    return receipt


def head(inspector,prefix,env,root):
    root.mkdir(mode=0o700)
    from experiments.cross_version.version_matrix.n8_matrix_prepare import _probe
    return _probe(inspector,'head',prefix,env,root)


def copy_file(source,destination):
    destination.parent.mkdir(parents=True,exist_ok=True)
    start=time.perf_counter_ns()
    with Path(source).open('rb',buffering=0) as raw,Path(destination).open('wb',buffering=0) as out:
        for block in iter(lambda:raw.read(1048576),b''):
            pending=memoryview(block)
            while pending:
                n=out.write(pending);need(n and n>0,'short experiment file write');pending=pending[n:]
        copied=time.perf_counter_ns();os.fsync(out.fileno());synced=time.perf_counter_ns()
    end=time.perf_counter_ns()
    return dict(copy_ns=copied-start,file_fsync_ns=synced-copied,close_ns=end-synced,total_ns=end-start,
                boundary='file data copy/fsync/close; parent setup and directory fsync excluded')


def version_forms(group,entries):
    V=group['V_stored'];kind=group['selection']
    ordinals=[V] if kind=='latest' else [1] if kind=='historical-first' else [V-1,V] if kind=='last-two' else list(range(1,V+1))
    if kind=='latest':return dict(kind='latest'),dict(kind='latest'),dict(kind='latest'),ordinals
    if kind=='all-filtered':return dict(kind='all'),dict(kind='all'),dict(kind='all'),ordinals
    chosen=[e['binding']['version'] for e in entries if e['ordinal'] in ordinals]
    lo,hi=min(chosen),max(chosen)+1
    for e in entries:need((lo<=e['binding']['version']<hi)==(e['ordinal'] in ordinals),'generation timestamp fences overlap')
    return (dict(kind='exact' if len(ordinals)==1 else 'between',ordinals=ordinals),
            dict(kind='ordinal' if len(ordinals)==1 else 'between',ordinals=ordinals),
            dict(kind='between',lower_inclusive=lo,upper_exclusive=hi),ordinals)


def bind_cases(registry,corpus,manifest,groups):
    from .client import PREDICATE
    result={};by_id={e['file_id']:e['logical_file'] for e in manifest['entries']}
    for group in groups:
        logicals=sorted(by_id[n] for n in range(group['C_paths']))
        model,native,physical,ordinals=version_forms(group,corpus['inventory'])
        request=dict(scope=dict(kind='all' if group['C_paths']==group['N'] else 'paths',logical_files=logicals,
            buckets=[],selection_basis='predeclared first C file_id values; nested; planted targets0/1 retained; P=C exact selectors'),
            selection=model,predicate=PREDICATE,operation='discover-values',roi=dict(kind='whole',origin=[0]*len(group['selected_unit_shape']),shape=group['selected_unit_shape']),
            output=dict(projection=['value','mask'],dtype='float32'),expected_K={k:group[k] for k in ('K_files','K_file_versions','K_units')})
        case=registry.register_case(build_case(corpus,request))
        scoped=sorted({e['binding']['path'] for e in corpus['inventory'] if e['logical_file'] in logicals})
        result[group['group_id']]=dict(corpus_id=corpus['corpus_id'],case_id=case['case_id'],oracle_sha256=digest(case['expected_matches']),
            scope=scoped,native_selection=native,physical_versions=physical,selected_ordinals=ordinals,
            expected_matches=case['expected_matches'],run_ids={})
    return result


def verify_raw(manifest,raw_root,fs_root):
    checked=[]
    for e in manifest['entries']:
        for root in (raw_root,fs_root):
            p=root/e['file'];st=p.stat()
            need(p.is_file() and not p.is_symlink() and st.st_nlink==1 and st.st_size==e['bytes'] and file_sha(p)==e['sha256'],'raw/FS source changed')
        checked.append(e['sha256'])
    return dict(complete=True,source_versions=len(checked),hashes_sha256=digest(checked))


def validate_sample(sample):
    from experiments.cross_version.resource_limited.resources import cpus
    groups=sample['cgroups'];parent=groups['parent']['files']
    need(parent['cpuset.cpus.partition']==CgroupV2Resources.PARTITION and cpus(parent['cpuset.cpus.effective'])=={2,3}
         and cpus(parent['cpuset.cpus.exclusive.effective'])=={2,3}
         and parent['cpu.max']==CgroupV2Resources.CPU_MAX,'actual exclusive root partition/quota differs')
    need(parent['memory.max']=='max' and groups['query']['files']['memory.max']==CgroupV2Resources.QUERY_MEMORY_MAX and
         groups['database']['files']['memory.max']=='max','actual memory limits differ')
    for role in ('query','database'):
        files=groups[role]['files']
        need(files['cpuset.cpus.partition']=='member' and files['cpu.max']=='max 100000' and
             not cpus(files['cpuset.cpus.exclusive.effective']),'actual leaf scheduling policy differs')
    for group in groups.values():
        files=group['files']
        need(cpus(files['cpuset.cpus.effective'])=={2,3},'group escaped fixed CPUs')
        events=dict(line.split() for line in files['memory.events'].splitlines())
        need(int(events.get('oom',0))==0 and int(events.get('oom_kill',0))==0,'query resource envelope had OOM')
    owned={pid for g in groups.values() for pid in g['recursive_pids']}
    for pid,process in sample['processes'].items():
        if int(pid) not in owned:continue
        if 'unavailable' in process:continue
        need(cpus(process['status']['Cpus_allowed_list'])<={2,3},'workload process escaped fixed CPUs')


def finish_children(process,manager,work):
    # Unwind the exact owned worker before touching its private mount roots.
    if process is not None and process.poll() is None:
        process.terminate()
        try:process.wait(timeout=120)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
    try:
        for path in work.glob('corpora/*/*/mount/mount'):
            if os.path.ismount(path):
                subprocess.run(['fusermount','-uz',str(path)],stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=30)
    finally:
        infrastructure.finish_query_children(process,manager,work)


def validate_coverage(work,rows):
    samples=[json.loads(s) for s in (work/'resource-samples.jsonl').read_text().splitlines()]
    lo=min(r['resource_before']['monotonic_ns'] for r in rows)
    hi=max(r['resource_after']['monotonic_ns'] for r in rows)
    need(samples and samples[0]['monotonic_ns']<=lo and samples[-1]['monotonic_ns']>=hi,
         'periodic resource series does not cover all queries')
    during=[s for s in samples if lo<=s['monotonic_ns']<=hi]
    for s in during:validate_sample(s)
    need(all(b['monotonic_ns']-a['monotonic_ns']<=5_000_000_000 for a,b in zip(samples,samples[1:])
             if b['monotonic_ns']>=lo and a['monotonic_ns']<=hi),'resource sampling gap exceeds five seconds')
    return dict(complete=True,periodic_samples=len(samples),during_queries=len(during),
        samples_sha256=file_sha(work/'resource-samples.jsonl'),scope='cumulative query envelopes including untimed audits')


def software_inventory(repo):
    from . import telemetry
    runtime=repo.parent/'runtime';release=(runtime/'current').resolve(strict=True)
    roles={}
    for name in ('datanode','confignode'):
        path=runtime/'run'/(name+'.pid')
        if path.exists():roles[name]=int(path.read_text())
    jars=sorted((release/'lib').rglob('*.jar'))
    need(jars,'original database jar inventory empty')
    return dict(release=str(release),process_roles=roles,
        database_jars=[telemetry.digest_file(p) for p in jars],
        original_config=telemetry.digest_file(release/'conf/iotdb-system.properties'),
        kernel_fuse=telemetry._command(['fusermount','--version']),
        source_credential_policy='existing shared owner; credentials not exported')


def worker_environment():
    # The Python client and product use different Arrow builds. Pass product
    # library paths only to private_mount, never to the client interpreter.
    env=dict(os.environ)
    for key in ('LD_LIBRARY_PATH','LD_PRELOAD',*infrastructure.DIAGNOSTIC):env.pop(key,None)
    env.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',
        NUMEXPR_NUM_THREADS='1',PYTHONDONTWRITEBYTECODE='1')
    return env


def bootstrap(args):
    code='''import ctypes,json,sys
from pathlib import Path
import numpy,h5py,pyarrow,pyarrow.parquet
from experiments.fixed_matrix import client,data,native,telemetry
from experiments.fixed_matrix.run import dependencies
dependencies(Path.cwd())
ctypes.CDLL(sys.argv[1])
print(json.dumps(dict(complete=True,numpy=numpy.__version__,h5py=h5py.__version__,
    hdf5=h5py.version.hdf5_version,pyarrow=pyarrow.__version__,native_loaded=True,
    process=telemetry.process(__import__('os').getpid(),inventory=True))))
'''
    started=time.perf_counter_ns()
    with (args.work/'bootstrap.private.stderr').open('x') as err:
        response=subprocess.run([sys.executable,'-c',code,str(args.library)],cwd=args.repo,
            env=worker_environment(),stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=err,timeout=120,text=True)
    need(response.returncode==0,'client bootstrap failed before any database/resource change; inspect private stderr')
    proof=json.loads(response.stdout);need(proof['complete'] is True,'client bootstrap proof missing')
    proof.update(wall_ns=time.perf_counter_ns()-started,service_modified=False,query_executed=False,
        library_environment='Python client clears LD_LIBRARY_PATH/LD_PRELOAD; product retains its separately verified runtime environment')
    atomic_json(args.work/'bootstrap.json',proof)
    return proof


def collect_campaigns(args,plans,bindings,registry,frozen,report,env,paired,*,campaigns):
    """Shared scientific loop. Replay changes paths/run identity, never queries."""
    from . import telemetry,client
    from fs_index.native import NativeContext
    binary=Path(frozen['product']['path']);inspector=Path(frozen['inspector'])
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    paths=read_json(args.work/'resource-paths.json');resource_id=frozen['resource_id']
    rows=0
    for campaign in campaigns:
        shift=(campaign*5)%len(plans)
        for plan in plans[shift:]+plans[:shift]:
            spec,root,manifest,fsroot,journal,item,scientific_head,journal_digest,corpus,groups,cfg,prep=(
                plan[k] for k in ('spec','root','manifest','fsroot','journal','item','scientific_head',
                                 'journal_digest','corpus','groups','cfg','prep'))
            key=spec['corpus_key']
            shift_case=campaign%len(groups);groups=groups[shift_case:]+groups[:shift_case]
            block=plan.get('execution_root',root)/('campaign-'+str(campaign));block.mkdir(parents=True)
            launch=launcher(block/'launcher',binary,item['namespace'],journal,False)
            head_before,_=head(inspector,item['namespace'],env,block/'head-before');need(head_before==scientific_head,'scientific head changed')
            print(json.dumps(dict(event='campaign_mount',corpus=key,campaign=campaign)),flush=True)
            with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                need(file_sha(Path('/proc')/str(context['pid'])/'exe')==PRODUCT_SHA,'running binary differs')
                actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual engine settings differ')
                native={arm:stack.enter_context(NativeContext(args.library,dict(cfg,build_index=False))) for arm in ARMS if arm!='IoTDB-FS'}
                environment=telemetry.environment(args.repo,artifact_paths=[args.library,binary],process_roles=dict(client=os.getpid(),daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=root)
                atomic_json(block/'environment.json',environment)
                mount=dict(campaign=campaign,daemon_pid=context['pid'],startup_ns=context['startup_ns'],environment_sha256=file_sha(block/'environment.json'))
                item['mounts'].append(mount)
                for group in groups:
                    gid=group['group_id'];b=bindings[gid]
                    query_plan=native['FS-index'].explain('index',b['native_selection'],client.PREDICATE,b['scope'])
                    atomic_json(block/('query-plan-'+group['table_id']+'.json'),query_plan)
                    for arm in ARMS:
                        run=registry.register_run(build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                            implementation=dict(binary_sha256=PRODUCT_SHA if arm=='IoTDB-FS' else frozen['native_library_sha256'],runner_sha256=digest(frozen['sources']),
                                engine=('native-hdf5' if spec['format']=='hdf5' else 'iotdb-indexed') if arm=='IoTDB-FS' else 'native-'+spec['format']),
                            clock=dict(kind='client-owned-full-query',d='start through actualdiscovery',q='discovery through ownedFloat32mask',profile_oracle='outside'),
                            cache=dict(protocol='FM1 retained contexts; two warmups; no final answers',
                                native_helper=cfg if arm!='IoTDB-FS' else None,
                                daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,
                                internal_caches_identical=False),
                            environment=dict(resource_id=resource_id,process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign,warmup=2,measured=10,schedule='six arm permutations repeated twice')))
                        b['run_ids'].setdefault(str(campaign),{})[arm]=run['run_id']
                    atomic_json(args.work/'bindings.json',bindings)
                    permutations=list(itertools.permutations(ARMS));rotation=(campaign*2)%6
                    schedule=(permutations[rotation:]+permutations[:rotation])*2
                    for repeat,order in enumerate(schedule):
                        for arm in order:
                            rb=telemetry.sample(paths);validate_sample(rb)
                            if arm=='IoTDB-FS':row=client.query_iotdb(context,group,b['scope'],b['physical_versions'],b['expected_matches'])
                            else:row=client.query_native(native[arm],arm,group,b['scope'],b['native_selection'],b['expected_matches'])
                            ra=telemetry.sample(paths);validate_sample(ra)
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],run_id=b['run_ids'][str(campaign)][arm],
                                oracle_sha256=b['oracle_sha256'],campaign=campaign,arm=arm,repeat=repeat,phase='warmup' if repeat<2 else 'measurement',
                                ok=True,resource_id=resource_id,resource_before=rb,resource_after=ra,environment_sha256=mount['environment_sha256'])
                            append(args.work/'queries.jsonl',row);rows+=1;report['query_records']=rows
                        atomic_json(args.work/'report.json',report)
                    print(json.dumps(dict(event='case_complete',group=gid,campaign=campaign,rows=rows)),flush=True)
            observed,_=head(inspector,item['namespace'],env,block/'head-after')
            need(observed==scientific_head and infrastructure.journal_snapshot(journal)==journal_digest,'query changed head/journal')
            need(file_sha(root/'index.sqlite')==prep['index_sha256'],'query changed index')
            mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
            atomic_json(args.work/'report.json',report)
    return rows


def worker(args):
    need(os.read(args.gate_fd,1)==b'1','worker not admitted');os.close(args.gate_fd)
    def interrupted(signum,frame):raise RuntimeError('fixed-matrix worker cancelled; preserve failed run and restore')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
    from . import data,telemetry,client
    from fs_index.native import NativeContext
    paired,publisher,env,startup=dependencies(args.repo)
    startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    frozen=read_json(args.work/'prepared.json');need(closure(args.repo)==frozen['sources'],'source closure changed')
    catalog=read_json(args.repo/'experiments/fixed_matrix/catalog.json')
    paths=read_json(args.work/'resource-paths.json');resource_id=frozen['resource_id']
    infrastructure.observe(paths)
    registry=Registry(args.work/'registry');binary=Path(frozen['product']['path'])
    inspector=Path(frozen['inspector']);writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    report=dict(protocol='iotdbfs-fixed-matrix-collection/1',complete=False,expected_queries=3456,query_records=0,corpora=[],
        catalog_sha256=CATALOG_SHA,resource_id=resource_id,product_sha256=PRODUCT_SHA,credentials_recorded=False)
    bindings={};preparation_refs={};rows=0;plans=[]
    atomic_json(args.work/'report.json',report)
    specs=data.specifications(catalog)
    # Precommitted order starts with fixed N8 groups; no result-dependent order.
    specs.sort(key=lambda s:(s['N'],s['V_stored'],s['size_profile'],s['history_scenario'],s['format']))
    atomic_json(args.work/'execution-order.json',dict(specifications=specs,arm_permutations=list(itertools.permutations(ARMS)),seed=20261021))
    try:
        for spec in specs:
            key=spec['corpus_key'];root=args.work/'corpora'/key;root.mkdir(parents=True,mode=0o700)
            item=dict(corpus_key=key,complete=False,namespace='vd_fm1_'+uuid.uuid4().hex[:16]);report['corpora'].append(item)
            atomic_json(args.work/'report.json',report)
            tick=time.perf_counter_ns();manifest=data.prepare(spec,root/'input');raw_ns=time.perf_counter_ns()-tick
            print(json.dumps(dict(event='raw_ready',corpus=key,versions=len(manifest['entries']))),flush=True)
            fsroot=root/'fs';fs_times=[]
            for e in manifest['entries']:
                timing=copy_file(root/'input'/e['file'],fsroot/e['file'])
                need(file_sha(fsroot/e['file'])==e['sha256'],'FS copy hash differs');fs_times.append(dict(file=e['file'],**timing))
            journal=root/'owner';pubroot=root/'publication';pubroot.mkdir()
            prefix='/fm1/'+key;launch=launcher(pubroot/'launcher',binary,item['namespace'],journal,True)
            published=[];publication_started=time.perf_counter_ns()
            with paired.sf.private_mount(Path(launch['path']),pubroot/'mount',env,writer,profile=True) as context:
                for e in manifest['entries']:
                    logical=prefix+'/'+e['logical_file'];target=context['mount']/logical.lstrip('/')
                    write=copy_file(root/'input'/e['file'],target)
                    version,proof=client.binding(context,spec['format'],logical)
                    need(file_sha(target)==e['sha256'],'published byte readback mismatch')
                    entry=dict(logical_file=e['logical_file'],ordinal=e['ordinal'],path=logical,version=version,bytes=e['bytes'],sha256=e['sha256'])
                    published.append(entry);append(root/'publication-bindings.jsonl',dict(**entry,file_write=write,profile_sequence=proof['sequence']))
                    if len(published)%16==0:print(json.dumps(dict(event='published',corpus=key,versions=len(published))),flush=True)
                if spec['V_stored']>1:
                    for logical in sorted({e['path'] for e in published}):
                        response,parts,_=client.call(context,client.discover_request(spec['format'],[logical],dict(kind='all'),False))
                        observed=client.discovered(spec['format'],response,parts);client.profiles(context,1)
                        counts={v:sum(r['version']==v and r['path']==logical for r in observed) for v in {r['version'] for r in observed}}
                        wanted={e['version']:4 for e in published if e['path']==logical}
                        need(counts==wanted,'published history contains missing/extra scientific versions')
            publication_ns=time.perf_counter_ns()-publication_started
            scientific_head,headproof=head(inspector,item['namespace'],env,root/'head-published')
            journal_digest=infrastructure.journal_snapshot(journal)
            publication=dict(complete=True,namespace=item['namespace'],head=scientific_head,bindings=published,
                publication_wall_ns=publication_ns,scope='includes file writes, exact binding and readback audits; individual write clocks separately recorded')
            atomic_json(root/'publication.json',publication)
            lookup={(e['logical_file'],e['ordinal']):e for e in published}
            inventory=[dict(logical_file=e['logical_file'],ordinal=e['ordinal'],bytes=e['bytes'],sha256=e['sha256'],units=e['units'],
                binding={k:lookup[e['logical_file'],e['ordinal']][k] for k in ('path','version')}) for e in manifest['entries']]
            corpus=registry.register_corpus(build_corpus(dict(bound=True,format=spec['format'],N_total=spec['N'],
                generation=dict(specification=spec,controls=manifest['controls']),inventory=inventory,
                source_evidence=dict(manifest_sha256=file_sha(root/'input/manifest.json'),raw_files=[{k:e[k] for k in ('logical_file','ordinal','file')} for e in manifest['entries']]),
                publication_evidence=dict(namespace=item['namespace'],head=scientific_head,receipt_sha256=file_sha(root/'publication.json'),bindings=published))))
            groups=[g for g in catalog['groups'] if g['group_id'] in spec['group_ids']]
            bound=bind_cases(registry,corpus,manifest,groups);bindings.update(bound)
            native_catalog=dict(protocol='iotdbfs-fixed-native/1',sources=[])
            for e in manifest['entries']:
                fs=fsroot/e['file'];stat=fs.stat();b=lookup[e['logical_file'],e['ordinal']]
                native_catalog['sources'].append(dict(path=str(fs),logical_path=b['path'],version=b['version'],ordinal=e['ordinal'],
                    size=stat.st_size,mtime_ns=stat.st_mtime_ns,format=spec['format']))
            atomic_json(root/'catalog.json',native_catalog)
            cfg=dict(catalog_path=str(root/'catalog.json'),index_path=str(root/'index.sqlite'),
                cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)
            before=telemetry.sample(paths);tick=time.perf_counter_ns()
            with NativeContext(args.library,dict(cfg,build_index=True)) as native:index_stats=native.stats()
            index_ns=time.perf_counter_ns()-tick;after=telemetry.sample(paths)
            prep=dict(complete=True,raw_generation_ns=raw_ns,FS_copy=fs_times,index_build_wall_ns=index_ns,index_stats=index_stats,
                resource_before=before,resource_after=after,index_sha256=file_sha(root/'index.sqlite'),index_bytes=(root/'index.sqlite').stat().st_size,
                publication_receipt_sha256=file_sha(root/'publication.json'),raw_validation=verify_raw(manifest,root/'input',fsroot))
            atomic_json(root/'preparation.json',prep)
            preparation_refs[corpus['corpus_id']]=dict(artifact=str(root/'preparation.json'),sha256=file_sha(root/'preparation.json'))
            item.update(corpus_id=corpus['corpus_id'],raw_manifest_sha256=file_sha(root/'input/manifest.json'),
                head=scientific_head,journal_digest=journal_digest,index_sha256=prep['index_sha256'],mounts=[])
            atomic_json(args.work/'bindings.json',bindings);atomic_json(args.work/'preparation-refs.json',preparation_refs)
            if spec['N']==8 and spec['V_stored']==1 and spec['size_profile']=='S0':
                check=root/'functional';check.mkdir()
                smoke=launcher(check/'launcher',binary,item['namespace'],journal,False)
                with paired.sf.private_mount(Path(smoke['path']),check/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                    readers={arm:stack.enter_context(NativeContext(args.library,dict(cfg,build_index=False))) for arm in ARMS if arm!='IoTDB-FS'}
                    group=groups[0];binding=bindings[group['group_id']];proof={}
                    for arm in ARMS:
                        if arm=='IoTDB-FS':proof[arm]=client.query_iotdb(context,group,binding['scope'],binding['physical_versions'],binding['expected_matches'])
                        else:proof[arm]=client.query_native(readers[arm],arm,group,binding['scope'],binding['native_selection'],binding['expected_matches'])
                    atomic_json(check/'verification.json',dict(complete=True,scientific_measurement=False,
                        reason='same registered N8 group functional mode/value/mask gate; excluded from all performance statistics',arms=proof))
                print(json.dumps(dict(event='functional_verified',corpus=key)),flush=True)
            plans.append(dict(spec=spec,root=root,manifest=manifest,fsroot=fsroot,journal=journal,
                item=item,scientific_head=scientific_head,journal_digest=journal_digest,corpus=corpus,groups=groups,cfg=cfg,prep=prep))
        need(len(plans)==14 and len(bindings)==32,'all fixed corpora must be frozen before timing')
        # Three whole-matrix time blocks. Precommitted corpus rotation avoids
        # nesting all three repeats of each corpus before the next publication.
        atomic_json(args.work/'campaign-order.json',dict(corpora=[p['spec']['corpus_key'] for p in plans],
            rotations=[0,5,10],case_order='catalog order rotated by campaign within corpus'))
        rows=collect_campaigns(args,plans,bindings,registry,frozen,report,env,paired,campaigns=range(3))
        for plan in plans:
            plan['item'].update(complete=True,final_raw_validation=verify_raw(plan['manifest'],plan['root']/'input',plan['fsroot']))
        atomic_json(args.work/'report.json',report)
        need(rows==3456 and len(bindings)==32,'fixed primary matrix incomplete')
        need(closure(args.repo)==frozen['sources'],'runner source changed during collection')
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
    except BaseException as error:
        report.update(failure=dict(type=type(error).__name__,message=str(error)[:500]))
        raise
    finally:atomic_json(args.work/'report.json',report)


def supervise(args,*,worker_module='experiments.fixed_matrix.run',worker_extra=(),finalizer=None):
    from . import telemetry
    from experiments.cross_version.hdf5_route3 import route3_control
    registration,native_binding=check_registration(args)
    paired,publisher,env,startup=dependencies(args.repo)
    product=infrastructure.verify_binaries(args.repo)['candidate'];need(product['sha256']==PRODUCT_SHA,'current product differs')
    args.work.mkdir(parents=True,mode=0o700,exist_ok=False)
    bootstrap(args)
    sources=closure(args.repo)
    for relative,sha in sources.items():
        dst=args.work/'source'/relative;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(args.repo/relative,dst);need(file_sha(dst)==sha,'source snapshot differs')
    inspector=args.repo/'experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe'
    need(file_sha(inspector)==infrastructure.INSPECTOR_SHA,'head inspector differs')
    identity=infrastructure.resource_identity([2,3])
    software=software_inventory(args.repo)
    environment=telemetry.environment(args.repo,artifact_paths=[args.library,Path(product['path']),args.native_binding],
        process_roles=software['process_roles'],data_path=args.repo)
    atomic_json(args.work/'software.json',software)
    atomic_json(args.work/'support-roles.json',{k:v for k,v in software['process_roles'].items() if k!='datanode'})
    atomic_json(args.work/'preflight-environment.json',environment)
    prepared=dict(protocol='iotdbfs-fixed-matrix-prepared/1',sources=sources,product=product,native_library=str(args.library),
        native_library_sha256=file_sha(args.library),native_build_binding_sha256=file_sha(args.native_binding),
        registration_receipt_sha256=file_sha(args.registration),inspector=str(inspector),resources=identity,
        software_receipt_sha256=file_sha(args.work/'software.json'),
        resource_id=digest(dict(configuration=identity,preflight_environment_sha256=file_sha(args.work/'preflight-environment.json'))))
    atomic_json(args.work/'prepared.json',prepared)
    args.env=env;args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text());args.report=args.work/'service-report.json';service={}
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(f in argv for f in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')),'original JVM memory differs')
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    proc=None;sampler=None;write_fd=None;handlers={};cancelled=False
    def cancel(sig,frame):
        nonlocal cancelled
        cancelled=True
        if proc and proc.poll() is None:proc.terminate()
    try:
        for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):handlers[sig]=signal.signal(sig,cancel)
        manager.create();atomic_json(args.work/'resource-before.json',manager.snapshot())
        with (args.work/'sampler.private.stderr').open('x') as err:
            sampler=subprocess.Popen([sys.executable,'-m','experiments.fixed_matrix.telemetry','--paths',str(args.work/'resource-paths.json'),
                '--roles',str(args.work/'support-roles.json'),
                '--output',str(args.work/'resource-samples.jsonl'),'--stop-file',str(args.work/'sampler.stop'),'--interval','1','--max-seconds','14000'],
                cwd=args.repo,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=err)
        with infrastructure.database_start_in_leaf(Path(infrastructure.__file__).with_name('resources.py'),manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service):
                try:
                    rd,write_fd=os.pipe()
                    command=[sys.executable,str(Path(infrastructure.__file__).with_name('resources.py')),'enter-leaf-exec','--leaf',manager.paths['query'],
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m',worker_module,'--worker',
                        '--repo',str(args.repo),'--work',str(args.work),'--library',str(args.library),'--native-binding',str(args.native_binding),
                        '--registration',str(args.registration),'--gate-fd',str(rd),*worker_extra]
                    with (args.work/'worker.stdout.log').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        proc=subprocess.Popen(command,cwd=args.repo,env=worker_environment(),stdin=subprocess.DEVNULL,stdout=out,stderr=err,pass_fds=(rd,),start_new_session=True)
                        os.close(rd);manager.attach_query(proc.pid)
                        if not cancelled:os.write(write_fd,b'1')
                        os.close(write_fd);write_fd=None;start=time.monotonic();expired=False;term_at=None
                        while proc.poll() is None:
                            infrastructure.observe(manager.paths)
                            need(sampler.poll() is None,'resource sampler exited unexpectedly')
                            if (cancelled or time.monotonic()-start>12600) and term_at is None:
                                proc.terminate();term_at=time.monotonic();expired=not cancelled
                            if term_at is not None and time.monotonic()-term_at>120:proc.kill()
                            time.sleep(5)
                        atomic_json(args.work/'worker-exit.json',dict(returncode=proc.returncode,cancelled=cancelled,elapsed_seconds=time.monotonic()-start,deadline_exceeded=expired))
                    need(proc.returncode==0,'fixed-matrix worker failed; no automatic retry; private diagnostics retained')
                finally:finish_children(proc,manager,args.work)
    finally:
        if write_fd is not None:os.close(write_fd)
        try:
            manager.close();atomic_json(args.work/'resources-restored.json',manager.state)
        finally:
            (args.work/'sampler.stop').touch()
            if sampler is not None:
                try:sampler.wait(timeout=10)
                except subprocess.TimeoutExpired:sampler.terminate();sampler.wait(timeout=10)
            for sig,handler in handlers.items():signal.signal(sig,handler)
            path=args.work/'report.json';report=read_json(path) if path.exists() else dict(complete=False,query_records=0)
            science=report.get('complete') is True;report.update(service,scientific_complete=science,resources_restored=manager.state['phase']=='closed')
            report['ready_for_analysis']=bool(science and report.get('original_service_restored') and report.get('controller_restoration_verified') and report['resources_restored'])
            report['complete']=False
            atomic_json(path,report)
    if report['ready_for_analysis']:
        try:
            from .analysis import summarize
            rows=[json.loads(s) for s in (args.work/'queries.jsonl').read_text().splitlines()]
            if finalizer is not None:
                finalizer(args,report,prepared,sampler)
                return
            resource_ids={str(c):prepared['resource_id'] for c in range(3)}
            need(sampler.returncode==0,'resource sampler did not finish cleanly')
            coverage=validate_coverage(args.work,rows)
            result=summarize(rows,read_json(args.repo/'experiments/fixed_matrix/catalog.json'),read_json(args.work/'bindings.json'),
                resource_ids=resource_ids,preparation_refs=read_json(args.work/'preparation-refs.json'),completion=dict(report,complete=True))
            atomic_json(args.work/'analysis.json',result)
            report.update(complete=True,analysis_sha256=file_sha(args.work/'analysis.json'),resource_coverage=coverage)
        except BaseException as error:
            report.update(complete=False,analysis_failure=dict(type=type(error).__name__,message=str(error)[:500]))
            raise
        finally:atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=report['complete'],report=str(args.work/'report.json'))))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('repo','work','library','native-binding','registration'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--worker',action='store_true',help=argparse.SUPPRESS);p.add_argument('--gate-fd',type=int,help=argparse.SUPPRESS)
    args=p.parse_args();args.repo=args.repo.resolve(strict=True)
    for k in ('work','library','native_binding','registration'):setattr(args,k,getattr(args,k).resolve())
    if args.worker:worker(args)
    else:
        need(not (args.registration.parent/'published-cases.json').exists(),
             'FM1 already published; use the query-only replay entry, never republish')
        supervise(args)

if __name__=='__main__':main()
