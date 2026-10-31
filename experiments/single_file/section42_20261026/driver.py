"""Data, private publication, and collection driver for Section 4.2."""
from pathlib import Path
import copy, json, os, re, shutil, statistics, time
from experiments.fixed_matrix import run,telemetry
from experiments.identity.registry import file_sha,read_json
from experiments.cross_version.resource_limited.resources import atomic_json
from . import read_matrix as rm, extensions_cases as ec, extensions_reader as er, writes, sessions, lifecycle_collection

BASE='experiments/cross_version/work/section42-20261026'
need=run.need

def dependencies(repo):
    """Use the isolated instance's existing operational defaults after reboot.

    The old shared helper requires a dead historical /proc PID. Keep its
    runtime/helper checks, but bind authentication to the SHA-pinned runtime
    status script instead. Credentials stay in memory and are never logged.
    """
    import sys
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    paired=startup.load_runner(repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner']=paired
    from experiments.cross_version.pilot import publish
    need(file_sha(paired.sf.__file__)==startup.HELPER_SHA,'frozen infrastructure import differs')
    runtime=repo.parent/'runtime'
    status=runtime/'bin/status.sh';env_file=runtime/'bin/env.sh'
    need(file_sha(status)=='871a09be30739f2799cfecb9bb2c7acce3d7712a9b53c0913afd336a05f8fab8' and
         file_sha(env_file)=='999fcc24cbb8ef01144c5bdde0d418e37c0a4e130a970b523fdc0a992a72efe8',
         'isolated instance operational connection source differs')
    defaults={key:re.search(r'\$\{'+key+r':-([^}]+)\}',status.read_text()).group(1)
              for key in ('IOTDB_USER','IOTDB_PASS')}
    env=dict(os.environ,**defaults,IOTDB_HOST='127.0.0.1',IOTDB_PORT='33667',IOTDB_DB=publish.DATABASE)
    inventory=read_json(repo/'experiments/multi_file/work/runner-20261013/environment.json')
    need(file_sha(Path(inventory['daemon']))==inventory['binaries']['daemon']['sha256'],'legacy runtime binary changed')
    need(publish.h5_runtime()==inventory['python_versions'],'offline Python library identities differ')
    env['LD_LIBRARY_PATH']=inventory['runtime_library_path']
    env.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    for key in run.infrastructure.DIAGNOSTIC:env.pop(key,None)
    return paired,publish,env,startup

def load_plan(repo):return read_json(repo/BASE/'plan.json')

def raw_sources(repo):
    main=rm.main_cases(repo)['sources']
    result={k:dict(v,id=k) for k,v in main.items()}
    for key,v in ec.source_registry().items():result[key]=dict(v,id=key,source_id=key,format_id=v['format'])
    return result

def preflight(repo,stage):
    need(stage in ('publish','main','extensions','sessions','lifecycle','lifecycle-c0',
        'lifecycle-c1','lifecycle-c2','all','all-read','smoke'),'unknown section42 stage')
    sources=raw_sources(repo)
    for key,s in sources.items():
        p=Path(s['raw']);need(p.is_file() and p.stat().st_size==s['bytes'] and file_sha(p)==s['sha256'],'frozen source changed: '+key)
    plan=load_plan(repo)
    need(re.fullmatch(r'vd_[a-z0-9_]{1,48}',plan['namespace']) is not None,'owner namespace prefix invalid')
    maincases=rm.main_cases(repo)['cases']
    for source in sources.values():
        need(isinstance(source,dict) and source['format_id'] in ('hdf5','grib','arrow'),'malformed raw source registry')
        locator_spec=source_with_filter(source,maincases)
        if source['format_id']=='grib':rm.discovery_request(dict(locator_spec,logical_path='/preflight',version=1))
    result=dict(stage=stage,plan=plan,plan_sha256=file_sha(repo/BASE/'plan.json'),raw_sources=sources,
        main_cases=[c['case_id'] for c in rm.main_cases(repo)['cases']],
        extensions_cases=[dict(id=c['id'],variant=c.get('variant','default')) for c in ec.case_registry()],
        native_prepared_reuse=True,iotdb_prepared_reuse=sessions.prepared_query_reuse_support(),
        grib_encoded_cache_ablation=sessions.encoded_cache_ablation_support())
    if stage == 'all' or stage.startswith('lifecycle'):
        campaigns=lifecycle_collection.campaigns_for_stage(stage)
        result.update(lifecycle_campaigns=list(campaigns),lifecycle_expected_records=30*len(campaigns),
            lifecycle_expected_queries=1110*len(campaigns))
    if stage!='publish':
        pub=repo/BASE/plan['publication_collection']/'publication-proof.json'
        proof=read_json(pub);need(proof['complete'],'private publications incomplete')
        result.update(publication_proof=str(pub),publication_proof_sha256=file_sha(pub))
    return result

def make_launcher(path,binary,namespace,journal,native_root,*,initialize,mode):
    receipt=run.infrastructure.launcher(path,binary,file_sha(binary),namespace,journal)
    declaration='SETTINGS='+repr(receipt['settings'])+'\n'
    settings=dict(receipt['settings'],**{'storage.version_directory.initialize':str(initialize).lower(),
        'storage.native_file_root':str(native_root),'query.hdf5.mode':mode,
        'query.hdf5.native_cache':'false','query.hdf5.read_parallelism':'1',
        'query.tasks.worker_threads':'4','query.tasks.max_batch_size':'4',
        'query.tasks.max_queued_tasks':'64','query.result_fetch.sender_threads':'1',
        'query.cache.memory_budget_bytes':'268435456','query.cache.max_entries':'65536',
        'storage.version_directory.reader_pool_size':'4'})
    source=path.read_text();need(source.count(declaration)==1,'launcher declaration changed')
    path.write_text(source.replace(declaration,'SETTINGS='+repr(settings)+'\n',1))
    receipt.update(settings=settings,sha256=file_sha(path));atomic_json(path.with_suffix('.json'),receipt)
    return receipt

def locator(context,logical,spec):
    source=dict(spec,logical_path=logical,version=1)
    if source['format_id']=='hdf5':
        request=dict(kind='dataset_catalog',request=dict(sources=dict(paths=[dict(kind='exact',path=logical)],versions=dict(kind='latest')),dataset_path=source.get('dataset','/T')))
        field='datasets'
    elif source['format_id']=='grib':
        request=rm.discovery_request(source);request['request']['sources']['versions']=dict(kind='latest');field='units'
    else:
        import pyarrow as pa
        case=next(c for c in ec.case_registry() if c['source']==source['source_id'])
        def visible(request):
            response,_=rm.control(context['socket'],request)
            body,_=rm.fetch(context,response)
            with pa.BufferReader(body) as stream:
                reader=pa.ipc.open_stream(stream)
                item=reader.read_next_batch_with_custom_metadata()
                need(item.batch.num_rows==1 and item.batch.schema.names==['index'],'index-only visibility result differs')
                meta=dict(item.custom_metadata or {})
                need(meta.get(b'iotdbfs.sources')==b'1' and meta.get(b'iotdbfs.source.0.path')==logical.encode(),'index-only visibility source differs')
                version=int(meta[b'iotdbfs.source.0.version'])
            return version
        # Arrow physical subjects require Exact selection. The current
        # publication receipt identifies its actual committed version; a fresh
        # index-only Exact query then independently verifies source visibility.
        profile_path=Path(context['root'])/'publication.jsonl'
        deadline=time.monotonic()+30
        commits=[]
        while not commits:
            if profile_path.exists():
                commits=[json.loads(line) for line in profile_path.read_text().splitlines() if line.strip()]
                commits=[row for row in commits if row.get('kind')=='content_commit' and row.get('ok') is True and row.get('path')==logical and row.get('source_bytes')==source['bytes']]
            need(time.monotonic()<deadline,'Arrow committed publication receipt missing')
            if not commits:time.sleep(.001)
        need(len(commits)==1,'ambiguous Arrow committed source receipt')
        version=commits[0]['version']
        exact_source=dict(source,version=version)
        need(visible(er.arrow_index_only_request(exact_source,case,latest=False))==version,'exact Arrow visibility differs')
        return dict(logical_path=logical,version=version,exact_visible=True,format_id='arrow',
            visibility_protocol='content_commit receipt then typed index-only Exact',catalog_units=1,
            explicit_external_catalog=False,index_rows=1)
    response,_=rm.control(context['socket'],request)
    rows=response.get(field,[])
    need(bool(rows) and all(r['path']==logical for r in rows),'publication visibility missing: '+source['source_id'])
    versions={r['version'] for r in rows};need(len(versions)==1,'ambiguous publication version')
    version=versions.pop();request['request']['sources']['versions']=dict(kind='exact',exact=version)
    exact,_=rm.control(context['socket'],request)
    need(bool(exact.get(field)) and all(r['path']==logical and r['version']==version for r in exact[field]),'exact visibility failed')
    return dict(logical_path=logical,version=version,exact_visible=True,format_id=source['format_id'],catalog_units=len(rows))

def source_with_filter(source,cases):
    source=copy.deepcopy(source)
    if source['format_id']=='grib':source['filter']=next(c['filter'] for c in cases if c['source_id']==source['source_id'])
    return source

def publication_collect(repo,work,proof,env,paired,paths,binary,report):
    plan=proof['plan'];journal=work/'owner';need(not journal.exists(),'owner journal must be newly created by the private owner');native_root=work/'native-files';native_root.mkdir(mode=0o700)
    all_sources=proof['raw_sources'];maincases=rm.main_cases(repo)['cases']
    receipt=dict(complete=False,namespace=plan['namespace'],journal=str(journal),native_root=str(native_root),campaigns=[])
    writer=repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    for campaign in range(3):
        block=work/('campaign-%d'%campaign);block.mkdir()
        make_launcher(block/'launcher',binary,plan['namespace'],journal,native_root,initialize=campaign==0,mode='iotdb-indexed')
        selected=proof['plan']['source_order'] if campaign==0 else [k for k in proof['plan']['source_order'] if k.startswith(('H-','G-'))]
        selected=[source_with_filter(all_sources[k],maincases) for k in selected]
        print(json.dumps(dict(event='publication_campaign',campaign=campaign,sources=[s['id'] for s in selected])),flush=True)
        local=block/'native-copies';local.mkdir()
        def native_copies():
            fs_rows=[]
            for s in selected:
                if not s['id'].startswith(('H-','G-')):continue
                rb=telemetry.sample(paths);run.validate_sample(rb)
                result=writes.checked_copy(s['raw'],local/(s['id']+Path(s['raw']).suffix),expected_bytes=s['bytes'])
                ra=telemetry.sample(paths);run.validate_sample(ra)
                need(file_sha(local/(s['id']+Path(s['raw']).suffix))==s['sha256'],'native copy hash differs')
                row=dict(campaign=campaign,source_id=s['id'],method='native FS',resource_before=rb,resource_after=ra,**result)
                fs_rows.append(row);run.append(work/'writes.jsonl',row)
        if campaign%2==0:native_copies()
        with paired.sf.private_mount(block/'launcher',block/'mount',env,writer,profile=True) as context:
            need(file_sha(Path('/proc')/str(context['pid'])/'exe')==file_sha(binary),'running product differs')
            rb=telemetry.sample(paths);run.validate_sample(rb)
            publication=writes.publish_sources(context=context,sources=selected,prefix='/'+plan['namespace']+'/c%d'%campaign,
                receipt_path=block/'publication.json',locator=lambda logical,s:locator(context,logical,s))
            ra=telemetry.sample(paths);run.validate_sample(ra)
            for s in publication['sources']:
                run.append(work/'writes.jsonl',dict(campaign=campaign,source_id=s['id'],method='IoTDB-FS',
                    resource_before=rb,resource_after=ra,**s))
            context_summary=dict(pid=context['pid'],mount=str(context['mount']),root=str(context['root']))
        if campaign%2==1:native_copies()
        receipt['campaigns'].append(dict(campaign=campaign,publication=str(block/'publication.json'),
            publication_sha256=file_sha(block/'publication.json'),context=context_summary,sources=publication['sources']))
        atomic_json(work/'publication-proof.json',receipt)
    receipt.update(complete=True,journal_digest=run.infrastructure.journal_snapshot(journal))
    atomic_json(work/'publication-proof.json',receipt)
    report.update(complete=True,scientific_queries=0,write_records=sum(5+len(c['sources']) for c in receipt['campaigns']),
        namespace=plan['namespace'],publication_proof_sha256=file_sha(work/'publication-proof.json'))

def collect(repo,work,proof,env,paired,paths,binary,report):
    if proof['stage']=='publish':publication_collect(repo,work,proof,env,paired,paths,binary,report)
    else:read_collect(repo,work,proof,env,paired,paths,binary,report)

def analyze(work):
    report=read_json(work/'report.json')
    if (work/'writes.jsonl').exists():
        rows=[json.loads(s) for s in (work/'writes.jsonl').read_text().splitlines()]
        cells=[]
        for key in sorted({r['source_id'] for r in rows}):
            for method in ('native FS','IoTDB-FS'):
                rr=[r for r in rows if r['source_id']==key and r['method']==method]
                if not rr:continue
                cells.append(dict(source_id=key,method=method,count=len(rr),
                    write_median_ms=statistics.median(r['total_ns'] if method=='native FS' else r['write']['total_ns'] for r in rr)/1e6,
                    locator_median_ms=None if method=='native FS' else statistics.median(r['source_locator_ns'] for r in rr)/1e6,
                    raw=rr))
        return dict(complete=True,stage='publish',cells=cells,resource_coverage=run.validate_coverage(work,rows),publication_proof_sha256=file_sha(work/'publication-proof.json'),
            lifecycle_measured=False,publication_and_visibility_separate=True)
    return analyze_reads(work)


def read_collect(repo,work,proof,env,paired,paths,binary,report):
    from . import read_driver
    return read_driver.collect(repo,work,proof,env,paired,paths,binary,report,make_launcher)

def analyze_reads(work):
    from . import read_driver
    return read_driver.analyze(work)
