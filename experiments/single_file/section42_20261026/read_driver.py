"""Retained-mode collection and strict source/mode/profile audits for Section 4.2."""
from contextlib import contextmanager
from pathlib import Path
import json, shutil, statistics, time
from experiments.fixed_matrix import run,telemetry,query_task_client
from experiments.identity.registry import file_sha,read_json
from experiments.cross_version.resource_limited.resources import atomic_json
from . import read_collection,extensions_collection,session_collection,read_matrix as rm,extensions_reader as er,lifecycle_collection
need=run.need


def _profile(context,rows,case,method,*,failed=False,leading_catalogs=0):
    count=(1 if case['format_id']=='arrow' else 2)*len(rows)+leading_catalogs
    if failed:
        # Failed typed requests still emit their ordinary profile; wait for its
        # complete line without asserting success.
        deadline=time.monotonic()+30;profiles=[]
        while len(profiles)<count:
            profiles+=query_task_client._harvest(context,context.get('profile_offset',0),0,False)
            if len(profiles)>=count:break
            need(time.monotonic()<deadline,'failed request profile tail missing');time.sleep(.001)
    else:profiles=query_task_client._harvest(context,context.get('profile_offset',0),count,True)
    need(len(profiles)==count,'profile envelope differs')
    typed=[p for p in profiles if p['operation']=='typed']
    catalog=[p for p in profiles if p['operation'] in ('dataset_catalog','format_catalog')]
    need(len(typed)==len(rows) and len(catalog)==(0 if case['format_id']=='arrow' else len(rows))+leading_catalogs,'catalog/typed profile count differs')
    expected_catalog='dataset_catalog' if case['format_id']=='hdf5' else 'format_catalog'
    need(all(p.get('ok') is True and p['operation']==expected_catalog for p in catalog),'catalog profile operation/status differs')
    if failed:
        need(all(p.get('ok') is False and p.get('error_code')==16 and p.get('error_phase') in (5,6) for p in typed),'expected ResourceExhausted typed failure not observed')
    bypath={p.get('path'):p for p in typed}
    for index,row in enumerate(rows):
        p=typed[index] if failed else bypath.get(row.get('result_path',row.get('path')))
        need(p is not None,'typed result profile missing')
        runtime=p['runtime_config']
        need(runtime['memory_budget_bytes']==268435456 and runtime['query_task_workers']==4 and runtime['query_task_max_batch']==4,'actual resource/execution settings differ')
        if case['format_id']=='hdf5':
            settings=p['server_hdf5_execution']
            need(settings['mode']==method and settings['read_parallelism']==1 and settings['native_cache_enabled'] is False,'HDF5 actual execution differs')
            need(settings['payload_backend']==('filesystem' if method=='native-hdf5' else 'database'),'actual HDF5 payload backend differs')
            if method=='native-hdf5':need(settings['payload_backend']=='filesystem' and p.get('iotdb_delta',{}).get('payload_calls',0)==0,'native-hdf5 used database payload')
        if not failed:need(p.get('counters',{}).get('typed_cursor_next_calls',0)>0 and p.get('counters',{}).get('typed_ipc_batches',0)>0,'new cursor/IPC execution missing')
        row.update(server_profile=p,mode_verified=True)
    return profiles


def collect(repo,work,proof,env,paired,paths,binary,report,make_launcher):
    publication=read_json(proof['publication_proof']);bindings=[]
    for c in publication['campaigns']:
        bindings.append({s['id']:s['exact_source'] for s in c['sources']})
    need(len(bindings)==3,'three publication campaigns required')
    master=Path(publication['journal']);journal_digest=publication['journal_digest'];namespace=publication['namespace']
    need(run.infrastructure.journal_snapshot(master)==journal_digest,'publication owner journal changed')
    native_root=Path(publication['native_root'])
    writer=repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    counts=dict(queries=0,expected_failures=0,sessions=0,mounts=0,qualification_queries=0)
    scope=dict(qualification=False)
    head_probe=repo/'experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe'
    head_before,_=run.head(head_probe,namespace,env,work/'head-before')
    @contextmanager
    def mount_factory(campaign,block_id,mode):
        block=work/('c%d-'%campaign+block_id);block.mkdir()
        journal=block/'owner';shutil.copytree(master,journal)
        need(run.infrastructure.journal_snapshot(journal)==journal_digest,'private journal differs')
        launch=make_launcher(block/'launcher',binary,namespace,journal,native_root,initialize=False,mode=mode)
        print(json.dumps(dict(event='read_block',campaign=campaign,block=block_id,mode=mode)),flush=True)
        with paired.sf.private_mount(block/'launcher',block/'mount',env,writer,profile=True) as context:
            need(file_sha(Path('/proc')/str(context['pid'])/'exe')==proof['product_sha256'],'running product identity differs')
            actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual daemon settings differ')
            context.update(profile_offset=0,max_result_bytes=67108864,hdf5_mode=mode)
            yield context
            need(not query_task_client._harvest(context,context['profile_offset'],0,False),'extra profile outside query envelopes')
        need(run.infrastructure.journal_snapshot(journal)==journal_digest,'query changed private owner journal')
        counts['mounts']+=1
        run.append(work/'mounts.jsonl',dict(campaign=campaign,block=block_id,mode=mode,pid=context['pid'],mount=str(context['mount']),journal_digest=journal_digest,unmounted=not __import__('os').path.ismount(context['mount'])))
    def stamp(case,method,campaign):
        return dict(case_id=case['case_id'],variant=case.get('variant','default'),format_id=case['format_id'],
            source_id=case.get('source_id',case.get('source')),source_sha256=case['sha256'],source_bytes=case['bytes'],
            exact_source=dict(path=case['logical_path'],version=case['version']),method=method,campaign=campaign,
            product_sha256=proof['product_sha256'] if method!='native FS' else None,
            resource_id=file_sha(work/'environment.json'))
    def measure(*,case,method,campaign,repeat,read,audit,context,expected_failure=False):
        rb=telemetry.sample(paths);run.validate_sample(rb)
        try:row,owned=read()
        except Exception as error:
            ra=telemetry.sample(paths);run.validate_sample(ra)
            row=dict(getattr(error,'partial_record',{}),**stamp(case,method,campaign),repeat=repeat,
                resource_before=rb,resource_after=ra,status='failed',error_type=type(error).__name__,error=str(error))
            if expected_failure:
                need(getattr(error,'response',{}).get('code')==16 or any(x in str(error).lower() for x in ('exhaust','budget','result size','result bytes','limit exceeded')),'expected resource failure was another error: '+str(error))
                row['expected_failure']=True
                _profile(context,[row],case,method,failed=True)
                counts['expected_failures']+=1
                run.append(work/'failures.jsonl',row)
                return
            run.append(work/'failures.jsonl',row);raise
        ra=telemetry.sample(paths);run.validate_sample(ra)
        need(not expected_failure,'expected oversize query unexpectedly succeeded')
        validation=audit(row,owned);del owned
        need(row['total_ns']==row['t2_ns']-row['t0_ns']==row['discovery_ns']+row['value_ns'],'complete owned clocks differ')
        if context:_profile(context,[row],case,method)
        row.update(stamp(case,method,campaign),repeat=repeat,phase=('first' if repeat==0 else 'warmup' if repeat<3 else 'measurement'),
            validation=validation,oracle_verified=True,resource_before=rb,resource_after=ra,status='ok')
        if scope['qualification']:
            row['sample_role']='qualification';run.append(work/'qualification.jsonl',row);counts['qualification_queries']+=1
        else:run.append(work/'queries.jsonl',row);counts['queries']+=1
        report.update(query_records=counts['queries'],expected_failure_records=counts['expected_failures'],mounts=counts['mounts'])
        atomic_json(work/'report.json',report)
    def emit_session(*,case,method,campaign,q,prepare_reuse,run:object,audit,context):
        from experiments.fixed_matrix import run as api
        rb=telemetry.sample(paths);api.validate_sample(rb)
        deferred=[];session=run(deferred)
        ra=telemetry.sample(paths);api.validate_sample(ra)
        need(len(deferred)==q and len(session['observations'])==q,'session query coverage differs')
        for row,owned in deferred:
            row.update(outer_query_total_ns=row['total_ns'],outer_discovery_ns=row['discovery_ns'],
                outer_value_and_release_ns=row['value_and_delivery_ns'],
                total_ns=row['reader_total_ns'],discovery_ns=row['t1_ns']-row['t0_ns'],value_ns=row['t2_ns']-row['t1_ns'])
            need(row['total_ns']==row['t2_ns']-row['t0_ns']==row['discovery_ns']+row['value_ns'],'session reader clocks differ')
            row['validation']=audit(row,owned);row['oracle_verified']=True
        deferred.clear()
        if context:_profile(context,session['observations'],case,method)
        session.update(stamp(case,method,campaign),q=q,prepare_reuse=prepare_reuse,
            resource_before=rb,resource_after=ra,oracle_verified=True)
        # Each query clock plus genuine session wall remains independently recorded.
        api.append(work/'sessions.jsonl',session)
        counts['sessions']+=1
        report.update(session_records=counts['sessions'])
        atomic_json(work/'report.json',report)
    stage=proof['stage']
    if stage=='main':read_collection.collect_main(repo,bindings,mount_factory,measure)
    elif stage=='extensions':extensions_collection.collect_extensions(repo,bindings,mount_factory,measure)
    elif stage=='sessions':session_collection.collect_sessions(repo,bindings,mount_factory,emit_session)
    elif stage in ('lifecycle','lifecycle-c0','lifecycle-c1','lifecycle-c2'):
        collect_lifecycles(repo,work,proof,env,paired,paths,binary,report,make_launcher,counts)
    elif stage in ('all','all-read'):
        scope['qualification']=True
        smoke(repo,bindings,mount_factory,measure)
        scope['qualification']=False
        read_collection.collect_main(repo,bindings,mount_factory,measure)
        extensions_collection.collect_extensions(repo,bindings,mount_factory,measure)
        session_collection.collect_sessions(repo,bindings,mount_factory,emit_session)
        if stage=='all':collect_lifecycles(repo,work,proof,env,paired,paths,binary,report,make_launcher,counts)
    elif stage=='smoke':smoke(repo,bindings,mount_factory,measure)
    else:raise ValueError('unknown collection stage')
    need(run.infrastructure.journal_snapshot(master)==journal_digest,'master owner state changed')
    head_after,_=run.head(head_probe,namespace,env,work/'head-after');need(head_after==head_before,'query mutated publication head')
    report.update(counts,complete=True,head_unchanged=True,master_journal_unchanged=True)


def smoke(repo,bindings,mount_factory,measure):
    from .read_matrix_native import NativeHdf5,NativeGrib
    allcases=rm.main_cases(repo,bindings[0])['cases']
    selected=[c for c in allcases if c['case_id'] in ('H-F1-T2','H-F2-R192','G-F1-M22','G-F3-B64901')]
    h=NativeHdf5(repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310');g=NativeGrib('/lib/x86_64-linux-gnu/libeccodes.so.0')
    items=[]
    for case in selected:
        audit=lambda row,owned,s=case:rm.validate_owned(s,owned)
        items.append((case,audit,lambda s=case:(h if s['format_id']=='hdf5' else g).read(s),lambda ctx,s=case:rm.read_iotdb(ctx,s)))
    # Small representative paths qualify the scientific and Arrow schemas;
    # the complete supplement loop retains every formal observation separately.
    sources=extensions_collection.source_registry()
    for c in extensions_collection.case_registry():
        if c['id'] not in ('A-F4-ETT7-C1-R128','A-F4-E370-C370-R128','H-F8-GEOS-R1','H-F8-OISST-R64','A-F9-E370-C370-R4096'):continue
        if c.get('variant','default')=='none':continue
        source=dict(sources[c['source']],**bindings[0][c['source']]);spec=dict(source,**c,source_id=c['source'],case_id=c['id'],format_id=c['format'])
        expected,meta=extensions_collection._oracle(source,c)
        def audit(row,owned,case=c,s=source,exp=expected):
            if 'catalog' in row:er.validate_typed(row,s,case)
            actual=er.digest_owned(case,owned);need(actual==exp,'smoke oracle mismatch');return actual
        reader=er.ArrowLocalReader(source,'FS-native') if c['format']=='arrow' else er.ScientificNativeReader(h,source);reader.prepare()
        items.append((spec,audit,lambda r=reader,case=c:extensions_collection._read(r,case),lambda ctx,s=source,case=c:extensions_collection._read_iotdb(ctx,s,case)))
    for spec,audit,native,product in items:
        measure(case=spec,method='native FS',campaign=0,repeat=0,read=native,audit=audit,context=None)
    for mode in ('iotdb-indexed','native-hdf5'):
        with mount_factory(0,'smoke-all-'+mode,mode) as ctx:
            for spec,audit,native,product in items:
                if mode=='native-hdf5' and spec['format_id']!='hdf5':continue
                measure(case=spec,method=mode,campaign=0,repeat=0,read=lambda call=product:call(ctx),audit=audit,context=ctx)


def analyze(work):
    stage=read_json(work/'preflight.json')['stage']
    query_path=work/'queries.jsonl';session_path=work/'sessions.jsonl'
    rows=[json.loads(s) for s in query_path.read_text().splitlines()] if query_path.exists() else []
    sessions=[json.loads(s) for s in session_path.read_text().splitlines()] if session_path.exists() else []
    failures=[json.loads(s) for s in (work/'failures.jsonl').read_text().splitlines()] if (work/'failures.jsonl').exists() else []
    cells=[]
    for key in sorted({(r['case_id'],r['variant'],r['method']) for r in rows}):
        rr=[r for r in rows if (r['case_id'],r['variant'],r['method'])==key]
        campaigns=[]
        for c in sorted({r['campaign'] for r in rr}):
            block=[r for r in rr if r['campaign']==c];warm=[r for r in block if r['phase']=='measurement']
            if warm:need(len(warm)==10 and len(block)==13,'warm cell incomplete')
            campaigns.append(dict(campaign=c,queries=len(block),warm_count=len(warm),
                first_T_ms=[r['total_ns']/1e6 for r in block if r['phase']=='first'],
                warm_median_ms={k:statistics.median(r[k] for r in warm)/1e6 for k in ('total_ns','discovery_ns','value_ns')} if warm else None))
        med={k:statistics.median(c['warm_median_ms'][k] for c in campaigns if c['warm_median_ms']) for k in ('total_ns','discovery_ns','value_ns')} if any(c['warm_median_ms'] for c in campaigns) else None
        cells.append(dict(case_id=key[0],variant=key[1],method=key[2],queries=len(rr),campaigns=campaigns,warm_median_ms=med))
    for cell in cells:
        if cell['method']=='native FS' or cell['warm_median_ms'] is None:continue
        reference=next(c for c in cells if (c['case_id'],c['variant'],c['method'])==(cell['case_id'],cell['variant'],'native FS'))
        ratios={}
        for label,key in [('R','total_ns'),('Rd','discovery_ns'),('Rq','value_ns')]:
            vals=[]
            for c in cell['campaigns']:
                ref=next(r for r in reference['campaigns'] if r['campaign']==c['campaign'])
                den=ref['warm_median_ms'][key]
                if den>0:vals.append(c['warm_median_ms'][key]/den)
            ratios[label]=statistics.median(vals) if vals else None
        cell['ratios_vs_native_FS']=ratios
    session_cells=[]
    for key in sorted({(s['case_id'],s['method'],s['prepare_reuse']) for s in sessions}):
        rr=[s for s in sessions if (s['case_id'],s['method'],s['prepare_reuse'])==key]
        need(len(rr)==3 and {s['campaign'] for s in rr}=={0,1,2},'independent session campaigns incomplete')
        session_cells.append(dict(case_id=key[0],method=key[1],prepare_reuse=key[2],count=3,
            median_ms={k:statistics.median(s[k] for s in rr)/1e6 for k in ('session_wall_ns','prepare_ns','release_ns','query_sum_ns')},
            raw_session_ns=[s['session_wall_ns'] for s in rr]))
    lifecycles=[json.loads(s) for s in (work/'lifecycles.jsonl').read_text().splitlines()] if (work/'lifecycles.jsonl').exists() else []
    qualification=[json.loads(s) for s in (work/'qualification.jsonl').read_text().splitlines()] if (work/'qualification.jsonl').exists() else []
    coverage=run.validate_coverage(work,rows+sessions+failures+lifecycles+qualification) if (rows or sessions or failures or lifecycles or qualification) else None
    lifecycle_cells=[]
    lifecycle_campaigns=()
    if stage == 'all' or stage.startswith('lifecycle'):
        lifecycle_campaigns=lifecycle_collection.campaigns_for_stage(stage)
        declared=read_json(work/'preflight.json').get('lifecycle_campaigns')
        need(declared is None or declared==list(lifecycle_campaigns),'preflight lifecycle campaign scope differs')
        expected_cases={case.replace('-F1','-F6') for case in lifecycle_collection.BASE_CASES}
        expected_keys={(campaign,case,method,q) for campaign in lifecycle_campaigns for case in expected_cases
            for method in ('native FS','IoTDB-FS') for q in (1,10,100)}
        actual=[(r['campaign'],r['case_id'],r['method'],r['q']) for r in lifecycles]
        need(len(actual)==len(set(actual)) and set(actual)==expected_keys,'lifecycle declared scope has missing/duplicate/unexpected rows')
        need(len(lifecycles)==30*len(lifecycle_campaigns) and sum(r['q'] for r in lifecycles)==1110*len(lifecycle_campaigns),
            'lifecycle query coverage differs from declared stage')
        need(all(r.get('oracle_verified') is True and r['session']['q']==r['q'] and
            len(r['session']['observations'])==r['q'] for r in lifecycles),'lifecycle session/validation coverage differs')
    else:need(not lifecycles,'non-lifecycle stage contains lifecycle records')
    for key in sorted({(r['case_id'],r['method'],r['q']) for r in lifecycles}):
        rr=sorted([r for r in lifecycles if (r['case_id'],r['method'],r['q'])==key],key=lambda r:r['campaign'])
        need(len(rr)==len(lifecycle_campaigns) and {r['campaign'] for r in rr}==set(lifecycle_campaigns),'lifecycle coverage differs')
        lifecycle_cells.append(dict(case_id=key[0],method=key[1],q=key[2],count=len(rr),
            campaigns=[r['campaign'] for r in rr],
            aggregation='single-campaign observation' if len(rr)==1 else 'median across declared completed campaigns',
            lifecycle_median_ms=statistics.median(r['lifecycle_wall_ns'] for r in rr)/1e6,
            write_median_ms=statistics.median(r['write']['total_ns'] for r in rr)/1e6,
            source_locator_median_ms=statistics.median(r['source_locator_ns'] for r in rr)/1e6,
            session_median_ms=statistics.median(r['session']['session_wall_ns'] for r in rr)/1e6,
            raw_lifecycle_ms=[r['lifecycle_wall_ns']/1e6 for r in rr],
            raw_lifecycle_by_campaign=[dict(campaign=r['campaign'],lifecycle_ms=r['lifecycle_wall_ns']/1e6) for r in rr]))
    return dict(complete=True,stage=stage,lifecycle_campaigns=list(lifecycle_campaigns),
        lifecycle_expected_records=30*len(lifecycle_campaigns),lifecycle_expected_queries=1110*len(lifecycle_campaigns),
        queries=len(rows),qualification_queries=len(qualification),sessions=len(sessions),session_queries=sum(s['q'] for s in sessions),
        expected_failures=len(failures),lifecycles=len(lifecycles),lifecycle_queries=sum(r['q'] for r in lifecycles),lifecycle_cells=lifecycle_cells,cells=cells,session_cells=session_cells,resource_coverage=coverage,
        transport_note='main GRIB/HDF5 uses result_fetch; Arrow/scientific supplements use complete immutable FUSE IPC read',
        daemon_lifetime='retained per phase/campaign/execution mode; fragment/metadata caches can carry across case/Q blocks; first is case repeat0, not cold',
        aggregation='three campaign warm medians; median of campaign ratios; first and warmups excluded from warm statistics',
        lifecycle_measured=bool(lifecycles))


def collect_lifecycles(repo,work,proof,env,paired,paths,binary,report,make_launcher,counts):
    campaigns=lifecycle_collection.campaigns_for_stage(proof['stage'])
    need(proof.get('lifecycle_campaigns',list(campaigns))==list(campaigns),'frozen lifecycle scope differs')
    writer=repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    @contextmanager
    def factory(campaign):
        block=work/('lifecycle-c%d'%campaign);block.mkdir()
        journal=block/'owner';native_root=block/'native-files';native_root.mkdir(mode=0o700)
        namespace=lifecycle_collection.lifecycle_namespace(proof['plan']['namespace'],work,campaign)
        make_launcher(block/'launcher',binary,namespace,journal,native_root,initialize=True,mode='iotdb-indexed')
        with paired.sf.private_mount(block/'launcher',block/'mount',env,writer,profile=True) as context:
            need(file_sha(Path('/proc')/str(context['pid'])/'exe')==proof['product_sha256'],'lifecycle product differs')
            context.update(profile_offset=0,max_result_bytes=67108864,hdf5_mode='iotdb-indexed')
            yield context
            need(not query_task_client._harvest(context,context['profile_offset'],0,False),'extra lifecycle profiles')
        counts['mounts']+=1
        run.append(work/'mounts.jsonl',dict(campaign=campaign,block='lifecycle',pid=context['pid'],mount=str(context['mount']),namespace=namespace,unmounted=not __import__('os').path.ismount(context['mount'])))
    def emit(**kwargs):
        rb=telemetry.sample(paths);run.validate_sample(rb)
        print(json.dumps(dict(event='lifecycle',case_id=kwargs['case']['case_id'],campaign=kwargs['campaign'],method=kwargs['method'],q=kwargs['q'])),flush=True)
        row,pending=lifecycle_collection.run_lifecycle_plan(**kwargs)
        ra=telemetry.sample(paths);run.validate_sample(ra)
        for record,owned in pending:
            record.update(outer_query_total_ns=record['total_ns'],outer_discovery_ns=record['discovery_ns'],
                total_ns=record['reader_total_ns'],discovery_ns=record['t1_ns']-record['t0_ns'],value_ns=record['t2_ns']-record['t1_ns'])
            need(record['total_ns']==record['discovery_ns']+record['value_ns'],'lifecycle query clocks differ')
            record['validation']=kwargs['query_audit'](record,owned);record['oracle_verified']=True
        pending.clear()
        if kwargs['context']:
            profiles=_profile(kwargs['context'],row['session']['observations'],kwargs['case'],'iotdb-indexed',leading_catalogs=2)
            row['source_visibility_profiles']=[p for p in profiles if p['operation']!='typed'][:2]
        row.update(resource_before=rb,resource_after=ra,oracle_verified=True,
            product_sha256=proof['product_sha256'] if kwargs['method']!='native FS' else None)
        run.append(work/'lifecycles.jsonl',row)
        counts['lifecycles']=counts.get('lifecycles',0)+1;report.update(lifecycle_records=counts['lifecycles']);atomic_json(work/'report.json',report)
    lifecycle_collection.collect_lifecycles(repo,work,factory,emit,campaigns=campaigns)
    report.update(lifecycle_campaigns=list(campaigns),lifecycle_expected_records=30*len(campaigns),
        lifecycle_expected_queries=1110*len(campaigns))
