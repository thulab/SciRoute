#!/usr/bin/env python3
"""Selective HDF5 route-3 study on the authorized original database.

Each invocation creates one new namespace and a unique output tree. Publication
is real and ordinal-major; no answer-specific catalog or metadata-only versions.
All five arms end at actual owned NumPy value/mask arrays in this client.
"""
from pathlib import Path
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
import statistics
import sys
import time
from types import SimpleNamespace

from experiments.cross_version.hdf5_route3 import route3_data as data
from experiments.cross_version.hdf5_route3 import route3_iotdb as remote
from experiments.cross_version.hdf5_route3.route3_native import NativeContext, counter_delta

ARMS=('FS-scan','FS-index','catalog-native-local','catalog-native-fuse','typed')
ROIS=((0,0,0,0),(0,0,16,16))
NATIVE_SHA=None  # Exact binary identity is supplied and frozen by the launcher.

def append(path, row):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(row,sort_keys=True,allow_nan=False)+'\n')
        stream.flush();os.fsync(stream.fileno())

def require(value, message):
    if not value:raise RuntimeError(message)

def selections(V,N,pilot=False):
    out=[{'name':'latest','kind':'latest','rounds':[V]}]
    if V>1:
        middle=max(1,V//2)
        out.append({'name':'historical-one','kind':'between','rounds':[middle]})
        if N<=512:
            first=max(1,middle-1)
            if pilot:first=1
            out.append({'name':'historical-two','kind':'between','rounds':[first,first+1]})
    return out

def freeze_selection(item,rounds):
    if item['kind']=='latest':return dict(item,fence=None)
    selected=[r for r in rounds if r['ordinal'] in item['rounds']]
    require(len(selected)==len(item['rounds']),'selected round receipt missing')
    return dict(item,fence=[selected[0]['fence'][0],selected[-1]['fence'][1]])

def native_selection(selection):
    return {'kind':'latest'} if selection['kind']=='latest' else {
        'kind':'between','lower':selection['fence'][0],'upper':selection['fence'][1]}

def validate_units(units,bindings,expected,origin):
    import numpy as np
    seen=set();rows=0;output_bytes=0
    for unit in units:
        key=(unit['path'],unit['version'])
        require(key in bindings,'output refers to an unrecorded published source')
        binding=bindings[key]
        identity=(binding['logical_file'],binding['ordinal'],unit['dataset'])
        require(identity in expected and identity not in seen,'wrong/duplicated output source/dataset')
        seen.add(identity);oracle=expected[identity]
        values,mask=unit['array'],unit['mask']
        require(values.dtype==np.dtype('<f4') and mask.dtype==np.dtype('u1'),'owned output dtype differs')
        require(values.flags.c_contiguous and mask.flags.c_contiguous and values.size==mask.size==oracle['elements'],'owned output shape differs')
        require(hashlib.sha256(values.tobytes()).hexdigest()==oracle['value_sha256'],'native independent value oracle differs')
        require(hashlib.sha256(mask.tobytes()).hexdigest()==oracle['mask_sha256'],'native independent mask oracle differs')
        rows+=values.size;output_bytes+=values.nbytes+mask.nbytes
    require(seen==set(expected),'selected historical result set differs')
    return {'selected_units':len(units),'selected_owned_rows':rows,'owned_output_bytes':output_bytes,
            'oracle_verified':True,'oracle_identity':'logical path, actual published version, dataset and ROI'}

def timed_query(arm,native,context,prefix,predicate,selection,origin,bindings,expected):
    before=native.stats() if native else None
    calls=[];native_discovery=None
    started=time.perf_counter_ns()
    if arm in ('FS-scan','FS-index'):
        native_discovery=native.select('scan' if arm=='FS-scan' else 'index',native_selection(selection),data.PREDICATE)
        rows=[{'path':r['logical_path'],'version':r['version'],'dataset':r['dataset'],
               'native_path':r['path']} for r in native_discovery['units']]
    else:
        rows,stats,_=remote.discover(context,prefix,predicate,selection['kind'],selection['fence'])
        calls.append(stats)
    ready=time.perf_counter_ns()
    value_calls=[]
    if arm=='typed':
        units,value_calls,_=remote.typed_values(context,rows,list(origin),list(data.ROI))
    else:
        units=[]
        for row in rows:
            if arm=='catalog-native-fuse':path=remote.native_path(context,row)
            elif arm=='catalog-native-local':path=bindings[(row['path'],row['version'])]['source']
            else:path=row['native_path']
            values,mask=native.read(path,row['dataset'],list(origin),list(data.ROI))
            units.append({**row,'array':values,'mask':mask})
    ended=time.perf_counter_ns()
    # Profile waits and oracle hashing are outside the defined query clock.
    instrumentation=sum(c.get('instrumentation_ns',0) for c in calls+value_calls)
    row={'arm':arm,'selection':selection['name'],'origin':list(origin),'shape':list(data.ROI),
         'total_ns':ended-started-instrumentation,
         'raw_client_wall_ns':ended-started,'instrumentation_ns':instrumentation,
         'discovery_client_ns':ready-started-sum(c.get('instrumentation_ns',0) for c in calls),
         'selected_value_client_ns':ended-ready-sum(c.get('instrumentation_ns',0) for c in value_calls),
         'discovery_calls':calls,'value_calls':value_calls,'native_discovery':native_discovery,
         'native_counters':counter_delta(before,native.stats()) if native else None,
         'timestamp_fence':selection['fence'],'selected_rounds':selection['rounds']}
    row.update(validate_units(units,bindings,expected,origin))
    return row

def summarize(rows):
    groups={}
    for row in rows:groups.setdefault((row['V'],row['selection'],tuple(row['origin']),row['arm']),[]).append(row)
    cells=[]
    for key,samples in sorted(groups.items()):
        warm=[r for r in samples if r['repeat']>0]
        campaigns=sorted({r['campaign'] for r in samples})
        cells.append(dict(zip(('V','selection','origin','arm'),key),observations=len(samples),
            warm_observations=len(warm),warm_p50_ms=statistics.median(r['total_ns'] for r in warm)/1e6,
            campaign_warm_p50_ms=[statistics.median(r['total_ns'] for r in warm if r['campaign']==c)/1e6 for c in campaigns],
            first_by_campaign_ms=[next(r['total_ns']/1e6 for r in samples if r['campaign']==c and r['repeat']==0) for c in campaigns]))
    return {'protocol':'iotdbfs-route3-analysis/1','cells':cells,
        'timing':'client complete owned values/masks; profile wait and independent oracle outside; setup/ingestion/index separately',
        'unit':'temporal campaigns; pooled warm repeats descriptive, not independent datasets'}

def phase(args,context,report,N,V,manifest,bindings,rounds):
    directory=args.work/f'phase-v{V}';directory.mkdir()
    sources=[]
    for entry in manifest['entries']:
        logical=report['logical_prefix']+'/'+entry['logical_file']
        binding=bindings[(logical,entry['ordinal'])]
        path=Path(binding['source']);st=path.stat()
        sources.append({'logical_path':logical,'ordinal':entry['ordinal'],'version':binding['version'],
                        'path':str(path),'size':st.st_size,'mtime_ns':st.st_mtime_ns})
    data.save(directory/'catalog.json',{'protocol':'iotdbfs-route3-publication/1','sources':sources})
    native_config={'catalog_path':str(directory/'catalog.json'),'index_path':str(directory/'catalog.sqlite'),
        'cache_files':8,'cache_datasets':8,'metadata_cache_bytes':2097152,'raw_chunk_cache_bytes':262144}
    setup_started=time.perf_counter_ns()
    with NativeContext(args.native_library,{**native_config,'build_index':True}) as builder:
        index_stats=builder.stats()
    entry={'V':V,'N':N,'source_catalog':str(directory/'catalog.json'),
        'catalog_sha256':data.sha_file(directory/'catalog.json'),
        'index':str(directory/'catalog.sqlite'),'index_sha256_before':data.sha_file(directory/'catalog.sqlite'),
        'index_build_wall_ns':time.perf_counter_ns()-setup_started,'index_native_stats':index_stats,
        'native_options':native_config,'source_versions':len(sources),'queries':0,
        'selections':[freeze_selection(s,rounds) for s in selections(V,N,args.pilot)]}
    require(N*max(len(s['rounds']) for s in entry['selections'])<=remote.MAX_SOURCES,'candidate source budget would be exceeded')
    from experiments.cross_version.version_directory.v04_n128 import journal_digest
    from experiments.cross_version.version_matrix.n8_matrix_prepare import _probe
    inspector=args.repo/'experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe'
    (directory/'head-before').mkdir();(directory/'head-after').mkdir()
    head_before,_=_probe(inspector,'head',args.prefix,args.env,directory/'head-before')
    journal_before=journal_digest(args.journal)
    time.sleep(.05)  # Drain publication profiles before the formal clock.
    context['route3_profile_offset']=remote.reset_profile_offset(context)
    by_version={(b['path'],b['version']):b for b in bindings.values()}
    phase_rows=[]
    with ExitStack() as stack:
        native={a:stack.enter_context(NativeContext(args.native_library,native_config)) for a in ARMS if a!='typed'}
        entry['native_context_setup_ns']=time.perf_counter_ns()-setup_started-entry['index_build_wall_ns']
        report['phases'].append(entry);data.save(args.report,report)
        groups=[(s,o) for s in entry['selections'] for o in ROIS]
        for campaign in range(args.campaigns):
            ordered=groups[campaign%len(groups):]+groups[:campaign%len(groups)]
            arm_order=ARMS[campaign%len(ARMS):]+ARMS[:campaign%len(ARMS)]
            for selection,origin in ordered:
                expected=data.expected(manifest,selection['rounds'],origin)
                for arm in arm_order:
                    for repeat in range(args.repeats):
                        row=timed_query(arm,native.get(arm),context,report['logical_prefix'],
                            remote.equality_predicate(data.PREDICATE),selection,origin,by_version,expected)
                        row.update(N=N,V=V,campaign=campaign,repeat=repeat,pilot=args.pilot)
                        append(args.work/'queries.jsonl',row);phase_rows.append(row)
                        entry['queries']+=1;report['query_records']+=1
                report['last_query']={k:phase_rows[-1][k] for k in ('N','V','campaign','selection','origin','arm','repeat')}
                data.save(args.report,report)
                print(json.dumps({'event':'query_group','records':report['query_records'],**report['last_query']}),flush=True)
    expected_count=args.campaigns*args.repeats*len(ARMS)*len(groups)
    require(len(phase_rows)==expected_count,'phase query cardinality differs')
    head_after,_=_probe(inspector,'head',args.prefix,args.env,directory/'head-after')
    require(head_after==head_before and journal_digest(args.journal)==journal_before,'formal query changed owner head/journal')
    require(data.sha_file(directory/'catalog.sqlite')==entry['index_sha256_before'],'formal query changed native index')
    entry.update(head=head_before,head_verified=True,journal_unchanged=True,complete=True,
        index_sha256_after=data.sha_file(directory/'catalog.sqlite'))
    data.save(directory/'analysis.json',summarize(phase_rows));data.save(args.report,report)

def run(args):
    args.repo=args.repo.resolve();args.work=args.work.resolve();args.native_library=args.native_library.resolve()
    args.work.mkdir(mode=0o700,parents=True,exist_ok=False)
    args.report=args.work/'report.json';args.journal=args.work/'owner';args.journal.mkdir(mode=0o700)
    args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve()
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text())
    report={'protocol':'iotdbfs-route3-run/1','complete':False,'pilot':args.pilot,'N':args.N,
        'milestones':args.milestones,'D':4,'K':2,'roi':data.ROI,'origins':ROIS,
        'prefix':args.prefix,'logical_prefix':'/route3-'+args.prefix,'phases':[],
        'scientific_versions':0,'query_records':0,'credentials_recorded':False,
        'source_modules':{str(p):data.sha_file(p) for p in Path(__file__).parent.glob('route3_*.py')},
        'native_library':str(args.native_library),'native_sha256':data.sha_file(args.native_library),
        'campaigns':args.campaigns,'repeats':args.repeats,
        'arm_definitions':{'FS-scan':'generic version catalog then native HDF5 metadata scan; caller timestamp pruning honored',
          'FS-index':'general all-history SQLite metadata index then same native HDF5',
          'catalog-native-local':'production IoTDB-FS dataset discovery then native HDF5 over immutable local FS bytes; hybrid prototype',
          'catalog-native-fuse':'same production discovery then native HDF5 over exact original database/FUSE history bytes',
          'typed':'same production discovery then current production typed ROI and owned client IPC copy'},
        'design':'fixed K/ROI; real version-major publication; no unconditional All; no budget overrides'}
    data.save(args.report,report)
    require(data.sha_file(args.native_library)==args.native_sha,'native library identity differs')
    daemon=args.repo/'experiments/cross_version/work/v04-verification-r3-20261017/build/bin/iotdb_fs'
    environment=args.repo/'experiments/multi_file/work/runner-20261013/environment.json'
    from experiments.cross_version.version_directory.v03_startup_wrapper import load_runner,adjust_mount
    frozen=args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py'
    paired=load_runner(frozen)
    sys.modules['small_runner']=paired
    _,publisher=remote._dependencies()
    descriptor,_,env=publisher.runtime(environment,1001546,publisher.DATABASE)
    require(env['IOTDB_HOST'] in ('localhost','127.0.0.1') and env['IOTDB_PORT']=='33667','original database endpoint changed')
    args.env=env;report['runtime_descriptor_sha256']=data.sha_file(environment)
    report['python_library_versions']=descriptor['python_versions']
    from experiments.cross_version.hdf5_route3.route3_control import sync_window
    adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    lock=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    require(lock.exists(),'existing experimental writer lock absent')
    started=time.monotonic();bindings={};previous={};rounds=[]
    try:
        with sync_window(args,report):
            launch=remote.create_launcher(args.work/'launcher',daemon,args.prefix,args.journal,initialize=True)
            report['launcher']=launch;data.save(args.report,report)
            with remote.mount(launch,args.work/'mount',env,lock) as context:
                report['mount']={k:str(context[k]) if isinstance(context[k],Path) else context[k] for k in ('mount','root','pid','startup_ns','config_sha256')}
                for ordinal in range(1,max(args.milestones)+1):
                    generation=time.perf_counter_ns()
                    manifest=data.prepare_round(args.work/'raw',args.N,ordinal)
                    report['raw_preparation_ns']=report.get('raw_preparation_ns',0)+time.perf_counter_ns()-generation
                    report['raw_bytes']=manifest['physical_bytes'];report['raw_allocated_bytes']=manifest['allocated_bytes']
                    items=[{'source':str(args.work/'raw'/e['file']),
                            'path':report['logical_prefix']+'/'+e['logical_file'],'sha256':e['sha256']}
                           for e in manifest['entries'] if e['ordinal']==ordinal]
                    def checkpoint(binding):
                        relative=binding['path'][len(report['logical_prefix'])+1:]
                        binding['logical_file']=relative
                        bindings[(binding['path'],binding['ordinal'])]=binding
                        append(args.work/'bindings.jsonl',binding)
                        report['scientific_versions']+=1
                        report['last_published']={k:binding[k] for k in ('path','version','ordinal')}
                        data.save(args.report,report)
                        print(json.dumps({'event':'published','N':args.N,'round':ordinal,
                            'science_versions':report['scientific_versions'],'publication_ms':binding['publication_ns']/1e6,
                            'binding_ms':binding['binding_sha_ns']/1e6}),flush=True)
                    receipt=remote.publish_round(context,items,ordinal,previous,checkpoint=checkpoint)
                    rounds.append({k:v for k,v in receipt.items() if k!='bindings'})
                    data.save(args.work/'rounds.json',rounds)
                    if ordinal in args.milestones:
                        phase(args,context,report,args.N,ordinal,manifest,bindings,rounds)
                report['publication_rounds']=rounds
                report['native_source_manifest_sha256']=data.sha_file(args.work/'raw/manifest.json')
                report['bindings_sha256']=data.sha_file(args.work/'bindings.jsonl')
            records=[json.loads(x) for x in (args.work/'queries.jsonl').read_text().splitlines()]
            require(all(r['oracle_verified'] for r in records),'incomplete scientific oracle records')
            data.save(args.work/'analysis.json',summarize(records))
            report.update(complete=True,queries_sha256=data.sha_file(args.work/'queries.jsonl'))
    except BaseException as error:
        report.update(complete=False,failure={'type':type(error).__name__,
            'message':'route3 experiment stopped; private error log retained; no automatic resubmission'})
        raise
    finally:
        report['controller_wall_seconds']=time.monotonic()-started
        data.save(args.report,report)
    print(json.dumps({'complete':report['complete'],'records':report['query_records'],
        'science_versions':report['scientific_versions'],'original_service_restored':report.get('original_service_restored')}),flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--native-library',type=Path,required=True);parser.add_argument('--native-sha',required=True)
    parser.add_argument('--N',type=int,choices=[8,128,512,1024],required=True)
    parser.add_argument('--milestones',type=lambda s:[int(v) for v in s.split(',')],default=[1,8,32])
    parser.add_argument('--prefix',required=True);parser.add_argument('--campaigns',type=int,default=5)
    parser.add_argument('--repeats',type=int,default=5);parser.add_argument('--pilot',action='store_true')
    args=parser.parse_args()
    require(args.prefix.startswith('vd_v04_rt3_') and args.prefix.replace('_','').isalnum() and len(args.prefix)<64,'private route3 prefix required')
    require(args.milestones==sorted(set(args.milestones)) and all(1<=v<=32 for v in args.milestones),'invalid milestones')
    require(args.campaigns>=1 and args.repeats>=2,'insufficient repeats')
    run(args)
