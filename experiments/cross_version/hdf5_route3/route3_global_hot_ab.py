#!/usr/bin/env python3
"""Fresh two-binary global-cache A/B on the existing N512/V1 namespace.

One retained mount per binary serves a 128-source selective probe, then the
512-source matrix. The probe uses four existing buckets, not a separate N128
publication. All five paths produce actual owned arrays inside their clocks.
"""
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
import argparse
import json
import statistics
import subprocess
import time
import uuid

from experiments.cross_version.hdf5_route3 import route3_selector_ab as ab

HELPER_SHA='7788de55a7c5cfd6c2be6ef6b17ced22e0792621ac9317372322cea7afce8d6f'
BASELINE_SHA='0fe78116bddb4b8c783cc0ea88c1e837b469c9e434e4e7fea371ea57aed91925'


def pilot_buckets(manifest,predicate):
    buckets={Path(e['logical_file']).parent.as_posix() for e in manifest['entries']}
    matches={Path(e['logical_file']).parent.as_posix() for e in manifest['entries']
             if any(u['attributes']==predicate for u in e['units'])}
    ab.require(len(buckets)==16 and len(matches)<=4,'unexpected frozen bucket topology')
    selected=set(matches)
    for bucket in sorted(buckets):
        if len(selected)==4:break
        selected.add(bucket)
    entries=[e for e in manifest['entries'] if Path(e['logical_file']).parent.as_posix() in selected]
    ab.require(len(entries)==128 and all(sum(Path(e['logical_file']).parent.as_posix()==b for e in entries)==32 for b in selected),
               'probe must select exactly four complete buckets/128 sources')
    return sorted(selected)


def scoped_discover(remote,context,prefix,predicate,paths):
    request=remote.discovery_request(prefix,predicate,'latest',None)
    request['request']['sources']['paths']=paths
    response,_,stats,_=remote._call(context,request,None,False,True)
    rows=[];seen=set()
    for item in response['datasets']:
        row=dict(path=item['path'],version=item['version'],dataset=item['dataset_path'])
        remote._logical(row['path'])
        ab.require(type(row['version']) is int and row['version']>=0 and
            any(row['path']==s['path'] or row['path'].startswith(s['path'].rstrip('/')+'/') for s in paths),
            'discovery escaped the frozen source scope')
        identity=(row['path'],row['version'],row['dataset'])
        ab.require(identity not in seen,'duplicate discovery result');seen.add(identity);rows.append(row)
    return rows,stats


def timed_query(arm,native,context,case,scope,modules):
    """Same owned-output endpoints as frozen route3_run, with explicit scopes."""
    remote,data,frozen=(modules[n] for n in ('route3_iotdb','route3_data','route3_run'))
    before=native.stats() if native else None
    calls=[];discovery=None;origin=[0,0,0,0]
    started=time.perf_counter_ns()
    if arm.startswith('FS-'):
        discovery=native.select('scan' if arm=='FS-scan' else 'index',{'kind':'latest'},data.PREDICATE)
        rows=[dict(path=r['logical_path'],version=r['version'],dataset=r['dataset'],native_path=r['path']) for r in discovery['units']]
    else:
        rows,stats=scoped_discover(remote,context,case['report']['logical_prefix'],
                                  remote.equality_predicate(data.PREDICATE),scope['paths'])
        calls.append(stats)
    ready=time.perf_counter_ns();value_calls=[]
    if arm=='typed':units,value_calls,_=remote.typed_values(context,rows,origin,list(data.ROI))
    else:
        units=[]
        for row in rows:
            path=remote.native_path(context,row) if arm=='catalog-native-fuse' else (
                case['bindings'][row['path'],row['version']]['source'] if arm=='catalog-native-local' else row['native_path'])
            values,mask=native.read(path,row['dataset'],origin,list(data.ROI))
            units.append(dict(row,array=values,mask=mask))
    ended=time.perf_counter_ns()
    instrumentation=sum(c.get('instrumentation_ns',0) for c in calls+value_calls)
    record=dict(arm=arm,selection='latest',origin=origin,shape=list(data.ROI),
        total_ns=ended-started-instrumentation,raw_client_wall_ns=ended-started,instrumentation_ns=instrumentation,
        discovery_client_ns=ready-started-sum(c.get('instrumentation_ns',0) for c in calls),
        selected_value_client_ns=ended-ready-sum(c.get('instrumentation_ns',0) for c in value_calls),
        discovery_calls=calls,value_calls=value_calls,native_discovery=discovery,
        native_counters=modules['route3_native'].counter_delta(before,native.stats()) if native else None,
        timestamp_fence=None,selected_rounds=[1],selector_paths=scope['paths'])
    record.update(frozen.validate_units(units,case['bindings'],case['expected']['latest',(0,0,0,0)],origin))
    return record


def summary(rows):
    cells=[]
    for binary,scope,arm in sorted({(r['binary'],r['scope'],r['arm']) for r in rows}):
        selected=[r for r in rows if (r['binary'],r['scope'],r['arm'])==(binary,scope,arm)]
        warm=[r for r in selected if r['repeat']>0]
        campaigns=sorted({r['campaign'] for r in selected})
        cells.append(dict(binary=binary,scope=scope,arm=arm,N=selected[0]['N'],backing_N=512,V=1,
            samples=len(selected),warm_samples=len(warm),warm_p50_ms=statistics.median(r['total_ns'] for r in warm)/1e6,
            discovery_p50_ms=statistics.median(r['discovery_client_ns'] for r in warm)/1e6,
            value_p50_ms=statistics.median(r['selected_value_client_ns'] for r in warm)/1e6,
            first_ms=[r['total_ns']/1e6 for r in selected if r['repeat']==0],
            campaign_warm_p50_ms=[statistics.median(r['total_ns'] for r in warm if r['campaign']==c)/1e6 for c in campaigns]))
    comparisons=[]
    for scope,arm in sorted({(r['scope'],r['arm']) for r in rows}):
        pair={c['binary']:c for c in cells if (c['scope'],c['arm'])==(scope,arm)}
        ab.require(set(pair)=={'baseline','candidate'},'incomplete fresh A/B cell')
        comparisons.append(dict(scope=scope,arm=arm,baseline_ms=pair['baseline']['warm_p50_ms'],
            candidate_ms=pair['candidate']['warm_p50_ms'],candidate_over_baseline=pair['candidate']['warm_p50_ms']/pair['baseline']['warm_p50_ms']))
    return dict(cells=cells,comparisons=comparisons,
        scope='210 fresh owned queries; probe C128 and full C512 share one N512/V1 backing namespace; no inherited rows; baseline then candidate retained mount blocks; temporal campaigns are not independent replication')


def run_probe(args,report,binary,digest,name,ab_context=False):
    root=args.work/'probes'/name;root.mkdir(mode=0o700,parents=True)
    if ab_context:
        receipt={}
        ab.context_probe(SimpleNamespace(context_probe=binary,context_probe_sha=digest,
            context_probe_timeout=1800,work=root,report=root/'report.json',env=args.env),receipt)
        report.setdefault('probes',{})[name]=receipt['context_probe']
        result=ab.load(root/'context-probe/result.json')
        ab.require(result.get('metadata_budget_observations',{}).get('membership_fixture_with_rejections_bytes')==11118,
                   'retained context fixture fees differ from accepted C baseline')
    else:
        ab.require(ab.sha(binary)==digest,'global probe binary SHA mismatch')
        prefix='vd_hot_'+uuid.uuid4().hex;journal=root/'owner'
        command=[str(binary),'--prefix',prefix,'--journal',str(journal)]
        started=time.perf_counter_ns()
        with (root/'stdout.private.log').open('x') as out,(root/'stderr.private.log').open('x') as err:
            process=subprocess.run(command,env=args.env,stdin=subprocess.DEVNULL,stdout=out,stderr=err,timeout=1800,text=True)
        records=[json.loads(x) for x in (root/'stdout.private.log').read_text().splitlines() if x.startswith('{')]
        ab.require(process.returncode==0 and len(records)==1,'actual global cache probe failed')
        result=records[0]
        ab.require(result.get('completed') is True and result.get('prefix')==prefix and result.get('journal')==str(journal),
                   'actual global probe completion identity mismatch')
        ab.save(root/'result.json',result)
        receipt=dict(complete=True,binary_sha256=digest,prefix=prefix,journal=str(journal),command=command,
            elapsed_ns=time.perf_counter_ns()-started,exit_code=process.returncode,result_sha256=ab.sha(root/'result.json'),
            stdout_sha256=ab.sha(root/'stdout.private.log'),stderr_sha256=ab.sha(root/'stderr.private.log'))
        ab.save(root/'receipt.json',receipt);report.setdefault('probes',{})[name]=receipt
        import audit_probe
        ab.require(ab.sha(audit_probe.__file__)=='f4d79520d3c7f0d9779c5bb713ec6c089bc16a173078821798d8bfb5c1d6cf06',
                   'independent probe auditor changed')
        audit=audit_probe.audit(root/'result.json',root/'receipt.json',args.global_probe_source,digest)
        ab.save(root/'audit.json',audit)
    ab.save(args.report,report)


def run(args):
    ab.require(ab.sha(ab.__file__)==HELPER_SHA,'frozen shared helper changed')
    args.repo=args.repo.resolve(strict=True);args.source_stage=args.source_stage.resolve(strict=True)
    args.work=args.work.resolve();ab.require(not args.work.exists(),'fresh work required')
    binaries={label:(path.resolve(strict=True),digest) for label,path,digest in (
        ('baseline',args.baseline,BASELINE_SHA),('candidate',args.candidate,args.candidate_sha))}
    ab.require(all(ab.sha(p)==h for p,h in binaries.values()) and len({h for p,h in binaries.values()})==2,'binary identities differ')
    library=args.native_library.resolve(strict=True);ab.require(ab.sha(library)==ab.NATIVE_SHA,'native helper changed')
    modules=ab.frozen_modules(args.source_stage,args.repo)
    data,remote,frozen=(modules[n] for n in ('route3_data','route3_iotdb','route3_run'))
    case=ab.prepare_case(args.source_stage,'n512r1','full',data,roi='aligned')
    args.work.mkdir(mode=0o700,parents=True);args.report=args.work/'report.json'
    prefix=case['report']['logical_prefix'];buckets=pilot_buckets(case['manifest'],data.PREDICATE)
    scopes=[dict(name='probe128',N=128,campaigns=2,repeats=3,
                 paths=[dict(kind='subtree',path=prefix+'/'+b) for b in buckets]),
            dict(name='full512',N=512,campaigns=3,repeats=5,paths=[dict(kind='subtree',path=prefix)])]
    pilot=args.work/'native-probe';pilot.mkdir()
    source_catalog=ab.load(case['root']/'phase-v1/catalog.json')
    source_catalog['sources']=[r for r in source_catalog['sources'] if Path(r['logical_path']).parent.name in buckets]
    ab.require(len(source_catalog['sources'])==128,'native pilot source count differs')
    ab.save(pilot/'catalog.json',source_catalog)
    pilot_config=dict(case['native_config'],catalog_path=str(pilot/'catalog.json'),index_path=str(pilot/'catalog.sqlite'))
    setup=time.perf_counter_ns()
    with modules['route3_native'].NativeContext(library,dict(pilot_config,build_index=True)) as builder:pilot_stats=builder.stats()
    scopes[0]['native_config']=pilot_config;scopes[1]['native_config']=case['native_config']
    report=dict(protocol='iotdbfs-global-hot-ab/1',complete=False,query_records=0,inherited_query_rows=0,
        expected_queries=210,runner_sha256=ab.sha(__file__),shared_helper_sha256=HELPER_SHA,native_sha256=ab.NATIVE_SHA,
        source_manifest_sha256=ab.sha(args.source_stage/'source-r2-manifest.json'),
        binaries={k:dict(path=str(p),sha256=h) for k,(p,h) in binaries.items()},scopes=scopes,mounts=[],
        pilot_index_build_ns=time.perf_counter_ns()-setup,pilot_index_stats=pilot_stats,
        pilot_catalog_sha256=ab.sha(pilot/'catalog.json'),pilot_index_sha256=ab.sha(pilot/'catalog.sqlite'),
        source_case={k:case[k] for k in ('name','N','stored_V','journal_digest','input_report_sha256','input_manifest_sha256','full_raw_validation_ns')},
        limits='existing 1024-source/64MiB request metadata; no corpus publication, initialized namespace, or budget override',
        timing='full client owned values/mask; profile wait and oracle excluded; index/context setup and mount/recovery separate',
        limitation='C128 is a selective four-bucket subset of backing N512/V1, not an independently published N128 population; one retained mount per binary, no cold-disk or independent replication claim')
    args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text())
    _,publisher=remote._dependencies()
    _,_,args.env=publisher.runtime(args.repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publisher.DATABASE)
    ab.require(args.env['IOTDB_HOST'] in ('localhost','127.0.0.1') and args.env['IOTDB_PORT']=='33667','original endpoint differs')
    paired=modules['paired'];modules['v03_startup_wrapper'].adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    writer_lock=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    inspector=args.repo/'experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe'
    head_probe=modules['n8_matrix_prepare']._probe;rows=[];started=time.monotonic()
    ab.save(args.report,report)
    try:
        with modules['route3_control'].sync_window(args,report):
            run_probe(args,report,args.context_probe,args.context_probe_sha,'v03-context',True)
            run_probe(args,report,args.global_probe,args.global_probe_sha,'global-hot')
            for label,(binary,digest) in binaries.items():
                root=args.work/label;root.mkdir();journal=root/'owner'
                ab.copy_journal(case['owner'],journal,case['journal_digest'])
                for name in ('head-before','head-after'):(root/name).mkdir()
                before,receipt=head_probe(inspector,'head',case['report']['prefix'],args.env,root/'head-before')
                ab.require(before==case['phase']['head'],'scientific head changed')
                launch=ab.create_launcher(modules['v03_n128'],root/'launcher',binary,digest,case['report']['prefix'],journal)
                mount=dict(binary=label,head_before=before,head_before_receipt=receipt,launcher=launch)
                report['mounts'].append(mount);ab.save(args.report,report);tick=time.perf_counter_ns()
                with paired.sf.private_mount(Path(launch['path']),root/'mount',args.env,writer_lock,profile=True) as context:
                    mount.update(startup_ns=context['startup_ns'],startup_wall_ns=time.perf_counter_ns()-tick,daemon_pid=context['pid'])
                    context['route3_profile_offset']=remote.reset_profile_offset(context)
                    for scope in scopes:
                        begin=time.perf_counter_ns()
                        with ExitStack() as stack:
                            native={arm:stack.enter_context(modules['route3_native'].NativeContext(library,scope['native_config'])) for arm in frozen.ARMS if arm!='typed'}
                            mount.setdefault('native_context_setup_ns',{})[scope['name']]=time.perf_counter_ns()-begin
                            for campaign in range(scope['campaigns']):
                                order=frozen.ARMS[campaign%5:]+frozen.ARMS[:campaign%5]
                                for arm in order:
                                    for repeat in range(scope['repeats']):
                                        row=timed_query(arm,native.get(arm),context,case,scope,modules)
                                        row.update(binary=label,scope=scope['name'],case='n512r1',N=scope['N'],backing_N=512,V=1,
                                                   campaign=campaign,repeat=repeat)
                                        row['profiled_sdk_query_calls']=ab.warm_sdk_queries(row)
                                        if repeat>0:ab.require(row['profiled_sdk_query_calls'] in (None,0),'warm socket issued SQL')
                                        ab.append(args.work/'queries.jsonl',row);rows.append(row);report['query_records']=len(rows)
                                report['last_group']=dict(binary=label,scope=scope['name'],campaign=campaign)
                                ab.save(args.report,report);print(json.dumps(dict(event='group',rows=len(rows),**report['last_group'])),flush=True)
                after,receipt=head_probe(inspector,'head',case['report']['prefix'],args.env,root/'head-after')
                ab.require(after==before and ab.journal_snapshot(journal)==ab.journal_snapshot(case['owner'])==case['journal_digest'],
                           'scientific head/journals changed')
                mount.update(complete=True,head_after=after,head_after_receipt=receipt,head_unchanged=True,journals_unchanged=True)
                ab.save(args.report,report)
            ab.require(len(rows)==210 and all(r['oracle_verified'] for r in rows),'fresh matrix incomplete')
            ab.require(ab.sha(pilot/'catalog.json')==report['pilot_catalog_sha256'] and
                       ab.sha(pilot/'catalog.sqlite')==report['pilot_index_sha256'],'pilot native catalog/index changed')
            for cfg in [pilot_config,case['native_config']]:
                ab.require(Path(cfg['catalog_path']).is_file() and Path(cfg['index_path']).is_file(),'native artifact missing')
            ab.require(ab.sha(case['root']/'phase-v1/catalog.json')==case['phase']['catalog_sha256'] and
                ab.sha(case['root']/'phase-v1/catalog.sqlite')==case['phase']['index_sha256_after'] and
                ab.sha(case['root']/'report.json')==case['input_report_sha256'] and
                ab.sha(case['root']/'raw/manifest.json')==case['input_manifest_sha256'],'original corpus/index evidence changed')
            ab.save(args.work/'analysis.json',summary(rows))
            report.update(complete=True,queries_sha256=ab.sha(args.work/'queries.jsonl'),analysis_sha256=ab.sha(args.work/'analysis.json'))
    except BaseException as error:
        report.update(complete=False,failure=dict(type=type(error).__name__,message='stopped; original safety window restores service; no retry'))
        raise
    finally:
        report['wall_seconds']=time.monotonic()-started;ab.save(args.report,report)
    print(json.dumps(dict(complete=report['complete'],queries=len(rows),restored=report.get('original_service_restored'))),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['repo','source-stage','work','baseline','candidate','native-library','context-probe','global-probe','global-probe-source']:
        p.add_argument('--'+name,type=Path,required=True)
    for name in ['candidate-sha','context-probe-sha','global-probe-sha']:p.add_argument('--'+name,required=True)
    run(p.parse_args())
