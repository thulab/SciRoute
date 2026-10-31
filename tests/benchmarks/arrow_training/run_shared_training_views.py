#!/usr/bin/env python3
"""Controlled multi-view sharing experiment; serial requests, no concurrency claim.

Same immutable source, four genuinely different views, byte-identical outputs.
FS uses either four private decoded caches or one shared reader. Typed isolated
uses four fresh daemon lifetimes (sum reference); shared interleaves requests in
one runtime. Application cache cold only. No OS/DB cache dropping.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys
import time
import numpy as np

from derived_window_reader import DerivedWindowReader
from prepare_electricity_derived import load_source
from prepare_public_windows import seal
from public_iotdbfs import IoTDBFSWindowReader, WindowArrays
from run_cache_parallel_ablation import Mount
from summarize_query_profile import read_server_log, summarize


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def views(pattern, requests=24, anchor_stop=15000):
    # Deterministic stratified coordinates, not chosen from observed timings.
    rng = np.random.default_rng(20260930)
    bases = rng.choice(np.arange(1024, anchor_stop), requests*2, replace=False).reshape(-1, 2)
    specs = [(64,24,list(range(64))), (96,24,list(range(32,128))),
             (128,48,list(range(64,192))), (192,48,list(reversed(range(128,192))))]
    result=[]
    for i,(length,horizon,channels) in enumerate(specs):
        offset = i*16000 if pattern == 'disjoint' else i*7
        result.append(dict(experiment_id=f'exp-{i}', length=length, horizon=horizon,
                           channels=channels, anchors=(bases+offset).tolist()))
    return result


def normalized(values, valid, spec, mean, std):
    channels=spec['channels']; length=spec['length']
    values=((values.astype(np.float64)-mean[channels])/std[channels]).astype(np.float32)
    values[~valid]=0
    return WindowArrays(np.ascontiguousarray(values[:,:length]), np.ascontiguousarray(valid[:,:length]),
                        np.ascontiguousarray(values[:,length:]), np.ascontiguousarray(valid[:,length:]))


def verify(actual, expected):
    for field in ('input_values','input_valid','target_values','target_valid'):
        a,b=getattr(actual,field),getattr(expected,field)
        if not a.flags.c_contiguous: raise ValueError('noncontiguous output')
        if a.dtype==np.float32: a,b=a.view(np.uint32),b.view(np.uint32)
        if not np.array_equal(a,b): raise ValueError(f'output mismatch: {field}')


def numeric_delta(after, before):
    return {k:v-before[k] for k,v in after.items() if isinstance(v,(int,float))}


def demand_units(item, specs):
    """Independent interval oracle for physical batch and training fragment reuse."""
    batches=[];fragments=[];logical_bytes=0
    for spec in specs:
        batch_keys=set();fragment_keys=set()
        for group in spec['anchors']:
            for anchor in group:
                begin,end=anchor-spec['length'],anchor+spec['horizon']
                logical_bytes+=(end-begin)*len(spec['channels'])*4
                for batch in item['batches']:
                    if batch['start_step']>=end or batch['end_step']<=begin:continue
                    batch_keys.add(batch['batch'])
                    if item['layout']=='step_major':fragment_keys.add((batch['batch'],))
                    else:
                        for row in batch.get('rows',[batch]):
                            lo=max(begin,row['start_step']);hi=min(end,row['end_step'])
                            if lo>=hi:continue
                            for channel in spec['channels']:
                                for tile in range((lo-row['start_step'])//4096,(hi-1-row['start_step'])//4096+1):
                                    fragment_keys.add((batch['batch'],row['row_in_batch'],channel,tile))
        batches.append(batch_keys);fragments.append(fragment_keys)
    return dict(logical_value_bytes=logical_bytes,
        fs_private_batch_units=sum(map(len,batches)),fs_shared_batch_units=len(set.union(*batches)),
        typed_private_fragment_units=sum(map(len,fragments)),typed_shared_fragment_units=len(set.union(*fragments)))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir','mount','socket','binary','artifacts','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--daemon-pid',type=int,required=True)
    p.add_argument('--repetitions',type=int,default=3)
    p.add_argument('--requests',type=int,default=24)
    p.add_argument('--cache-mib',type=int,default=1024)
    p.add_argument('--cases',choices=('all','sm128','sm128-overlap'),default='all')
    p.add_argument('--anchor-stop',type=int,default=15000)
    args=p.parse_args(); args.output.mkdir(parents=True,exist_ok=False)
    if args.cache_mib<4 or args.cache_mib%4:raise ValueError('cache MiB must be a positive multiple of four')
    save(args.output/'invocation.json',dict(argv=sys.argv))
    (args.output/'runner.py').write_bytes(Path(__file__).read_bytes())
    selected_cases=([['original','overlap'],['sm128','overlap'],['sm128','disjoint']] if args.cases=='all'
        else [['sm128','overlap'],['sm128','disjoint']] if args.cases=='sm128' else [['sm128','overlap']])
    def workload(pattern):return views(pattern,args.requests,args.anchor_stop)
    mount=Mount(args)
    artifacts=json.loads(args.artifacts.read_text())
    variants={k:artifacts[k] for k in ('original','sm128')}
    base=args.work_dir/'manifests'
    original=json.loads((base/'UCI_Electricity_370-pb00.json').read_text())
    norm=json.loads((base/'UCI_Electricity_370-pb01-normalization.json').read_text())
    base_windows=json.loads((base/'UCI_Electricity_370-pb01-windows-L96-H24-S1.json').read_text())
    for item in variants.values():
        if hashlib.sha256(Path(item['local']).read_bytes()).hexdigest()!=item['sha256']:
            raise ValueError('artifact changed')
    oracle=load_source(Path(variants['original']['local']))
    mean=np.array([s['mean'] for s in norm['statistics']],dtype=np.float64)
    std=np.array([s['stddev'] for s in norm['statistics']],dtype=np.float64)
    expected={}
    for pattern in {p for _,p in selected_cases}:
        for i,spec in enumerate(workload(pattern)):
            for j,anchors in enumerate(spec['anchors']):
                slices=[slice(a-spec['length'],a+spec['horizon']) for a in anchors]
                values=np.stack([oracle['values'][s][:,spec['channels']] for s in slices])
                valid=np.stack([oracle['valid'][s][:,spec['channels']] for s in slices])
                expected[pattern,i,j]=normalized(values,valid,spec,mean,std)
    settings={'query.cache.max_entries':16384,'query.cache.max_inflight':256,
              'query.cache.worker_threads':4,'query.cache.memory_budget_bytes':1073741824,
              'query.training.read_parallelism':4,'query.training.reuse_prepared_validation':'true',
              'query.training.compiled_transform':'true'}
    configs={}
    for sharing in ('shared','isolated'):
        values=dict(settings,**{'query.cache.memory_budget_bytes':args.cache_mib*1024**2//(1 if sharing=='shared' else 4)})
        config=args.output/(sharing+'.private.properties')
        config.write_text('\n'.join(l for l in Path(mount.original_config).read_text().splitlines()
            if l.split('=',1)[0].strip() not in settings)+'\n'+
            '\n'.join(f'{k}={v}' for k,v in values.items())+'\n')
        config.chmod(0o600);configs[sharing]=config
    method=dict(protocol='iotdbfs-shared-views-experiment/1',repetitions=args.repetitions,
        requests_per_experiment=args.requests,experiments=4,passes=2,
        patterns={p:workload(p) for _,p in selected_cases},
        aggregate_cache_budget=args.cache_mib*1024**2,private_cache_budget=args.cache_mib*1024**2//4,
        cases=selected_cases,
        warm_zero_reads_expected=args.cache_mib>=1024,
        binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        schedule='serial round-robin; typed isolated summed fresh-daemon per-experiment references',
        cold='decoded application cache only; OS and IoTDB caches remain warm',
        output='normalized Float32 bit patterns, bool masks, contiguous private tensors',
        limits='FS whole IPC batch decoder vs typed projection tiles; no optimal FS projection claim; no concurrent throughput or RSS claim',
        preparation='all views prepared before execution; reported separately; cold end-to-end adds prepare and client construction',
        warm='second pass reuses same prepared IDs and decoded cache; result routes released per request')
    method['demand_units']={f'{layout}-{pattern}':demand_units(variants[layout],workload(pattern))
        for layout,pattern in selected_cases}
    save(args.output/'method.json',method);save(args.output/'artifacts.json',variants)
    observations=[]
    def record(row):
        observations.append(row);save(args.output/'results.json',observations)
        print('PASS',row['tag'],row['pass_index'],round(row['execution_seconds'],4),
              'read',row['runtime_delta'].get('read_bytes'),flush=True)

    def typed_reader(item,spec):
        pb=copy.deepcopy(original)
        if item['layout']=='step_major':
            pb['arrow'].update(path=item['logical'].lstrip('/'),sha256=item['sha256'],bytes=item['bytes'],
                rows=item['rows'],batch_row_counts=[1]*len(item['rows']),compression='none',value_layout=item['layout'])
        w=copy.deepcopy(base_windows)
        w.update(input_length=spec['length'],prediction_length=spec['horizon'],arrow_sha256=item['sha256'])
        split=w['splits']['train']; split.update(first_anchor=spec['length'],
            stop_anchor_exclusive=split['target_interval'][1]-spec['horizon']+1)
        split['count']=split['stop_anchor_exclusive']-split['first_anchor']
        reader=IoTDBFSWindowReader(socket_path=args.socket,mount=args.mount,pb00=pb,
            normalization=norm,windows=seal(w),split='train',channels=spec['channels'],
            source_version_exact=item['version'])
        reader.view['experiment_id']=spec['experiment_id']
        reader.view['view_id']='sharing-'+spec['experiment_id']
        return reader

    try:
        for rep in range(args.repetitions):
            cases=list(method['cases'])
            if rep%2:cases.reverse()
            for layout,pattern in cases:
                item=variants[layout];specs=workload(pattern)
                arms=[('fs','isolated'),('fs','shared'),('typed','isolated'),('typed','shared')]
                if rep%2:arms.reverse()
                for backend,sharing in arms:
                    tag=f'r{rep}-{layout}-{pattern}-{backend}-{sharing}'
                    # Isolated typed runs each view in its own daemon lifetime;
                    # aggregate the reference, never label this a concurrent wall time.
                    cohorts=[[i] for i in range(4)] if backend=='typed' and sharing=='isolated' else [list(range(4))]
                    for cohort in cohorts:
                        suffix=f'-e{cohort[0]}' if len(cohort)==1 else ''
                        profile=args.output/(tag+suffix+'-profile.jsonl')
                        if backend=='typed':mount.start(configs[sharing],profile,tag+suffix)
                        readers={};setup=prepare=0.
                        try:
                            for i in cohort:
                                start=time.perf_counter()
                                if backend=='fs':
                                    readers[i]=(readers[cohort[0]] if sharing=='shared' and readers else
                                        DerivedWindowReader(item,mode='local',max_cache_bytes=
                                            args.cache_mib*1024**2//(1 if sharing=='shared' else 4)))
                                else:readers[i]=typed_reader(item,specs[i])
                                setup+=time.perf_counter()-start
                                if backend=='typed':
                                    start=time.perf_counter();readers[i]._prepare();prepare+=time.perf_counter()-start
                            unique=list({id(r):r for r in readers.values()}.values())
                            for repeat in range(2):
                                before={id(r):copy.deepcopy(r.metrics) for r in unique}
                                times={i:[] for i in cohort};cpu_start=time.process_time()
                                verify_seconds=0.
                                for j in range(args.requests):
                                    for i in cohort:
                                        spec=specs[i];anchors=spec['anchors'][j];r=readers[i]
                                        start=time.perf_counter()
                                        if backend=='typed':actual=r.read_windows(anchors)
                                        else:
                                            v,m=r.read_windows([a-spec['length'] for a in anchors],
                                                spec['length']+spec['horizon'],spec['channels'])
                                            actual=normalized(v,m,spec,mean,std)
                                        times[i].append(time.perf_counter()-start)
                                        start=time.perf_counter();verify(actual,expected[pattern,i,j])
                                        verify_seconds+=time.perf_counter()-start
                                cpu=time.process_time()-cpu_start
                                if backend=='typed':
                                    requests=[x for r in unique for x in r.metrics['request_timings'][len(before[id(r)]['request_timings']):]]
                                    summary=summarize(read_server_log(profile),requests)
                                    if summary['excluded_count'] or summary['request_count']!=len(cohort)*args.requests:
                                        raise ValueError('incomplete or overlapping profile')
                                    budget=args.cache_mib*1024**2//(1 if sharing=='shared' else 4)
                                    for request in summary['requests']:
                                        config=request['server']['runtime_config']
                                        if config['memory_budget_bytes']!=budget or config['max_entries']!=16384:
                                            raise ValueError('effective runtime budget differs from experiment')
                                    runtime={k:v['sum'] for k,v in summary['runtime_delta'].items()}
                                    stages={k:v['sum'] for k,v in summary['server_seconds'].items()}
                                    resident=max(x['server']['runtime_after']['cache_owned_bytes'] for x in summary['requests'])
                                else:
                                    deltas=[numeric_delta(r.metrics,before[id(r)]) for r in unique]
                                    runtime={k:sum(d[k] for d in deltas) for k in deltas[0]}
                                    stages={};resident=sum(r.cache_bytes for r in unique)
                                record(dict(tag=tag,cohort=cohort,repetition=rep,layout=layout,pattern=pattern,
                                    backend=backend,sharing=sharing,pass_index=repeat,
                                    execution_seconds=sum(sum(t) for t in times.values()),
                                    request_seconds={str(i):v for i,v in times.items()},
                                    setup_seconds=setup if repeat==0 else 0.,prepare_seconds=prepare if repeat==0 else 0.,
                                    client_cpu_including_verification=cpu,verification_seconds=verify_seconds,
                                    runtime_delta=runtime,server_seconds=stages,cache_owned_bytes=resident,
                                    client_metrics={str(i):numeric_delta(readers[i].metrics,before[id(readers[i])]) for i in cohort},
                                    requests=len(cohort)*args.requests,samples=len(cohort)*args.requests*2,passed=True))
                        finally:
                            for r in {id(r):r for r in readers.values()}.values():r.close()
    finally:
        mount.start(mount.original_config,None,'restored',args.binary)
        save(args.output/'restored.json',dict(pid=mount.pid,profile=False))
    for item in variants.values():
        if hashlib.sha256(Path(item['local']).read_bytes()).hexdigest()!=item['sha256']:
            raise ValueError('artifact changed during experiment')
    save(args.output/'complete.json',dict(passed=True,observations=len(observations)))


if __name__=='__main__': main()
