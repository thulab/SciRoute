#!/usr/bin/env python3
"""Separate layout benefit from storage/service cost on a frozen training trace.

Primary factorial: exact original versus SM128/none, local mmap versus IoTDB
ranges, sharing decoder/cache/assembler/normalization/output contract. CM8192
and CM128 plus local read are controls. Production typed routes are a separate
end-to-end arm, not silently equated with the direct range helper.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import time
import numpy as np
from derived_window_reader import DerivedWindowReader
from index_original_training_arrow import index_original
from prepare_electricity_derived import load_source
from prepare_public_windows import seal
from public_iotdbfs import IoTDBFSWindowReader, WindowArrays
from run_cache_parallel_ablation import Mount
from summarize_query_profile import read_server_log, summarize


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def delta(after, before):
    return {k:(v[len(before[k]):] if isinstance(v,list) else
        v-before[k] if isinstance(v,(int,float)) else v) for k,v in after.items()}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir','manifest','trace','mount','socket','binary','helper','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--daemon-pid',type=int,required=True)
    p.add_argument('--source-version',type=int,default=1790670177863)
    p.add_argument('--repetitions',type=int,default=5)
    p.add_argument('--warm-passes',type=int,default=3)
    args=p.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    mount=Mount(args)
    base=args.work_dir/'manifests'
    original=json.loads((base/'UCI_Electricity_370-pb00.json').read_text())
    norm=json.loads((base/'UCI_Electricity_370-pb01-normalization.json').read_text())
    windows=json.loads((base/'UCI_Electricity_370-pb01-windows-L96-H24-S1.json').read_text())
    trace=json.loads(args.trace.read_text())['backend_metrics']['train']['request_timings']
    anchors=[r['anchors'] for r in trace]
    if len(anchors)!=51 or sum(map(len,anchors))!=102:
        raise ValueError('requires frozen 51-request/102-window trace')
    source=args.work_dir/'arrow'/original['arrow']['path']
    started=time.perf_counter()
    original_item=index_original(source,source_version=args.source_version,
        source_logical_path='/'+original['arrow']['path'],mounted=str(args.mount/original['arrow']['path']))
    index_seconds=time.perf_counter()-started
    if original_item['sha256']!=original['arrow']['sha256']:
        raise ValueError('original source differs from frozen PB00')
    variants={'original':original_item}
    published=json.loads(args.manifest.read_text())
    for tag,layout,block in [('cm8192','channel_major',8192),('cm128','channel_major',128),('sm128','step_major',128)]:
        found=[v for v in published if (v['layout'],v['block_steps'],v['codec'])==(layout,block,'none')]
        if len(found)!=1:raise ValueError(f'cannot uniquely resolve {tag}')
        variants[tag]=found[0]
    for tag,item in variants.items():
        if hashlib.sha256(Path(item['local']).read_bytes()).hexdigest()!=item['sha256']:
            raise ValueError(f'{tag} artifact hash mismatch')
    save(args.output/'original-index.json',original_item)
    save(args.output/'artifacts.json',variants)
    oracle=load_source(source)
    mean=np.asarray([s['mean'] for s in norm['statistics']],dtype=np.float64)
    std=np.asarray([s['stddev'] for s in norm['statistics']],dtype=np.float64)
    def normalize(values,valid):
        values=((values.astype(np.float64)-mean)/std).astype(np.float32)
        values[~valid]=0
        return WindowArrays(np.ascontiguousarray(values[:,:96]),np.ascontiguousarray(valid[:,:96]),
            np.ascontiguousarray(values[:,96:]),np.ascontiguousarray(valid[:,96:]))
    expected=[]
    for group in anchors:
        starts=[a-96 for a in group]
        expected.append(normalize(np.stack([oracle['values'][s:s+120] for s in starts]),
            np.stack([oracle['valid'][s:s+120] for s in starts])))
    def verify(actual, reference):
        for field in ('input_values','input_valid','target_values','target_valid'):
            a,b=getattr(actual,field),getattr(reference,field)
            if not a.flags.c_contiguous:raise ValueError('noncontiguous returned tensor')
            if a.dtype==np.float32:a,b=a.view(np.uint32),b.view(np.uint32)
            if not np.array_equal(a,b):raise ValueError(f'output mismatch: {field}')
    settings={'query.cache.max_entries':16384,'query.cache.max_inflight':256,
        'query.cache.worker_threads':4,'query.cache.memory_budget_bytes':1073741824,
        'query.training.read_parallelism':4,'query.training.reuse_prepared_validation':'true',
        'query.training.compiled_transform':'true'}
    config=args.output/'private.properties'
    config.write_text('\n'.join(l for l in Path(mount.original_config).read_text().splitlines()
        if l.split('=',1)[0].strip() not in settings)+'\n'+
        '\n'.join(f'{k}={v}' for k,v in settings.items())+'\n')
    config.chmod(0o600)
    observations=[]
    def record(row):
        observations.append(row)
        with (args.output/'observations.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
        save(args.output/'results.json',observations)
        print('PASS',row['tag'],row['temperature'],round(row['wall_seconds'],4),flush=True)
    arms=[(tag,mode) for tag in variants for mode in ('local','read','iotdb')]
    method=dict(repetitions=args.repetitions,warm_passes=args.warm_passes,requests=51,samples=102,
        width=370,input_length=96,horizon=24,codec='none',cache_bytes=1024**3,
        original_index_seconds=index_seconds,trace_sha256=hashlib.sha256(args.trace.read_bytes()).hexdigest(),
        binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        cold='new decoded cache; OS and IoTDB caches retained; not storage cold',
        primary='identical Python decoder/cache/assembler/normalization; mmap versus raw IoTDB range helper',
        production='server shared decoder cache, authorization/scheduler/IPC/FUSE; reported separately',
        output='owned contiguous Float32 input/target and bool masks; exact normalized bits verified',
        setup='index/artifact construction and reader setup separate; typed first prepare inside first request',
        order='cyclic rotation and reversal by independent repetition; no concurrent timed arms')
    save(args.output/'method.json',method)
    try:
        for rep in range(args.repetitions):
            ordered=arms[rep%len(arms):]+arms[:rep%len(arms)]
            if rep%2:ordered=list(reversed(ordered))
            for tag,mode in ordered:
                item=variants[tag];run=f'r{rep}-{tag}-{mode}'
                reader=DerivedWindowReader(item,mode=mode,helper=args.helper,config=mount.original_config,
                    env=mount.env,log=args.output/(run+'.helper.log'),max_cache_bytes=1024**3)
                try:
                    for repeat in range(args.warm_passes+1):
                        before=copy.deepcopy(reader.metrics);times=[];read_times=[];normalize_times=[]
                        for group,ref in zip(anchors,expected):
                            begin=time.perf_counter();v,m=reader.read_windows([a-96 for a in group],120,range(370))
                            fetched=time.perf_counter();actual=normalize(v,m);end=time.perf_counter()
                            times.append(end-begin);read_times.append(fetched-begin);normalize_times.append(end-fetched)
                            verify(actual,ref)
                        record(dict(tag=run,repetition=rep,layout=tag,mode=mode,pass_index=repeat,
                            temperature='cold' if repeat==0 else 'warm',wall_seconds=sum(times),
                            raw_window_seconds=sum(read_times),normalization_seconds=sum(normalize_times),
                            setup_seconds=reader.setup_seconds,request_seconds=times,
                            metrics=delta(reader.metrics,before),passed=True))
                finally:reader.close()
            for tag in (('original','sm128') if rep%2==0 else ('sm128','original')):
                item=variants[tag];run=f'r{rep}-{tag}-typed';profile=args.output/(run+'-profile.jsonl')
                mount.start(config,profile,run)
                pb=copy.deepcopy(original);view=windows
                if tag!='original':
                    pb['arrow'].update(path=item['logical'].lstrip('/'),sha256=item['sha256'],bytes=item['bytes'],
                        rows=item['rows'],batch_row_counts=[1]*len(item['rows']),compression='none',value_layout=item['layout'])
                    view=seal(dict(windows,arrow_sha256=item['sha256']))
                begin=time.perf_counter()
                reader=IoTDBFSWindowReader(socket_path=args.socket,mount=args.mount,pb00=pb,normalization=norm,
                    windows=view,split='train',channels=range(370),source_version_exact=item['version'],
                    registration_mode='prepared_compact',conversion_mode='buffers')
                setup=time.perf_counter()-begin
                try:
                    for repeat in range(args.warm_passes+1):
                        before=copy.deepcopy(reader.metrics);times=[]
                        for group,ref in zip(anchors,expected):
                            begin=time.perf_counter();actual=reader.read_windows(group);times.append(time.perf_counter()-begin)
                            verify(actual,ref)
                        metrics=delta(reader.metrics,before)
                        summary=summarize(read_server_log(profile),metrics['request_timings'])
                        if summary['excluded_count'] or summary['request_count']!=51:
                            raise ValueError('invalid server profile attribution')
                        record(dict(tag=run,repetition=rep,layout=tag,mode='typed',pass_index=repeat,
                            temperature='cold' if repeat==0 else 'warm',wall_seconds=sum(times),setup_seconds=setup,
                            preparation_seconds=metrics['preparation_seconds'],request_seconds=times,metrics=metrics,
                            server_seconds={k:v['sum'] for k,v in summary['server_seconds'].items()},
                            runtime_delta={k:v['sum'] for k,v in summary['runtime_delta'].items()},passed=True))
                finally:reader.close()
    finally:
        mount.start(mount.original_config,None,'restored',args.binary)
        save(args.output/'restored.json',dict(pid=mount.pid,profile=False))
    for item in variants.values():
        if hashlib.sha256(Path(item['local']).read_bytes()).hexdigest()!=item['sha256']:
            raise ValueError('artifact changed during experiment')
    save(args.output/'complete.json',dict(passed=True,observations=len(observations)))

if __name__=='__main__':main()
