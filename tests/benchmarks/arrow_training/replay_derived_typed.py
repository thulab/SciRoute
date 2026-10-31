#!/usr/bin/env python3
"""Replay the frozen training trace through production typed windows."""
import argparse
import copy
import json
from pathlib import Path
import time
import numpy as np
from public_iotdbfs import IoTDBFSWindowReader
from prepare_public_windows import seal
from prepare_electricity_derived import load_source
from run_cache_parallel_ablation import Mount
from summarize_query_profile import read_server_log


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir','manifest','trace','mount','socket','binary','fallback','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--daemon-pid',type=int,required=True)
    p.add_argument('--blocks',type=int,nargs='+',default=[128,512,2048])
    p.add_argument('--codec',default='lz4')
    p.add_argument('--layouts',nargs='+',default=['channel_major','step_major'])
    p.add_argument('--requests',type=int,default=51)
    p.add_argument('--reuse-context',action='store_true',help='keep the prepared ID for the warm pass')
    args=p.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    base=args.work_dir/'manifests'
    original=json.loads((base/'UCI_Electricity_370-pb00.json').read_text())
    normalization=json.loads((base/'UCI_Electricity_370-pb01-normalization.json').read_text())
    windows=json.loads((base/'UCI_Electricity_370-pb01-windows-L96-H24-S1.json').read_text())
    trace=json.loads(args.trace.read_text())['backend_metrics']['train']['request_timings'][:args.requests]
    oracle=load_source(args.work_dir/'arrow'/original['arrow']['path'])
    mean=np.array([s['mean'] for s in normalization['statistics']],dtype=np.float64)
    std=np.array([s['stddev'] for s in normalization['statistics']],dtype=np.float64)
    variants=[None]+[v for v in json.loads(args.manifest.read_text()) if v['layout'] in args.layouts and v['codec']==args.codec and v['block_steps'] in args.blocks]
    mount=Mount(args)
    settings={'query.cache.max_entries':16384,'query.cache.max_inflight':256,'query.cache.worker_threads':4,
        'query.cache.memory_budget_bytes':1073741824,'query.training.read_parallelism':4,
        'query.training.reuse_prepared_validation':'true','query.training.compiled_transform':'true'}
    config=args.output/'private.properties'
    config.write_text('\n'.join(l for l in Path(mount.original_config).read_text().splitlines() if l.split('=',1)[0].strip() not in settings)+'\n'+'\n'.join(f'{k}={v}' for k,v in settings.items())+'\n')
    config.chmod(0o600)
    results=[]
    reader=None
    try:
        for item in variants:
            tag='original' if item is None else f'{item["layout"]}-{item["block_steps"]}-{item["codec"]}'
            profile=args.output/f'{tag}-profile.jsonl'
            mount.start(config,profile,tag)
            pb00=copy.deepcopy(original);view=windows;version=1790670177863
            if item:
                pb00['arrow'].update(path=item['logical'].lstrip('/'),sha256=item['sha256'],bytes=item['bytes'],rows=item['rows'],batch_row_counts=[1]*len(item['rows']),compression=item['codec'],value_layout=item['layout'])
                view=seal(dict(windows,arrow_sha256=item['sha256']));version=item['version']
            reader=None
            for repeat in range(2):
                begin=time.perf_counter()
                if reader is None:
                    reader=IoTDBFSWindowReader(socket_path=args.socket,mount=args.mount,pb00=pb00,normalization=normalization,
                        windows=view,split='train',channels=range(370),source_version_exact=version,
                        registration_mode='prepared_compact',conversion_mode='buffers')
                    constructor=time.perf_counter()-begin
                else:constructor=0
                before=copy.deepcopy(reader.metrics)
                elapsed=[]
                try:
                    for row in trace:
                        begin=time.perf_counter();arrays=reader.read_windows(row['anchors']);elapsed.append(time.perf_counter()-begin)
                        for i,anchor in enumerate(row['anchors']):
                            s=anchor-96;v=((oracle['values'][s:s+120].astype(np.float64)-mean)/std).astype(np.float32)
                            m=oracle['valid'][s:s+120];v[~m]=0
                            actual=np.concatenate([arrays.input_values[i],arrays.target_values[i]])
                            mask=np.concatenate([arrays.input_valid[i],arrays.target_valid[i]])
                            if not np.array_equal(actual.view(np.uint32),v.view(np.uint32)) or not np.array_equal(mask,m):raise ValueError('typed derived values differ')
                    result=dict(tag=tag,temperature='cold' if repeat==0 else 'warm',wall_seconds=sum(elapsed),constructor_seconds=constructor,
                        metrics={k:(v[len(before[k]):] if isinstance(v,list) else v-before[k] if isinstance(v,(int,float)) else v) for k,v in reader.metrics.items()},
                        reused_context=bool(repeat and args.reuse_context),request_seconds=elapsed,passed=True)
                    results.append(result)
                    (args.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
                    print('PASS',tag,result['temperature'],round(result['wall_seconds'],3),flush=True)
                finally:
                    if not args.reuse_context or repeat==1:
                        reader.close();reader=None
    finally:
        if reader is not None:reader.close()
        mount.start(mount.original_config,None,'restored',args.fallback)
        (args.output/'restored.json').write_text(json.dumps(dict(pid=mount.pid,profile=False))+'\n')

if __name__=='__main__':main()
