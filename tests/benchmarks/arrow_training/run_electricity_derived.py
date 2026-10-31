#!/usr/bin/env python3
"""Prepare/publish and replay Electricity training traces on derived Arrow blocks.

No production mount restart for replay. Original bytes and source versions remain
unchanged. The direct range adapter is explicitly separate from typed IPC/FUSE.
"""
from __future__ import annotations
import argparse
import copy
import json
import os
from pathlib import Path
import time
import numpy as np
from derived_window_reader import DerivedWindowReader
from prepare_arrow_query_variants import ingest_exact
from prepare_electricity_derived import load_source
from public_backends import LocalArrowWindowReader
from public_iotdbfs import source_version


def daemon_environment(pid, mount):
    process=Path(f'/proc/{pid}')
    if str(mount).encode() not in process.joinpath('cmdline').read_bytes().split(b'\0'):
        raise ValueError('specified PID does not own experiment mount')
    env=os.environ.copy()
    for item in process.joinpath('environ').read_bytes().split(b'\0'):
        if b'=' in item:
            k,v=item.split(b'=',1);env[k.decode()]=v.decode()
    return env


def save(path, value):
    path.write_text(json.dumps(value,indent=2)+'\n')


def publish(args):
    parent=args.mount/'electricity-derived-v1'
    parent.mkdir(exist_ok=True)
    published=[]
    for sidecar in sorted(args.variants.glob('*.arrows.json')):
        item=json.loads(sidecar.read_text())
        logical=f'/electricity-derived-v1/{Path(item["local"]).name}'
        dest=args.mount/logical.lstrip('/')
        ingestion,verify=ingest_exact(Path(item['local']),dest,item['bytes'],item['sha256'])
        item.update(logical=logical,mounted=str(dest),version=source_version(args.socket,logical),
                    ingestion_seconds=ingestion,readback_seconds=verify)
        published.append(item)
        print(json.dumps({k:item[k] for k in ('layout','block_steps','codec','bytes','version','ingestion_seconds')}),flush=True)
    if not published:raise ValueError('no derived sidecars')
    save(args.variants/'published.json',published)


def replay(args):
    args.output.mkdir(parents=True,exist_ok=False)
    env=daemon_environment(args.daemon_pid,args.mount)
    published=json.loads((args.variants/'published.json').read_text())
    report=json.loads(args.trace.read_text())
    anchors=[row['anchors'] for row in report['backend_metrics']['train']['request_timings']]
    if len(anchors)!=51 or sum(map(len,anchors))!=102:raise ValueError('expected standard 50-step trace')
    pb00=json.loads((args.work_dir/'manifests/UCI_Electricity_370-pb00.json').read_text())
    source=args.work_dir/'arrow'/pb00['arrow']['path']
    oracle=load_source(source)
    rows=[]
    cache_bytes=args.cache_mib*1024**2
    for repetition in range(args.repetitions):
        variants=published if repetition%2==0 else list(reversed(published))
        for width in args.widths:
            channels=tuple(range(width))
            for item in [None]+variants:
                modes=['original-local'] if item is None else args.modes
                for mode in modes:
                    tag=f'r{repetition}-{width}-'+('original' if item is None else f'{item["layout"]}-{item["block_steps"]}-{item["codec"]}')+'-'+mode
                    if item is None:
                        setup=time.perf_counter()
                        reader=LocalArrowWindowReader(source,row_index=pb00['arrow']['rows'],max_cache_bytes=cache_bytes)
                        setup=time.perf_counter()-setup
                    else:
                        reader=DerivedWindowReader(item,mode=mode,helper=args.helper,config=env['IOTDB_CONFIG_FILE'],env=env,
                            log=args.output/f'{tag}.log',max_cache_bytes=cache_bytes)
                        setup=reader.setup_seconds
                    try:
                        for repeat in range(args.repeats+1):
                            before=copy.deepcopy(reader.metrics)
                            elapsed=[]
                            for group in anchors:
                                starts=[a-96 for a in group]
                                begin=time.perf_counter()
                                if item is None:
                                    pairs=[reader.read_window(s,120,channels) for s in starts]
                                    values=np.stack([v for v,m in pairs]);valid=np.stack([m for v,m in pairs])
                                else:values,valid=reader.read_windows(starts,120,channels)
                                elapsed.append(time.perf_counter()-begin)
                                # Exact Float32 bits and masks, outside measured read time.
                                for i,s in enumerate(starts):
                                    ev=oracle['values'][s:s+120,:width];em=oracle['valid'][s:s+120,:width]
                                    if not np.array_equal(valid[i],em) or not np.array_equal(values[i].view(np.uint32)[em],ev.view(np.uint32)[em]):
                                        raise ValueError(f'window differs: {tag}/{s}')
                            after=reader.metrics
                            row=dict(tag=tag,repetition=repetition,repeat=repeat,temperature='cold' if repeat==0 else 'warm',
                                mode=mode,layout='original' if item is None else item['layout'],width=width,
                                block_steps=8192 if item is None else item['block_steps'],codec='none' if item is None else item['codec'],
                                wall_seconds=sum(elapsed),request_seconds=elapsed,setup_seconds=setup,
                                cache_bytes=cache_bytes,passed=True,metrics={k:v-before.get(k,0) for k,v in after.items() if isinstance(v,(int,float))},
                                file_bytes=source.stat().st_size if item is None else item['bytes'])
                            rows.append(row)
                            with (args.output/'observations.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
                        print('PASS',tag,round(rows[-args.repeats-1]['wall_seconds'],4),round(rows[-1]['wall_seconds'],4),flush=True)
                    finally:reader.close()
    save(args.output/'results.json',rows)
    save(args.output/'method.json',dict(trace=str(args.trace),requests=51,samples=102,input_length=96,horizon=24,
        modes=args.modes,repetitions=args.repetitions,warm_repeats=args.repeats,cache_bytes=cache_bytes,
        cold='empty application cache, OS/IoTDB caches retained',
        timer='read/decode/cache/owned assembly; verification excluded; setup separate',
        remote='one-time exact manifest validation; direct backend range helper, NOT production typed IPC/FUSE'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=['publish','replay'])
    for name in ('work-dir','variants','mount','socket','output','trace','helper'):
        p.add_argument('--'+name,type=Path)
    p.add_argument('--daemon-pid',type=int)
    p.add_argument('--modes',nargs='+',default=['local','read','iotdb','fuse'])
    p.add_argument('--widths',nargs='+',type=int,default=[370,7])
    p.add_argument('--cache-mib',type=int,default=1024)
    p.add_argument('--repeats',type=int,default=10)
    p.add_argument('--repetitions',type=int,default=2)
    args=p.parse_args()
    (publish if args.phase=='publish' else replay)(args)

if __name__=='__main__':main()
