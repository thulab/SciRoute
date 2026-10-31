#!/usr/bin/env python3
"""Verify H06 reuse, batching and bounded concurrent calls with synthetic latency.

The memory backend deliberately sleeps per transport call. Wall times diagnose
wiring and are not an estimate of live IoTDB/FUSE throughput or speedup.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import platform
import subprocess
import h5py
import numpy as np
import run_h04 as h04


def fixtures(root):
    cases = []
    for name, count, chunk, latency in [('fragmented',4096,64,5000), ('tiny',32768,16,0)]:
        path = root / (name+'.h5')
        data = np.arange(count*2,dtype='<f4').reshape(count,2)
        with h5py.File(path,'w') as file:
            dataset=file.create_dataset('data',data=data,chunks=(chunk,1))
            file['alias']=dataset
        cases.append(dict(name=name,path=path,origin=[0,0],shape=[count,1],latency=latency))
    # One 28 MiB native chunk exercises four <=8 MiB transport calls without
    # inflating the returned rectangle. The unqueried bytes are native-granule
    # read amplification, not transport overread.
    path=root/'large_chunk.h5'
    count=7*1024*1024
    with h5py.File(path,'w') as file:
        dataset=file.create_dataset('data',data=np.arange(count,dtype='<f4'),chunks=(count,))
        file['alias']=dataset
    cases.append(dict(name='large_chunk',path=path,origin=[3],shape=[32],latency=10000))
    return cases


def check(case, report, output):
    h04.require(report['schema_version']=='iotdbfs-hdf5-h06-probe/1','wrong schema')
    by_name={run['label']:run for run in report['runs']}
    h04.require(set(by_name)=={'cold','warm','alias','overlap','concurrent'},'missing or duplicate scenario')
    h04.require(len(report['runs'])==5,'duplicate scenario')
    with h5py.File(case['path'],'r') as file:
        dataset=file['data']
        permitted=h04.allowed_ranges(dataset,case['origin'],case['shape'])
        expected_bytes=sum(end-start for start,end in permitted)
        expected_chunks=len(permitted)
        for run in report['runs']:
            selection=tuple(slice(o,o+s) for o,s in zip(run['origin'],run['shape']))
            h04.require((output/(run['label']+'.bin')).read_bytes()==h04.canonical(dataset[selection]),'h5py value mismatch')
            h04.require(run['active_calls_after']==0,'leaked active transport call')
            h04.require(run['calls']==run['payload_calls'],'runtime/backend call counts differ')
            h04.require(run['decode_cpu_samples'] <= run['decode_count'], 'codec CPU sample count exceeds decodes')
            reads=[r for r in report['reads'] if r['kind']=='payload' and r['phase']==run['label']]
            h04.require(sum(r['length'] for r in reads)==run['read_bytes'],'byte count mismatch')
            h04.require(len(reads)==run['read_ranges'],'range count mismatch')
            for read in reads:
                h04.require(0<read['length']<=8*1024*1024,'transport bound violated')
                h04.require(h04.covered(read['offset'],read['length'],permitted),'read outside allocated selected chunks')
            if run['label'] in ('warm','alias','overlap'):
                h04.require(run['calls']==run['decode_count']==run['read_bytes']==0,'resident data was read or decoded again')
                h04.require(run['payload_read_nanoseconds']==run['decode_nanoseconds']==
                            run['decode_cpu_nanoseconds']==run['decode_cpu_samples']==0,
                            'resident hit reports nonexistent read/decode work')
            else:
                h04.require(run['decode_count']==expected_chunks,'single-flight decode count mismatch')
                h04.require(run['payload_read_nanoseconds'] > 0 and run['decode_nanoseconds'] > 0,
                            'cold producer time instrumentation was not observed')
                h04.require(run['read_bytes']==expected_bytes,'duplicate reads or missing chunk bytes')
                h04.require(h04.range_union([(r['offset'],r['offset']+r['length']) for r in reads])==h04.range_union(permitted),'physical coverage mismatch')
            h04.require(run['evictions']==0,'working set churned despite fitting byte/entry budgets')
            h04.require(run['memory_peak_bytes']<=256*1024*1024,'runtime exceeded configured memory budget')
        cold=by_name['cold']
        expected_peak=min(report['parallelism'],cold['calls'])
        if case['latency']:
            h04.require(cold['active_calls_peak']==expected_peak,'requested transport parallelism not observed')
        if case['name']=='large_chunk':
            h04.require(cold['calls']==4 and cold['decode_count']==1,'large native chunk was not split into four calls')
        if case['name']=='fragmented' and report['multi_range']:
            h04.require(cold['batch_calls']>0 and cold['calls']<expected_chunks,'true multirange batching not observed')
    return dict(fixture=case['name'],parallelism=report['parallelism'],multi_range=report['multi_range'],
                source_sha256=h04.sha(case['path']),runs=report['runs'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    root=args.output/'fixtures';root.mkdir(exist_ok=True)
    report=dict(schema_version='iotdbfs-hdf5-h06-validation/1',platform=platform.platform(),
                probe_sha256=h04.sha(args.probe),oracle_h5py=h5py.__version__,oracle_hdf5=h5py.version.hdf5_version,
                latency_model='synthetic sleep per memory transport call; no live IoTDB performance claim',cases=[],failures=[])
    for case in fixtures(root):
        modes=[(p,m) for p in (1,2,4) for m in (False,True)] if case['name']!='tiny' else [(1,True)]
        for parallel,multi in modes:
            label=f'{case["name"]}-p{parallel}-multi{int(multi)}';output=args.output/'artifacts'/label
            output.mkdir(parents=True,exist_ok=True)
            try:
                command=[str(args.probe.resolve()),str(case['path'].resolve()),str(output.resolve()),
                         ','.join(map(str,case['origin'])),','.join(map(str,case['shape'])),str(parallel),str(int(multi)),str(case['latency'])]
                run=subprocess.run(command,text=True,capture_output=True,timeout=120)
                h04.require(run.returncode==0,run.stderr[:3000])
                raw=json.loads((output/'query.json').read_text());report['cases'].append(check(case,raw,output))
            except Exception as error:
                report['failures'].append(dict(id=label,error=str(error)))
    report['expected_cases']=13;report['passed']=len(report['cases'])==13 and not report['failures']
    (args.output/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ('passed','expected_cases','failures')},indent=2))
    return 0 if report['passed'] else 1

if __name__=='__main__':
    raise SystemExit(main())
