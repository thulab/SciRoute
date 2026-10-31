#!/usr/bin/env python3
"""H03 ingestion oracle: original B, complete allocated indices and graph aliases.

The probe uses real IngestionModule/StructuredBlobStorage with a recording
in-memory backend. This does not claim a live IoTDB mount or typed query route.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import time
import h5py
import numpy as np
HERE=Path(__file__).resolve().parent

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def require(v,m):
    if not v: raise AssertionError(m)

def extra_fixtures(root):
    graph=root/'h03_graph.h5'
    with h5py.File(graph,'w',userblock_size=512) as f:
        a=f.create_dataset('a',data=np.arange(315,dtype='<f4').reshape(5,7,9),chunks=(2,3,4))
        f.create_dataset('b',data=np.arange(77,dtype='>f8').reshape(7,11))
        f['a_alias']=a
        g=f.create_group('group')
        g['a_again']=a
        g['loop']=g
        f['group_alias']=g
        f['self']=f['/']
        g['external']=h5py.ExternalLink('/iotdbfs-h03-never-open.h5','/data')
    many=root/'h03_paged_chunks.h5'
    with h5py.File(many,'w') as f:
        f.create_dataset('data',data=np.arange(2048,dtype='<f4'),chunks=(1,))
    return [graph,many]

def resolve(report,path):
    current=report['root']
    edges={(e['parent'],e['name']):e for e in report['links']}
    for component in path.strip('/').split('/'):
        edge=edges[(current,component)]
        require(edge['kind']==0,'resolved a non-hard link')
        current=edge['target']
    return current

def check_report(path,output):
    r=json.loads((output/'inventory.json').read_text())
    require(r['schema_version']=='iotdbfs-hdf5-h03-probe/1','wrong probe schema')
    require(r['format_id']=='hdf5' and r['directory_complete'],'identity/incomplete directory')
    require(r['payload_written']==path.stat().st_size==r['source_bytes'],'B was not copied once')
    require(sha(path)==sha(output/'payload.bin'),'published B differs from original bytes')
    require((output/'layout.bin').read_bytes().startswith(b'IOTDBFS-HDF5-LAYOUT/3\n'),'wrong layout version')
    for read in r['source_reads']:
        require(0<=read['offset']<=r['source_bytes'] and 0<=read['length']<=r['source_bytes']-read['offset'],
                'out-of-source metadata or copy read')
    checked=0
    with h5py.File(path,'r') as f:
        for d in r['datasets']:
            require(resolve(r,d['path'])==d['id'],'canonical path does not resolve to dataset ID')
            # External/VDS/unsupported types are retained as unavailable, never
            # followed or decoded by this oracle. Supported native metadata is
            # independently compared against h5py's separate HDF5 library.
            if not d['supported']: continue
            native=f[d['path']]
            require(list(native.shape)==d['shape'],'dataset extent mismatch')
            require(native.dtype.itemsize==d['element_bytes'],'element width mismatch')
            require(d['index_complete'],'supported baseline lost complete index')
            dcpl=native.id.get_create_plist()
            pipeline=[dcpl.get_filter(i) for i in range(dcpl.get_nfilters())]
            require([x[0] for x in pipeline]==[x['id'] for x in d['filters']],'filter order mismatch')
            require([list(x[2]) for x in pipeline]==[x['parameters'] for x in d['filters']],'filter parameters mismatch')
            if native.chunks:
                require(list(native.chunks)==d['chunk_shape'],'chunk extent mismatch')
                expected=[]
                for i in range(native.id.get_num_chunks()):
                    c=native.id.get_chunk_info(i)
                    expected.append({'origin':list(c.chunk_offset),'offset':c.byte_offset,
                                     'size':c.size,'filter_mask':c.filter_mask})
                require(len(expected)==d['allocated_chunks'],'native allocated count mismatch')
                require(sorted(expected,key=lambda c:c['origin'])==d['chunks'],'chunk record/address/mask mismatch')
            else:
                require(native.id.get_offset()==d['contiguous_offset'],'contiguous absolute offset mismatch')
                require(native.id.get_storage_size()==d['contiguous_size'],'contiguous storage length mismatch')
            checked+=1
    if path.name=='h03_graph.h5':
        aliases=['/a','/a_alias','/group/a_again','/group/loop/a_again',
                 '/group_alias/a_again','/self/a','/self/self/group/loop/a_again']
        require(len({resolve(r,p) for p in aliases})==1,'hard aliases/cycles lost shared identity')
        require(len(r['datasets'])==2,'aliases duplicated datasets')
    return {'id':path.stem,'source_sha256':sha(path),'layout_sha256':sha(output/'layout.bin'),
            'source_bytes':r['source_bytes'],'layout_bytes':r['layout_bytes'],
            'datasets_checked':checked,'total_datasets':len(r['datasets']),
            'source_calls':len(r['source_reads']),
            'additional_index_read_bytes':sum(x['length'] for x in r['source_reads'] if x['after_copy']),
            'layout_read_bytes':r['layout_read_bytes']}

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--probe',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--public-dir',type=Path,required=True);args=ap.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    generator_path=HERE.parents[1]/'fixtures/hdf5/generate_h00.py'
    spec=importlib.util.spec_from_file_location('h03_generator',generator_path)
    generator=importlib.util.module_from_spec(spec);spec.loader.exec_module(generator)
    root=args.output/'fixtures';manifest=generator.generate(root)
    paths=[root/f['path'] for f in manifest['files']]+extra_fixtures(root)
    public=json.loads((HERE/'public-data.lock.json').read_text())
    for f in public['files']:
        path=args.public_dir/f['filename'];require(sha(path)==f['sha256'],'public hash mismatch');paths.append(path)
    report={'schema_version':'iotdbfs-hdf5-h03-validation/1','platform':platform.platform(),
            'probe_sha256':sha(args.probe),'oracle_h5py':h5py.__version__,
            'oracle_hdf5':h5py.version.hdf5_version,'expected_files':len(paths),'cases':[],'failures':[]}
    for path in paths:
        output=args.output/'artifacts'/path.stem;output.mkdir(parents=True,exist_ok=True)
        try:
            # A failed invocation must not inherit a successful old report.
            for name in ['inventory.json','payload.bin','layout.bin']:(output/name).unlink(missing_ok=True)
            begin=time.perf_counter()
            run=subprocess.run([str(args.probe.resolve()),str(path.resolve()),str(output.resolve())],
                               text=True,capture_output=True,timeout=60)
            elapsed=time.perf_counter()-begin
            require(run.returncode==0,run.stderr[:2000])
            case=check_report(path,output);case['subprocess_wall_seconds']=elapsed;report['cases'].append(case)
        except Exception as exc:report['failures'].append({'id':path.stem,'error':str(exc)})
    report['passed']=not report['failures'] and len(report['cases'])==len(paths)
    (args.output/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'cases':len(report['cases']),
                      'expected_files':len(paths),'failures':report['failures']},indent=2))
    return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
