#!/usr/bin/env python3
"""Scientific discovery -> exact IPC/FUSE verification in an isolated h09 DB.

Uses the explicitly provided shared writer lock. Never creates a database or
modifies existing files; unique test source versions and reports are retained.
"""
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid


def need(condition, message):
    if not condition:
        raise AssertionError(message)


def control(path, request):
    data = json.dumps(request, allow_nan=False).encode()
    def read(sock, n):
        result = b""
        while len(result) < n:
            part = sock.recv(n-len(result))
            need(part, "short control response")
            result += part
        return result
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(300)
        sock.connect(str(path))
        sock.sendall(struct.pack(">I", len(data))+data)
        size, = struct.unpack(">I", read(sock, 4))
        need(0 < size <= 1024*1024, "control frame limit")
        result = json.loads(read(sock, size))
    need(result.get("ok"), str(result))
    return result


def main(args):
    import h5py
    import numpy as np
    import pyarrow as pa
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
    from a12_migration_smoke import connection_environment
    need(args.database.startswith("h09probe"), "requires isolated h09 database")
    root = args.output.resolve(); root.mkdir(parents=True, exist_ok=False)
    env = connection_environment(args.source_pid, args.database)
    env['IOTDBFS_NAMESPACE_LOCK'] = str(args.writer_lock.resolve())
    env['IOTDBFS_QUERY_SOCKET'] = str(root/'query.sock')
    env['IOTDBFS_QUERY_PROFILE_JSONL'] = str(root/'profile.jsonl')
    env['IOTDB_CONFIG_FILE'] = '/nonexistent/sc-isolated.properties'
    binary = args.build.resolve()/'bin/iotdb_fs'
    mount = root/'mount'; mount.mkdir()
    prefix = '/h09-sc-'+uuid.uuid4().hex[:12]
    evidence = {'protocol':'iotdbfs-hdf5-sc-live/1','sources':[], 'checks':{}}
    expected = []
    for raw in args.raw:
        with h5py.File(raw, 'r') as file:
            dataset = file['sst']; values = dataset[0,0,356:364,400:408].astype('float64')
            attrs = dataset.attrs
            valid = (values != float(np.asarray(attrs['_FillValue']).flat[0]))
            valid &= values >= float(np.asarray(attrs['valid_min']).flat[0])
            valid &= values <= float(np.asarray(attrs['valid_max']).flat[0])
            values = values*float(np.asarray(attrs['scale_factor']).flat[0])+float(np.asarray(attrs['add_offset']).flat[0])
            expected.append((values.ravel(),valid.ravel(),file['time'][0],file['lat'][356:364],file['lon'][400:408]))
    process = None
    def start():
        nonlocal process
        with (root/'mount.log').open('ab') as log:
            process = subprocess.Popen([str(binary),'-f','-o','attr_timeout=0,entry_timeout=0',str(mount)],env=env,cwd=root,stdout=log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+60
        while not (os.path.ismount(mount) and (root/'query.sock').exists()):
            need(process.poll() is None,'mount exited; see mount.log')
            need(time.monotonic()<deadline,'mount startup timeout')
            time.sleep(.1)
    def stop():
        nonlocal process
        if os.path.ismount(mount):
            subprocess.run(['fusermount','-u',str(mount)],check=True,timeout=30)
        if process:
            process.wait(timeout=30); process=None
    try:
        start()
        (mount/prefix[1:]).mkdir()
        for i,raw in enumerate(args.raw):
            dest=mount/prefix[1:]/f'day{i}.nc'; shutil.copyfile(raw,dest)
            digest=hashlib.sha256(raw.read_bytes()).hexdigest()
            need(hashlib.sha256(dest.read_bytes()).hexdigest()==digest,'ordinary file bytes changed')
            evidence['sources'].append({'path':prefix+f'/day{i}.nc','sha256':digest})
        def discover(path=prefix):
            return control(root/'query.sock', {'kind':'dataset_catalog','request':{
                'sources':{'paths':[{'kind':'subtree','path':path}]},'dataset_path':'/sst',
                'predicate':{'kind':'compare','field':'hdf5.attr:long_name','operator':'eq','value':{'type':'utf8','value':'Daily sea surface temperature'}}}})
        found=discover(); need(len(found['datasets'])==len(args.raw),'cross-file discovery count')
        evidence['catalog']=found
        def bound(v,kind):return {'kind':kind,'value':{'type':'float64','value':float(v)}}
        for match in found['datasets']:
            i=int(Path(match['path']).stem[3:]); values,valid,t,lat,lon=expected[i]
            request={'kind':'typed','request':{'sources':{'paths':[{'kind':'exact','path':match['path']}],
                'versions':{'kind':'exact','exact':match['version']}},'subset':{
                'projection':['hdf5.science.value:%2Fsst','hdf5.coord.time','hdf5.coord.latitude','hdf5.coord.longitude'],
                'slices':[{'dimension':'hdf5.coord.latitude','lower':bound(-1,'inclusive'),'upper':bound(1,'exclusive')},
                          {'dimension':'hdf5.coord.longitude','lower':bound(100,'inclusive'),'upper':bound(102,'exclusive')}]}}}
            result=control(root/'query.sock',request)
            with pa.memory_map(result['path'],'r') as stream:
                table=pa.ipc.open_stream(stream).read_all()
            need(table.num_rows==64,'ROI shape')
            column=table.column(0)
            np.testing.assert_array_equal(column.is_valid().to_numpy(),valid)
            np.testing.assert_array_equal(column.to_numpy()[valid],values[valid])
            np.testing.assert_array_equal(table.column(1).to_numpy(),np.repeat(t,64))
            np.testing.assert_array_equal(table.column(2).to_numpy(),np.repeat(lat,8))
            np.testing.assert_array_equal(table.column(3).to_numpy(),np.tile(lon,8))
            metadata={k.decode():v.decode() for k,v in table.schema.metadata.items()}
            need(any('scale_factor' in k for k in metadata),'missing scientific transform metadata')
            evidence.setdefault('ipc',[]).append({'path':match['path'],'rows':64,'bytes':Path(result['path']).stat().st_size,'metadata':metadata})
            del table, column, stream
            gc.collect()
        # Rename copies immutable catalog before target publication.
        old=mount/prefix[1:]/'day0.nc';new=mount/prefix[1:]/'renamed.nc';old.rename(new)
        renamed=discover();need(any(x['path'].endswith('/renamed.nc') for x in renamed['datasets']),'renamed catalog absent')
        need(not any(x['path'].endswith('/day0.nc') for x in renamed['datasets']),'deleted source still discoverable')
        evidence['checks']['rename_catalog']=True
        stop();start()
        restarted=discover();need(len(restarted['datasets'])==len(args.raw),'catalog lost on daemon restart')
        evidence['checks']['persistent_catalog_restart']=True
        evidence['checks'].update(cross_file_discovery=True,exact_science_ipc=True,mmap=True,original_bytes=True)
        evidence['ok']=True
    finally:
        try:
            stop()
        finally:
            (root/'report.json').write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps({'ok':True,'report':str(root/'report.json')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--build',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--database',required=True);p.add_argument('--source-pid',type=int,required=True);p.add_argument('--writer-lock',type=Path,required=True)
    p.add_argument('--raw',type=Path,nargs='+',required=True);main(p.parse_args())
