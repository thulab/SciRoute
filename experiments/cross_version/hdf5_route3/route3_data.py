#!/usr/bin/env python3
"""Fully allocated, independently owned HDF5 histories for route-3 experiments."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import time

PROTOCOL = 'iotdbfs-route3-hdf5-data/1'
PREDICATE = dict(shortName='t', typeOfLevel='isobaricInhPa', level=850,
                 step=0, validityDate=20260101, validityTime=0)
SHAPE = (1, 1, 128, 128)
CHUNKS = (1, 1, 32, 32)
ORIGINS = ((0, 0, 0, 0), (0, 0, 16, 16))
ROI = (1, 1, 64, 64)
SEED = 20261018

def require(value, message):
    if not value:
        raise ValueError(message)

def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

def save(path, data):
    path=Path(path);temporary=path.with_name(path.name+'.pending')
    with temporary.open('w') as out:
        json.dump(data,out,indent=2,sort_keys=True,allow_nan=False);out.write('\n');out.flush();os.fsync(out.fileno())
    os.replace(temporary,path)

def plan(N):
    import numpy as np
    require(N in (8,128,512,1024),'unsupported planned N')
    rng=np.random.default_rng(SEED)
    # The two positives are distributed among the first 128 logical IDs. Their
    # physical names/dataset slots are independently permuted; no query receives
    # their identities. Catalog build enumerates every source and every dataset.
    targets=set(int(x) for x in rng.choice(min(N,128),2,replace=False))
    names=np.random.default_rng(SEED+N).permutation(N)
    result=[]
    for file_id,name in enumerate(names):
        slot=int(np.random.default_rng(SEED+file_id*17).integers(4))
        units=[]
        for dataset_id in range(4):
            attrs=dict(PREDICATE)
            if file_id not in targets or dataset_id!=slot:
                field=list(attrs)[(file_id*4+dataset_id)%6]
                attrs[field]=attrs[field]+'_other' if isinstance(attrs[field],str) else attrs[field]+1
            units.append({'dataset':f'/fields/d{dataset_id:02d}','attributes':attrs})
        result.append({'file_id':file_id,'logical_file':f'b{int(name)%16:02d}/f{int(name):06d}.h5','units':units})
    require(sum(u['attributes']==PREDICATE for f in result for u in f['units'])==2,'K2 plan differs')
    return result

def write_first(path, item):
    import h5py
    import numpy as np
    with h5py.File(path,'x',libver='latest',track_order=False) as file:
        for slot,unit in enumerate(item['units']):
            rng=np.random.default_rng(SEED+item['file_id']*193+slot*13)
            values=rng.uniform(-100,100,size=SHAPE).astype('<f4')
            dataset=file.create_dataset(unit['dataset'],data=values,chunks=CHUNKS,
                compression='gzip',compression_opts=1,shuffle=True,track_times=False)
            for key,value in unit['attributes'].items():
                dataset.attrs[key]=np.bytes_(value.encode('ascii')) if isinstance(value,str) else value

def inspect(path, item):
    import h5py
    import numpy as np
    units=[]
    with h5py.File(path,'r') as file:
        for unit in item['units']:
            dataset=file[unit['dataset']]
            require(tuple(dataset.shape)==SHAPE and tuple(dataset.chunks)==CHUNKS,'shape/chunks differ')
            require(dataset.dtype==np.dtype('<f4') and dataset.compression=='gzip' and dataset.shuffle,'dtype/filter differs')
            actual={}
            for key in PREDICATE:
                value=dataset.attrs[key]
                if isinstance(value,np.generic):value=value.item()
                actual[key]=value.decode('ascii') if isinstance(value,bytes) else value
            require(actual==unit['attributes'],'metadata differs')
            require(dataset.id.get_num_chunks()==16,'source contains unallocated chunks')
            oracles=[]
            for origin in ORIGINS:
                slices=tuple(slice(a,a+b) for a,b in zip(origin,ROI))
                array=np.ascontiguousarray(dataset[slices],dtype='<f4')
                require(array.shape==ROI and np.isfinite(array).all(),'native source ROI differs')
                oracles.append({'origin':list(origin),'shape':list(ROI),'elements':array.size,
                    'value_sha256':hashlib.sha256(array.tobytes()).hexdigest(),
                    'mask_sha256':hashlib.sha256(b'\1'*array.size).hexdigest()})
            units.append({**unit,'oracles':oracles})
    return units

def prepare_round(root,N,ordinal,byte_limit=24*1024**3):
    """Create exactly one complete round; never truncate old files or retry it."""
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    require(1<=ordinal<=32,'ordinal outside planned history')
    manifest_path=root/'manifest.json'
    if manifest_path.exists():
        manifest=json.loads(manifest_path.read_text())
        require(manifest['protocol']==PROTOCOL and manifest['N']==N and manifest['rounds']==ordinal-1,'round is not append-only')
    else:
        require(ordinal==1,'first round must be 1')
        manifest={'protocol':PROTOCOL,'N':N,'D':4,'K':2,'rounds':0,'seed':SEED,'predicate':PREDICATE,
            'shape':SHAPE,'chunks':CHUNKS,'roi':ROI,'origins':ORIGINS,'entries':[],
            'physical_bytes':0,'allocated_bytes':0,'preparation_ns':0,'storage':'fully allocated independent regular files; shared by FS reader arms and immutable ingestion input; no links/reflinks/sparse copies'}
    require(shutil.disk_usage(root).free>8*1024**3,'less than 8GiB available before raw generation')
    directory=root/f'v{ordinal:03d}'
    directory.mkdir(exist_ok=False)
    started=time.perf_counter_ns();seen=set()
    for item in plan(N):
        target=directory/item['logical_file'];target.parent.mkdir(parents=True,exist_ok=True)
        if ordinal==1:write_first(target,item)
        else:
            previous=root/f'v{ordinal-1:03d}'/item['logical_file']
            with previous.open('rb') as source,target.open('xb') as output:
                shutil.copyfileobj(source,output,1024*1024)
            import h5py
            import numpy as np
            with h5py.File(target,'r+') as file:
                dataset=file[item['units'][(ordinal-2)%4]['dataset']]
                value=np.asarray([dataset[0,0,0,0]],dtype='<f4');value.view('<u4')[0]^=np.uint32(ordinal-1)
                dataset[0,0,0,0]=value[0]
        with target.open('rb') as source:os.fsync(source.fileno())
        st=target.stat();require(st.st_nlink==1 and not target.is_symlink(),'non-independent source')
        identity=(st.st_dev,st.st_ino);require(identity not in seen,'duplicate inode');seen.add(identity)
        require(manifest['physical_bytes']+st.st_size<=byte_limit,'raw byte capacity exceeded')
        units=inspect(target,item)
        manifest['entries'].append({'file_id':item['file_id'],'logical_file':item['logical_file'],'ordinal':ordinal,
            'file':target.relative_to(root).as_posix(),'bytes':st.st_size,'allocated_bytes':st.st_blocks*512,
            'inode':st.st_ino,'device':st.st_dev,'nlink':st.st_nlink,'mtime_ns':st.st_mtime_ns,
            'sha256':sha_file(target),'units':units})
        manifest['physical_bytes']+=st.st_size;manifest['allocated_bytes']+=st.st_blocks*512
    manifest['rounds']=ordinal;manifest['preparation_ns']+=time.perf_counter_ns()-started
    save(manifest_path,manifest)
    return manifest

def expected(manifest,ordinals,origin):
    wanted=manifest['predicate'];out={}
    for entry in manifest['entries']:
        if entry['ordinal'] not in ordinals:continue
        for unit in entry['units']:
            if unit['attributes']==wanted:
                oracle=next(o for o in unit['oracles'] if o['origin']==list(origin))
                out[(entry['logical_file'],entry['ordinal'],unit['dataset'])]=oracle
    require(len(out)==2*len(ordinals),'oracle cardinality differs')
    return out
