"""Fixed-matrix IPC client; both arms finish with client-owned values/masks.

Profile harvesting and independent verification happen only after t2. No legacy
native internal clock or residual discovery timer enters this protocol.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import socket
import struct
import time
from urllib.parse import quote

PREDICATE = {'shortName':'t','typeOfLevel':'isobaricInhPa','level':500,
             'step':0,'validityDate':20261013,'validityTime':0}


def need(ok, message):
    if not ok: raise RuntimeError(message)


def receive(stream, count):
    parts=[]
    while count:
        data=stream.recv(count);need(data,'truncated control response')
        parts.append(data);count-=len(data)
    return b''.join(parts)


def control(context, request):
    started=time.perf_counter_ns()
    payload=json.dumps(request,separators=(',',':'),allow_nan=False).encode()
    need(len(payload)<=1048576,'control request over limit')
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(300);stream.connect(str(context['socket']))
        stream.sendall(struct.pack('>I',len(payload))+payload)
        length,=struct.unpack('>I',receive(stream,4));need(0<length<=1048576,'invalid response bound')
        response=json.loads(receive(stream,length))
    need(response.get('ok') is True,'query control failed: '+str(response.get('error','unknown')))
    return response, time.perf_counter_ns()-started


def ipc(context, response, values=False):
    import numpy as np
    import pyarrow as pa
    if 'path' not in response:return [],0
    path=Path(response['path']);need(path.is_relative_to(context['mount']),'IPC escaped owned mount')
    out=[]
    with pa.memory_map(str(path),'r') as stream:
        size=stream.size();reader=pa.ipc.open_stream(stream)
        try:
            for item in reader.iter_batches_with_custom_metadata():
                batch=item.batch;meta=dict(item.custom_metadata or {})
                if values:
                    need(batch.num_columns==1 and batch.column(0).type==pa.float32(),'value schema differs')
                    array=np.array(batch.column(0).to_numpy(zero_copy_only=False),dtype='<f4',copy=True)
                    mask=np.array(batch.column(0).is_valid().to_numpy(zero_copy_only=False),dtype='u1',copy=True)
                    array[mask==0]=0
                    out.append(dict(meta=meta,array=array,mask=mask))
                else:out.append(dict(meta=meta,columns=[c.to_pylist() for c in batch.columns]))
                del batch,item
        finally:reader.close();del reader
    return out,size


def call(context,request,values=False):
    response,elapsed=control(context,request);parts,size=ipc(context,response,values)
    return response,parts,dict(control_ns=elapsed,ipc_bytes=size,response_path=response.get('path'))


def profiles(context,count):
    path=Path(context['root'])/'queries.jsonl';offset=context.get('profile_offset',0)
    records=[];deadline=time.monotonic()+30
    while len(records)<count:
        if path.exists():
            with path.open('rb') as stream:stream.seek(offset);raw=stream.read(16*1024*1024+1)
            need(len(raw)<=16*1024*1024,'profile envelope too large')
            end=raw.rfind(b'\n')
            if end>=0:
                data=raw[:end+1];records.extend(json.loads(line) for line in data.splitlines() if line.strip());offset+=len(data)
        need(len(records)<=count,'unexpected extra registration profile')
        if len(records)==count:break
        need(time.monotonic()<deadline,'profiles incomplete after response');time.sleep(.001)
    context['profile_offset']=offset
    records.sort(key=lambda r:r['sequence'])
    need(len({r['sequence'] for r in records})==count and all(r.get('ok') is True for r in records),'invalid registration profiles')
    return records


def predicate(fmt):
    if fmt=='hdf5': fields={k:'hdf5.attr:'+quote(k,safe='') for k in PREDICATE}
    else:fields={k:'grib.message.'+v for k,v in {'shortName':'short_name','typeOfLevel':'type_of_level','level':'level','step':'step','validityDate':'validity_date','validityTime':'validity_time'}.items()}
    return dict(kind='and',operands=[dict(kind='compare',field=fields[k],operator='eq',
        value=dict(type='utf8' if isinstance(v,str) else 'int64',value=v)) for k,v in sorted(PREDICATE.items())])


def sources(fmt,paths,versions):
    return dict(paths=[dict(kind='exact',path=p) for p in paths],versions=versions,
        formats=dict(adapters='listed',adapter_ids=[fmt],include_byte_passthrough=False))


def discover_request(fmt,paths,versions,filter_units=True):
    source=sources(fmt,paths,versions)
    if fmt=='hdf5':
        req=dict(sources=source,max_sources=1024,max_datasets=65536,max_catalog_bytes=67108864)
        if filter_units:req['predicate']=predicate(fmt)
        return dict(kind='dataset_catalog',request=req)
    subset=dict(projection=['grib.message.offset'])
    if filter_units:subset['predicate']=predicate(fmt)
    return dict(kind='typed',request=dict(sources=source,subset=subset))


def discovered(fmt,response,parts):
    if fmt=='hdf5':
        return [dict(path=r['path'],version=r['version'],dataset=r['dataset_path']) for r in response['datasets']]
    rows=[]
    for part in parts:
        meta=part['meta'];need(meta.get(b'iotdbfs.sources')==b'1','metadata missing source identity')
        path=meta[b'iotdbfs.source.0.path'].decode();version=int(meta[b'iotdbfs.source.0.version'])
        for offset in part['columns'][0]:rows.append(dict(path=path,version=version,offset=offset))
    return rows


def value_request(fmt,row):
    subset=dict(projection=['hdf5.value:'+quote(row['dataset'],safe='')] if fmt=='hdf5' else ['grib.data.value'])
    if fmt=='grib':subset['predicate']=predicate(fmt)
    return dict(kind='typed',request=dict(sources=sources(fmt,[row['path']],dict(kind='exact',exact=row['version'])),subset=subset))


def binding(context,fmt,path):
    response,parts,_=call(context,discover_request(fmt,[path],dict(kind='latest'),False))
    rows=discovered(fmt,response,parts);proof=profiles(context,1)
    need(len(rows)==4 and len({r['version'] for r in rows})==1 and all(r['path']==path for r in rows),'publication inventory differs')
    return rows[0]['version'],proof[0]


def unit_key(fmt,row):
    return row['path'],row['version'],row['dataset'] if fmt=='hdf5' else int(row['offset'])


def validate(units,expected,fmt):
    import numpy as np
    actual={}
    for row,array,mask in units:
        key=unit_key(fmt,row);need(key not in actual,'duplicate output unit')
        need(array.flags.owndata and mask.flags.owndata,'output is not client-owned')
        actual[key]=dict(elements=array.size,value_sha256=hashlib.sha256(array.astype('<f4',copy=False).tobytes(order='C')).hexdigest(),
            mask_sha256=hashlib.sha256(mask.astype('u1',copy=False).tobytes(order='C')).hexdigest())
    wanted={(m['binding']['path'],m['binding']['version'],m['unit'][8:] if fmt=='hdf5' else int(m['unit'].split(':')[1])):m['native_oracle'] for m in expected}
    need(set(actual)==set(wanted),'discovery/output identities differ from full native oracle')
    for key,value in actual.items():
        need(all(value[k]==wanted[key][k] for k in value),'value/mask oracle mismatch')
    return sum(v['elements']*5 for v in actual.values())


def query_iotdb(context,group,scope,version_selector,expected):
    import numpy as np
    fmt=group['format'];stats=[];units=[]
    t0=time.perf_counter_ns()
    response,parts,stat=call(context,discover_request(fmt,scope,version_selector));stats.append(stat)
    rows=discovered(fmt,response,parts)
    need(all(r['path'] in scope for r in rows),'discovery escaped fixed candidate scope')
    t1=time.perf_counter_ns()
    for row in rows:
        _,parts,stat=call(context,value_request(fmt,row),True);stats.append(stat)
        for part in parts:
            meta=part['meta'];need(meta.get(b'iotdbfs.sources')==b'1' and
                meta.get(b'iotdbfs.source.0.path')==row['path'].encode() and
                meta.get(b'iotdbfs.source.0.version')==str(row['version']).encode(),'value provenance differs')
        arrays=[p['array'] for p in parts];masks=[p['mask'] for p in parts]
        need(arrays,'empty value result')
        units.append((row,arrays[0] if len(arrays)==1 else np.concatenate(arrays),masks[0] if len(masks)==1 else np.concatenate(masks)))
    t2=time.perf_counter_ns()
    prof=profiles(context,len(stats))
    for callstat,p in zip(stats,prof):
        callstat['server_profile']=p
        if callstat['response_path']:need(p.get('path')==callstat['response_path'],'profile/request path mismatch')
    if fmt=='hdf5':
        for p in prof:
            need(p.get('server_hdf5_execution',{}).get('mode')=='native-hdf5','actual mode is not native-hdf5')
        for p in prof[1:]:
            c=p['counters'];need(c.get('hdf5.native.slabs',0)>0 and c.get('hdf5.native.source.calls',0)>0,'native HDF source/slab not used')
            need(not c.get('hdf5.selection.decoder_compiles',0) and not c.get('hdf5.layout.pages.calls',0),'indexed HDF engine used')
    output=validate(units,expected,fmt)
    need(output==group['owned_output_bytes'],'owned output size differs')
    return dict(total_ns=t2-t0,discovery_ns=t1-t0,value_ns=t2-t1,
        oracle_verified=True,mode_verified=True,execution_mode='native-hdf5' if fmt=='hdf5' else 'iotdb-indexed',
        owned_output_bytes=output,selected_units=len(rows),discovery_call=stats[0],value_calls=stats[1:])


def query_native(native,arm,group,scope,selection,expected):
    t0=time.perf_counter_ns()
    result=native.select('scan' if arm=='FS-scan' else 'index',selection,PREDICATE,scope)
    rows=result['units'];t1=time.perf_counter_ns();units=[]
    for row in rows:
        array,mask=native.read(row)
        normalized=dict(path=row['logical_path'],version=row['version'])
        normalized.update(dataset=row['dataset']) if group['format']=='hdf5' else normalized.update(offset=row['offset'])
        units.append((normalized,array,mask))
    t2=time.perf_counter_ns()
    output=validate(units,expected,group['format']);need(output==group['owned_output_bytes'],'native output differs')
    return dict(total_ns=t2-t0,discovery_ns=t1-t0,value_ns=t2-t1,oracle_verified=True,mode_verified=True,
        execution_mode='native-hdf5' if group['format']=='hdf5' else 'native-grib',
        owned_output_bytes=output,selected_units=len(rows),native_discovery=result,native_stats=native.stats())
