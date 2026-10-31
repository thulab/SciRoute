"""Section 4.2 main GRIB/HDF5 exact-query components, without live orchestration.

Native filesystem reads and IoTDB-FS reads share entry/discovery/owned-output
clocks. The caller owns mounts, resources, repetition order and profile audits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import struct
import time
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pyarrow as pa

try:
    from .read_matrix_native import NativeHdf5, NativeGrib, need, clock_record
except ImportError:
    from read_matrix_native import NativeHdf5, NativeGrib, need, clock_record


def sha_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda: stream.read(1024*1024), b''):
            value.update(part)
    return value.hexdigest()


def main_cases(repo, bindings=None):
    """Load existing raw inputs/oracles; never generate or publish sources.

    bindings maps H-T2/H-T160/H-T1024/G-M22/G-M4598 to newly published source
    receipts containing logical_path and version. Legacy strings are solely
    historical manifest keys, and are not experiment-group labels.
    """
    repo, bindings = Path(repo), bindings or {}
    root = repo/'experiments/single_file'
    hpath = root/'work/prep-20261012/data/manifest.json'
    gpath = root/'work/grib-paired-q100-20261012/data-manifest.json'
    h, g = json.loads(hpath.read_text()), json.loads(gpath.read_text())
    sources, cases = {}, []
    for source_id, oldkey in [('H-T2','small'),('H-T160','medium'),('H-T1024','large')]:
        raw = h['sources'][oldkey]
        sources[source_id] = dict(source_id=source_id, format_id='hdf5',
            raw=str((hpath.parent/raw['file']).resolve()), bytes=raw['bytes'],sha256=raw['sha256'],
            source_shape=raw['shape'], dtype='<f4', dataset='/T',
            oracle_manifest=str(hpath), oracle_manifest_sha256=sha_file(hpath))
        for case_id,oldcase in [('H-F1-T'+str(raw['shape'][0]),'point')]+(
            [('H-F2-R2016','small_roi'),('H-F2-R192','cross_chunk')] if source_id=='H-T160' else []):
            case = dict(sources[source_id],case_id=case_id,**h['cases'][oldcase])
            case['oracle'] = dict(h['cases'][oldcase])
            case.update(bindings.get(source_id,{}))
            cases.append(case)
    for source_id,oldkey,case_specs in [
        ('G-M22','small',[('G-F1-M22','point')]),
        ('G-M4598','medium',[('G-F1-M4598','point'),('G-F2-Y4-X4','roi'),('G-F3-B64901','binary_one')])]:
        raw = g['sources'][oldkey]
        sources[source_id] = {key:raw[key] for key in ('raw','bytes','sha256')}
        sources[source_id].update(source_id=source_id,format_id='grib',
            candidate_units=len(raw['inventory']), selected_message=dict(raw['selected']),
            oracle_manifest=str(gpath),oracle_manifest_sha256=sha_file(gpath))
        for case_id,oldcase in case_specs:
            cases.append(dict(sources[source_id],case_id=case_id,**raw['cases'][oldcase],
                              **bindings.get(source_id,{})))
    return dict(protocol='iotdbfs-section42-main-inputs/1',sources=sources,cases=cases)


def exact_sources(spec):
    need(isinstance(spec.get('logical_path'),str) and type(spec.get('version')) is int and
         spec['version'] > 0,'exact published source receipt missing')
    return dict(paths=[dict(kind='exact',path=spec['logical_path'])],
                versions=dict(kind='exact',exact=spec['version']),
                formats=dict(adapters='listed',adapter_ids=[spec['format_id']],
                             include_byte_passthrough=False))


def grib_predicate(spec):
    names = {'level':'level','step':'step','typeOfLevel':'type_of_level','shortName':'short_name'}
    return dict(kind='and',operands=[dict(kind='compare',field='grib.message.'+names[key],
        operator='eq',value=dict(type='utf8' if isinstance(value,str) else 'int64',value=value))
        for key,value in spec['filter'].items()])


def discovery_request(spec):
    if spec['format_id']=='hdf5':
        return dict(kind='dataset_catalog',request=dict(sources=exact_sources(spec),
            dataset_path=spec.get('dataset','/T'),max_sources=1,max_datasets=65536,
            max_catalog_bytes=64*1024*1024))
    return dict(kind='format_catalog',request=dict(sources=exact_sources(spec),
        subset=dict(projection=['grib.message.offset'],predicate=grib_predicate(spec))))


def value_request(spec):
    if spec['format_id']=='hdf5':
        subset = dict(projection=['hdf5.value:'+quote(spec.get('dataset','/T'),safe='')],slices=[
            dict(dimension='hdf5.axis.'+str(i),
                 lower=dict(kind='inclusive',value=dict(type='uint64',value=origin)),
                 upper=dict(kind='exclusive',value=dict(type='uint64',value=origin+extent)))
            for i,(origin,extent) in enumerate(zip(spec['origin'],spec['shape']))])
    else:
        subset = dict(projection=(['grib.message.bytes'] if spec['view']=='binary' else
            ['grib.grid.latitude','grib.grid.longitude','grib.data.value']),predicate=grib_predicate(spec))
        if spec['view']=='binary':
            subset['output']=dict(mode='binary_view')
        else:
            subset['slices']=[dict(dimension=dimension,
                lower=dict(kind='inclusive',value=dict(type='float64',value=lower)),
                upper=dict(kind='inclusive',value=dict(type='float64',value=upper)))
                for dimension,(lower,upper) in zip(('grib.grid.latitude','grib.grid.longitude'),spec['bounds'])]
    return dict(kind='typed',request=dict(sources=exact_sources(spec),subset=subset))


def _receive(stream,count):
    body=bytearray(count)
    offset=0
    while offset<count:
        received=stream.recv_into(memoryview(body)[offset:])
        need(received>0,'short control/result body')
        offset+=received
    return body


def _open(socket_path):
    stream=socket.socket(socket.AF_UNIX)
    try:
        stream.settimeout(300)
        stream.connect(str(socket_path))
        return stream
    except BaseException:
        stream.close()
        raise


def _send(stream,request):
    payload=json.dumps(request,separators=(',',':'),allow_nan=False).encode()
    need(0<len(payload)<=1024*1024,'control request exceeds frame budget')
    stream.sendall(struct.pack('>I',len(payload))+payload)


def _response(stream):
    length,=struct.unpack('>I',_receive(stream,4))
    need(0<length<=1024*1024,'control response exceeds frame budget')
    response=json.loads(_receive(stream,length))
    if response.get('ok') is not True:
        message=response.get('message',response.get('error','unknown'))
        error=ValueError('control request failed: code='+str(response.get('code','unknown'))+
                         ', phase='+str(response.get('phase','unknown'))+', message='+str(message))
        error.response=response
        raise error
    return response


def control(socket_path,request):
    start=time.perf_counter_ns()
    with _open(socket_path) as stream:
        _send(stream,request)
        response=_response(stream)
    return response,time.perf_counter_ns()-start


def fetch(context,response):
    route=Path(response['path'])
    need(route.is_absolute() and route.is_relative_to(context['mount']) and '..' not in route.parts,
         'result route escaped owned mount')
    start=time.perf_counter_ns()
    with _open(context['socket']) as stream:
        _send(stream,dict(kind='result_fetch',version=1,path=str(route)))
        header=_response(stream)
        need(header.get('kind')=='result_fetch' and header.get('version')==1,'fetch protocol differs')
        size=header.get('size')
        need(type(size) is int and 0<size<=context.get('max_result_bytes',64*1024*1024),
             'result body exceeds fetch bound')
        body=_receive(stream,size)
    return body,dict(result_path=str(route),ipc_bytes=size,fetch_ns=time.perf_counter_ns()-start)


def extract_owned(body,spec):
    chunks,metadata=[],[]
    with pa.BufferReader(body) as stream:
        with pa.ipc.open_stream(stream) as reader:
            schema=reader.schema
            for item in reader.iter_batches_with_custom_metadata():
                chunks.append(item.batch)
                metadata.append(dict(item.custom_metadata or {}))
            table=pa.Table.from_batches(chunks,schema=schema)
            if spec['format_id']=='grib' and spec['view']=='binary':
                need(table.num_columns==1 and pa.types.is_binary(table.column(0).type),
                     'binary result schema differs')
                result=dict(values=[bytes(memoryview(v.as_py())) for v in table.column(0)],
                    mask=np.array(table.column(0).is_valid().to_numpy(zero_copy_only=False),dtype='u1',copy=True))
            elif spec['format_id']=='grib':
                need(table.num_columns==3 and [c.type for c in table.columns]==
                     [pa.float64(),pa.float64(),pa.float32()],'GRIB value result schema differs')
                result=dict(latitude=np.array(table.column(0).to_numpy(zero_copy_only=False),dtype='<f8',copy=True),
                    longitude=np.array(table.column(1).to_numpy(zero_copy_only=False),dtype='<f8',copy=True),
                    values=np.array(table.column(2).to_numpy(zero_copy_only=False),dtype='<f4',copy=True),
                    mask=np.array(table.column(2).is_valid().to_numpy(zero_copy_only=False),dtype='u1',copy=True))
            else:
                need(table.num_columns==1 and table.column(0).type==pa.float32(),
                     'HDF5 physical value result schema differs')
                result=dict(values=np.array(table.column(0).to_numpy(zero_copy_only=False),dtype='<f4',copy=True),
                    mask=np.array(table.column(0).is_valid().to_numpy(zero_copy_only=False),dtype='u1',copy=True))
            result['metadata']=metadata
            result['schema_metadata']=dict(schema.metadata or {})
    need(all(not isinstance(v,np.ndarray) or v.flags.owndata for v in result.values()),
         'client result not owned')
    return result


def read_iotdb(context,spec,request_override=None,discover=True):
    """Return unvalidated owned outputs; caller audits hashes/profiles later."""
    start=time.perf_counter_ns()
    catalog,discovery_control_ns=None,0
    if discover:
        catalog,discovery_control_ns=control(context['socket'],discovery_request(spec))
        units=catalog.get('datasets' if spec['format_id']=='hdf5' else 'units',[])
        need(len(units)==1 and units[0]['path']==spec['logical_path'] and
             units[0]['version']==spec['version'],'discovery exact source differs')
        if spec['format_id']=='hdf5':
            need(units[0]['dataset_path']==spec.get('dataset','/T'),'discovery dataset differs')
    discovered=time.perf_counter_ns()
    request=request_override if request_override is not None else value_request(spec)
    response,value_control_ns=control(context['socket'],request)
    body,stat=fetch(context,response)
    fetched=time.perf_counter_ns()
    owned=extract_owned(body,spec)
    end=time.perf_counter_ns()
    return clock_record(start,discovered,end,rows=owned['mask'].size,
        dtype='binary' if spec.get('view')=='binary' else '<f4',
        discovery_control_ns=discovery_control_ns,value_control_ns=value_control_ns,
        ipc_decode_owned_ns=end-fetched,discovery_response=catalog,
        request_sha256=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        transport='result_fetch_v1',**stat),owned


def validate_owned(spec,owned):
    """Outside measured clocks. Canonical bytes validate every element/mask."""
    mask=owned['mask']
    need(mask.dtype==np.dtype('u1') and mask.flags.owndata,'mask dtype/ownership differs')
    values=owned['values']
    if spec.get('view')=='binary':
        value_bytes=b''.join(values)
        need(len(values)==1,'binary unit count differs')
    else:
        need(isinstance(values,np.ndarray) and values.flags.owndata and values.dtype==np.dtype('<f4'),
             'value dtype/ownership differs')
        value_bytes=values.tobytes(order='C')
    hashes=dict(value_sha256=hashlib.sha256(value_bytes).hexdigest(),
                mask_sha256=hashlib.sha256(mask.tobytes(order='C')).hexdigest())
    if spec['format_id']=='grib':
        for name in ('latitude','longitude'):
            array=owned.get(name,np.empty(0,dtype='<f8'))
            need(array.dtype==np.dtype('<f8') and array.flags.owndata,'coordinate dtype/ownership differs')
            hashes[name+'_sha256']=hashlib.sha256(array.tobytes(order='C')).hexdigest()
    oracle=spec['oracle']
    need(mask.size==oracle['rows'],'output element count differs')
    need(all(oracle[key]==value for key,value in hashes.items()),'raw oracle hash differs')
    metadata=owned.get('metadata')
    if metadata is not None:
        need(bool(metadata),'result metadata omitted')
        for item in metadata:
            need(item.get(b'iotdbfs.sources')==b'1' and
                 item.get(b'iotdbfs.source.0.path')==spec['logical_path'].encode() and
                 item.get(b'iotdbfs.source.0.version')==str(spec['version']).encode() and
                 item.get(b'iotdbfs.source.0.format_id')==spec['format_id'].encode(),
                 'exact IPC provenance differs')
            if spec['format_id']=='grib' and spec['view']=='values':
                actual=[int(item[('iotdbfs.domain.dimension.'+str(i)+'.shape').encode()]) for i in range(2)]
                need(actual==oracle['messages'][0]['shape'],'GRIB output domain differs')
        if spec['format_id']=='hdf5':
            schema_meta=owned['schema_metadata']
            prefix=b'iotdbfs.result_descriptor.0.'
            need(schema_meta.get(b'iotdbfs.result_descriptors')==b'1','HDF5 result descriptor omitted')
            fields={'source.path':spec['logical_path'],'source.version':str(spec['version']),
                    'dataset.path':spec.get('dataset','/T'),'order':'C',
                    'selection.row_count':str(oracle['rows']),'output.value_type':'float32'}
            for axis in range(len(spec['shape'])):
                fields.update({f'axis.{axis}.full_extent':str(spec['source_shape'][axis]),
                    f'axis.{axis}.selection_origin':str(spec['origin'][axis]),
                    f'axis.{axis}.selection_extent':str(spec['shape'][axis])})
            need(all(schema_meta.get(prefix+k.encode())==v.encode() for k,v in fields.items()),
                 'HDF5 source/selection descriptor differs')
    return dict(oracle_verified=True,rows=mask.size,
                owned_output_bytes=len(value_bytes)+mask.nbytes+
                    sum(owned[k].nbytes for k in ('latitude','longitude') if k in owned),**hashes)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry',action='store_true')
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[3])
    parser.add_argument('--bindings',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    need(args.registry,'this entry only freezes input metadata; caller runs measurements')
    registry=main_cases(args.repo,json.loads(args.bindings.read_text()) if args.bindings else None)
    need(not args.output.exists(),'refuse to overwrite input registry')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(registry,indent=2,sort_keys=True)+'\n')
    print(json.dumps(dict(output=str(args.output),cases=len(registry['cases']),sources=len(registry['sources']))))


if __name__=='__main__':
    main()
