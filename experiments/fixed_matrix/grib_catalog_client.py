"""Explicit GRIB catalog discovery; unchanged FM1 typed value delivery.

No protocol fallback. Oracle and profiles are checked outside the full t0..t2
clock, matching the sealed client. Catalog parsing/validation is inside d.
"""
from __future__ import annotations
import time
from . import client


def catalog_request(scope, versions):
    return dict(kind='format_catalog', request=client.discover_request('grib', scope, versions)['request'])


def catalog_rows(response):
    client.need(response.get('kind') == 'format_catalog' and 'path' not in response,
                'catalog response must directly deliver positions without a result route')
    units=response.get('units')
    client.need(isinstance(units,list) and len(units)<=65536,'catalog unit bound differs')
    rows=[]
    for unit in units:
        client.need(isinstance(unit,dict) and all(k in unit for k in
                    ('path','version','format_id','revision','unit_id','offset','length')),'catalog locator incomplete')
        client.need(isinstance(unit['path'],str) and unit['path'].startswith('/') and
                    type(unit['version']) is int and unit['version']>0 and unit['format_id']=='grib' and
                    type(unit['revision']) is int and 0<=unit['revision']<=2**64-1 and
                    isinstance(unit['unit_id'],str) and unit['unit_id'] and
                    type(unit['offset']) is int and unit['offset']>=0 and
                    type(unit['length']) is int and unit['length']>0 and
                    unit['offset']<=2**64-1-unit['length'] and
                    unit['unit_id']=='message:'+str(unit['offset'])+':'+str(unit['length']),'catalog locator invalid')
        rows.append(dict(unit))
    client.need(len({(r['path'],r['version'],r['offset']) for r in rows})==len(rows),'duplicate catalog locator')
    return rows


def verify_locators(rows,expected):
    wanted={}
    for match in expected:
        tag,offset,length=match['unit'].split(':')
        client.need(tag=='message','oracle unit is not GRIB')
        wanted[(match['binding']['path'],match['binding']['version'],int(offset))]=int(length)
    actual={(r['path'],r['version'],r['offset']):r['length'] for r in rows}
    client.need(actual==wanted,'catalog positions/lengths differ from sealed oracle')


def query(context,group,scope,versions,expected,*,lightweight):
    client.need(group['format']=='grib','GRIB discovery ablation only')
    if not lightweight:
        row=client.query_iotdb(context,group,scope,versions,expected)
        row['discovery_protocol']='typed'
        return row
    import numpy as np
    stats=[];units=[]
    t0=time.perf_counter_ns()
    response,elapsed=client.control(context,catalog_request(scope,versions))
    rows=catalog_rows(response)
    stats.append(dict(control_ns=elapsed,ipc_bytes=0,response_path=None))
    client.need(all(r['path'] in scope for r in rows),'discovery escaped fixed candidate scope')
    t1=time.perf_counter_ns()
    for row in rows:
        _,parts,stat=client.call(context,client.value_request('grib',row),True);stats.append(stat)
        for part in parts:
            meta=part['meta'];client.need(meta.get(b'iotdbfs.sources')==b'1' and
                meta.get(b'iotdbfs.source.0.path')==row['path'].encode() and
                meta.get(b'iotdbfs.source.0.version')==str(row['version']).encode(),'value provenance differs')
        arrays=[p['array'] for p in parts];masks=[p['mask'] for p in parts]
        client.need(arrays,'empty value result')
        units.append((row,arrays[0] if len(arrays)==1 else np.concatenate(arrays),
                      masks[0] if len(masks)==1 else np.concatenate(masks)))
    t2=time.perf_counter_ns()
    profiles=client.profiles(context,len(stats))
    for callstat,profile in zip(stats,profiles):
        callstat['server_profile']=profile
        if callstat['response_path']:
            client.need(profile.get('path')==callstat['response_path'],'profile/request path differs')
    client.need(profiles[0].get('operation')=='format_catalog' and 'path' not in profiles[0],
                'catalog must not publish a typed result route')
    verify_locators(rows,expected)
    output=client.validate(units,expected,'grib')
    client.need(output==group['owned_output_bytes'],'owned output size differs')
    return dict(total_ns=t2-t0,discovery_ns=t1-t0,value_ns=t2-t1,oracle_verified=True,
        mode_verified=True,execution_mode='iotdb-indexed',owned_output_bytes=output,
        selected_units=len(rows),discovery_call=stats[0],value_calls=stats[1:],
        discovery_protocol='format_catalog',catalog_units=rows)
