"""Diagnostic replay on an owned failed M1 namespace; no scientific timing."""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
from . import run


def raw_control(context,request):
    payload=json.dumps(request,separators=(',',':')).encode()
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(300);s.connect(str(context['socket']));s.sendall(struct.pack('>I',len(payload))+payload)
        length,=struct.unpack('>I',run.client.receive(s,4));response=json.loads(run.client.receive(s,length))
    if response.get('ok') is not True:raise RuntimeError(json.dumps(response,sort_keys=True))
    return response,0


def main():
    p=argparse.ArgumentParser();p.add_argument('failed',type=Path);p.add_argument('root',type=Path);a=p.parse_args()
    frozen=json.loads((a.failed/'frozen.json').read_text());a.root.mkdir(exist_ok=False)
    old=a.failed/'grib-c0-q100-IoTDB-FS'; source=(old/'launch').read_text();ns={};exec(source.split('import os')[0],ns)
    settings=ns['SETTINGS'];settings['storage.version_directory.initialize']='false'
    env=dict(os.environ,**run.connection_environment(282710,frozen['database']))
    for key in ('LD_LIBRARY_PATH','LD_PRELOAD','IOTDB_CONFIG_FILE'):env.pop(key,None)
    run.client.control=raw_control
    for capacity in (1024,4096):
        root=a.root/str(capacity);root.mkdir();settings['query.frontend.max_registrations']=str(capacity)
        launch=root/'launch';launch.write_text(source.replace('SETTINGS='+repr(ns['SETTINGS']), 'SETTINGS='+repr(settings)))
        # Rebuild definitions explicitly; the suffix is the original exec wrapper.
        launch.write_text('#!/usr/bin/python3\nSETTINGS='+repr(settings)+'\nBINARY='+repr(ns['BINARY'])+'\nimport os'+source.split('import os',1)[1]);launch.chmod(0o700)
        report=dict(capacity=capacity,completed_queries=0,failed=False)
        before=run.sha(old/'owner/state')
        with run.private_mount(launch,root/'mount',env,root/'namespace.lock',profile=True) as c:
            try:
                scope=['/f'+str(i)+'.grib' for i in range(8)]
                for i in range(1600):
                    _,units=run.query(c,None,'IoTDB-FS','grib',scope,{'kind':'latest'})
                    report['completed_queries']+=1
            except RuntimeError as e:
                report.update(failed=True,error=str(e))
        report['owner_unchanged']=before==run.sha(old/'owner/state')
        run.need(report['owner_unchanged'],'query replay changed owner state')
        run.save(root/'report.json',report);print(json.dumps(report),flush=True)
    run.need(json.loads((a.root/'1024/report.json').read_text())['completed_queries']==512,'failure boundary differs')
    run.need(json.loads((a.root/'4096/report.json').read_text())['completed_queries']==1600,'capacity control did not complete')

if __name__=='__main__':main()
