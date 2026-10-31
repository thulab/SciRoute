"""M1: independent, durable publish/update/query traces; no old timing reused."""
from __future__ import annotations
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import time
import uuid

from experiments.fixed_matrix import data, client
from experiments.fixed_matrix.grib_catalog_client import catalog_rows
from experiments.fixed_matrix.hdf5_batch_client import append_unit, batch_request
from experiments.single_file.runner import private_mount
from experiments.single_file.environment import connection_environment
from experiments.cross_version.version_directory.v00_head_probe import Worker
from fs_index.maintenance import Reader, Writer

ARMS = ('FS-scan', 'FS-index', 'IoTDB-FS')
need = client.need
clock = time.perf_counter_ns
sha = data._sha


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n')


def query_kind(q, i):
    return 'v1' if q >= 10 and i % 10 == 0 else 'latest'


def prepare(root, fmt, campaign, q):
    root.mkdir()
    original = data._base_values
    # Distinct actual values across campaign AND q, not merely distinct paths.
    delta = (campaign * 3 + (1, 10, 100).index(q) + 1) * 1024
    codes = data._codes() if fmt == 'grib' else None
    entries = []
    try:
        for ordinal in (1, 2, 3):
            data._base_values = lambda f, s, n, o=ordinal: original(f, s, n) + delta + o * 8
            for file_id in range(8):
                path = root / f'f{file_id}-v{ordinal}.{fmt}'
                spec = dict(size_profile='S0')
                if fmt == 'grib':
                    data._grib_create(path, spec, file_id, codes)
                    units = data._grib_inspect(path, codes)
                else:
                    data._hdf_create(path, spec, file_id)
                    units = data._hdf_inspect(path)
                path.chmod(0o444)
                entries.append(dict(file_id=file_id, ordinal=ordinal, file=str(path),
                    logical_path=f'/f{file_id}.{fmt}', bytes=path.stat().st_size, sha256=sha(path), units=units))
    finally:
        data._base_values = original
    need(len({e['sha256'] for e in entries}) == 24, 'payloads must differ across all versions')
    save(root/'manifest.json', dict(format=fmt, campaign=campaign, q=q, entries=entries))
    return entries


def expected(entries, current, kind, bindings, fmt):
    out=[]
    for e in entries:
        if e['ordinal'] != (1 if kind == 'v1' else current[e['file_id']]): continue
        for unit in e['units']:
            if not data._matches(unit['attributes']): continue
            identity = ('dataset:'+unit['dataset'] if fmt=='hdf5' else
                        'message:'+str(unit['offset'])+':'+str(unit['length']))
            out.append(dict(binding=dict(path=e['logical_path'],version=bindings[e['file_id'],e['ordinal']]),
                            unit=identity,native_oracle=unit['oracle']))
    need(len(out)==2, 'fixed two selected units required')
    return out


def db_discover(context,fmt,scope,versions,filtered=True):
    request=client.discover_request(fmt,scope,versions,filtered)
    if fmt=='grib': request['kind']='format_catalog'
    response,_=client.control(context,request)
    return catalog_rows(response) if fmt=='grib' else client.discovered(fmt,response,[])


def query(context, reader, arm, fmt, scope, versions):
    units=[]; start=clock()
    if arm=='IoTDB-FS':
        rows=db_discover(context,fmt,scope,versions); selected=clock()
        if fmt=='hdf5' and rows:
            response,_=client.control(context,batch_request(rows))
            need(response.get('kind')=='published_routes' and len(response['items'])==len(rows),'batch delivery differs')
            for row,result in zip(rows,response['items']):
                parts,_=client.ipc(context,result,True); append_unit(row,parts,units)
        else:
            for row in rows:
                _,parts,_=client.call(context,client.value_request(fmt,row),True)
                append_unit(row,parts,units)
        refreshed=None
    else:
        result=reader.select('scan' if arm=='FS-scan' else 'index', versions,client.PREDICATE,scope)
        rows=result['units']; selected=clock(); refreshed={k:v for k,v in result.items() if k!='units'}
        for row in rows:
            values,mask=reader.read(row)
            normalized=dict(path=row['logical_path'],version=row['version'])
            normalized.update(dataset=row['dataset']) if fmt=='hdf5' else normalized.update(offset=row['offset'])
            units.append((normalized,values,mask))
    end=clock()
    return dict(total_ns=end-start,discovery_ns=selected-start,value_ns=end-selected,refresh=refreshed),units


def launcher(path,binary,root,namespace,initialize):
    settings={'storage.version_directory.mode':'owner','storage.version_directory.table_prefix':namespace,
              'storage.version_directory.owner_directory':str(root/'owner'),
              'storage.version_directory.initialize':str(initialize).lower(),
              'storage.native_file_root':str(root/'native-files'),'query.hdf5.mode':'native-hdf5',
              'storage.version_directory.reader_pool_size':'2','query.frontend.max_registrations':'4096','log.dir':str(root/'logs')}
    code='''#!/usr/bin/python3
import os
from pathlib import Path
with Path(os.environ['IOTDB_CONFIG_FILE']).open('a') as f:
    for k,v in SETTINGS.items(): f.write(k+'='+v+'\\n')
os.execv(BINARY,[BINARY,*__import__('sys').argv[1:]])
'''
    path.write_text('#!/usr/bin/python3\nSETTINGS='+repr(settings)+'\nBINARY='+repr(str(binary))+'\n'+code.split('\n',1)[1])
    path.chmod(0o700)
    return settings


def resource():
    membership=Path('/proc/self/cgroup').read_text().strip().split('::')[1]
    group=Path('/sys/fs/cgroup')/membership.lstrip('/')
    result={k:(group/k).read_text().strip() for k in ('cpu.max','cpuset.cpus.effective','memory.max','memory.swap.max','memory.events','memory.peak','cpu.stat')}
    result.update(cgroup=str(group),parent_cpu_max=(group.parent/'cpu.max').read_text().strip(),parent_partition=(group.parent/'cpuset.cpus.partition').read_text().strip())
    return result


def proc_usage(pid):
    p=Path('/proc')/str(pid)
    fields=(p/'stat').read_text().split(') ')[1].split()
    return dict(start_ticks=fields[19],cpu_ticks=int(fields[11])+int(fields[12]),io=(p/'io').read_text(),status={k:v.strip() for k,v in (line.split(':',1) for line in (p/'status').read_text().splitlines() if ':' in line) if k in ('VmRSS','VmHWM','Threads','Cpus_allowed_list','Mems_allowed_list')})


def run_trace(args,root,entries,fmt,campaign,q,arm,env,admin):
    root.mkdir(); scope=sorted({e['logical_path'] for e in entries}); bindings={}; current={i:1 for i in range(8)}
    pending=[]; publications=[]; namespace='vd_m1_'+uuid.uuid4().hex[:16]
    launch=root/'launch'; settings=launcher(launch,args.binary,root,namespace,True) if arm=='IoTDB-FS' else None
    rb=resource(); db_before=proc_usage(args.datanode_pid)
    need((Path('/proc')/str(args.datanode_pid)/'cgroup').read_text().strip().endswith('/'+Path(rb['cgroup']).parent.name+'/database'),'DataNode outside shared parent')
    need(rb['memory.max']=='2147483648' and rb['cpu.max']=='max 100000' and rb['parent_cpu_max']=='200000 100000' and rb['parent_partition']=='root' and rb['cpuset.cpus.effective']=='2-3','resource policy differs')
    initialization=None; finish=None; status=None; checkpoint=None
    with ExitStack() as stack:
        start=clock()
        context=stack.enter_context(private_mount(launch,root/'mount',env,root/'namespace.lock',profile=True)) if arm=='IoTDB-FS' else None
        writer=stack.enter_context(Writer(args.library,root/'store',create=True,indexed=arm=='FS-index')) if context is None else None
        reader=stack.enter_context(Reader(args.library,root/'store')) if writer else None
        initialized=clock(); initialization=initialized-start
        for event,e in enumerate(entries):
            tick=clock()
            if writer:
                receipt=writer.publish(e['file'],logical_path=e['logical_path'],format=fmt,version=e['ordinal'],
                    ordinal=e['ordinal'],expected_version=e['ordinal']-1,op_id=f"f{e['file_id']}-v{e['ordinal']}")
                version=receipt['result']['version']
                need(receipt['metrics']['extracted_files']==int(arm=='FS-index'),'incremental extraction count differs')
            else:
                target=context['mount']/e['logical_path'].lstrip('/')
                with open(e['file'],'rb',buffering=0) as source, target.open('wb',buffering=0) as output:
                    shutil.copyfileobj(source,output,1024*1024); copied=clock(); os.fsync(output.fileno()); synced=clock()
                closed=clock()
                rows=db_discover(context,fmt,[e['logical_path']],{'kind':'latest'},False)
                need(len(rows)==4 and len({r['version'] for r in rows})==1,'publication inventory incomplete')
                version=rows[0]['version']
                receipt=dict(copy_ns=copied-tick,file_fsync_ns=synced-copied,close_ns=closed-synced,binding_ns=clock()-closed)
            publications.append(dict(event=event,file_id=e['file_id'],ordinal=e['ordinal'],total_ns=clock()-tick,receipt=receipt,version=version))
            bindings[e['file_id'],e['ordinal']]=version; current[e['file_id']]=e['ordinal']
            if event==7:
                base_lo=min(bindings.values()); base_hi=max(bindings.values())+1
            if event<8:continue
            if arm=='IoTDB-FS':need(version>=base_hi,'history fence overlaps update')
            for i in range(q):
                kind=query_kind(q,i)
                selector=({'kind':'between','lower_inclusive':base_lo,'upper_exclusive':base_hi} if arm=='IoTDB-FS'
                    else {'kind':'ordinal','ordinals':[1]}) if kind=='v1' else {'kind':'latest'}
                row,units=query(context,reader,arm,fmt,scope,selector)
                row.update(update=event-8,query=i,kind=kind)
                # Save current ordinals; hash/oracle work is deferred beyond the trace.
                pending.append((row,units,dict(current)))
        foreground=clock()
        if writer:
            reader.close(); wal_before=(root/'store/index.sqlite-wal').stat().st_size
            checkpoint=writer.checkpoint(); checkpoint['wal_bytes_before']=wal_before
            need(not checkpoint['busy'] and checkpoint['wal_bytes']==0,'unfinished SQLite checkpoint')
            writer.close()
        else:
            need(admin.call('flush')=='OK','database FLUSH failed')
        flushed=clock(); stack.close(); end=clock()
    ra=resource(); db_after=proc_usage(args.datanode_pid)
    save(root/'measured-before-audit.json',dict(total_ns=end-start,foreground_ns=foreground-start,initialization_ns=initialization,explicit_flush_ns=flushed-foreground,close_ns=end-flushed,publications=publications,queries=[r for r,_,_ in pending],checkpoint=checkpoint,oracle_pending=True))
    need('oom_kill 0' in ra['memory.events'] and 'oom 0' in ra['memory.events'],'resource OOM')
    records=[]
    for row,units,snapshot in pending:
        want=expected(entries,snapshot,row['kind'],bindings,fmt)
        need(client.validate(units,want,fmt)==40960,'owned output differs')
        row['oracle_verified']=True; records.append(row)
    del pending
    if arm=='IoTDB-FS':
        profiles=0; counters={}
        for line in (root/'mount/queries.jsonl').open():
            p=json.loads(line);need(p.get('ok') is True,'failed server query profile');profiles+=1
            if fmt=='hdf5':need(p.get('server_hdf5_execution',{}).get('mode')=='native-hdf5','HDF mode differs')
            for k,v in p.get('counters',{}).items():
                if isinstance(v,(int,float)):counters[k]=counters.get(k,0)+v
        status=dict(profiles=profiles,counters=counters,native_file_bytes=sum(p.stat().st_size for p in (root/'native-files').rglob('*') if p.is_file()))
    else:
        with Reader(args.library,root/'store') as r:status=r.status()
        need(status['test_faults_enabled'] is False,'test library forbidden')
    # Reopen all historical sources; full-file hash checks prove retention and public version identity.
    if arm=='IoTDB-FS':
        launcher(root/'reopen',args.binary,root,namespace,False)
        with private_mount(root/'reopen',root/'reopen-mount',env,root/'namespace.lock',profile=False) as c:
            for e in entries:
                v=bindings[e['file_id'],e['ordinal']]; p=Path(e['logical_path'])
                exact=c['mount']/f'.{p.stem}_{v}{p.suffix}'
                need(sha(exact)==e['sha256'],'reopened historical bytes differ')
            for kind in ('latest','v1'):
                selector={'kind':'latest'} if kind=='latest' else {'kind':'between','lower_inclusive':base_lo,'upper_exclusive':base_hi}
                _,units=query(c,None,arm,fmt,scope,selector)
                client.validate(units,expected(entries,current,kind,bindings,fmt),fmt)
    else:
        with Reader(args.library,root/'store') as r:
            for e in entries:
                sel=r.select('scan' if arm=='FS-scan' else 'index',{'kind':'ordinal','ordinals':[e['ordinal']]},e['units'][0]['attributes'],[e['logical_path']])
                need(bool(sel['units']),'reopened exact catalog differs')
                need(sha(sel['units'][0]['native_path'])==e['sha256'],'reopened bytes differ')
            for kind in ('latest','v1'):
                selector={'kind':'latest'} if kind=='latest' else {'kind':'ordinal','ordinals':[1]}
                _,units=query(None,r,arm,fmt,scope,selector)
                client.validate(units,expected(entries,current,kind,bindings,fmt),fmt)
    row=dict(complete=True,format=fmt,campaign=campaign,q=q,arm=arm,namespace=namespace if context else None,
        total_ns=end-start,foreground_ns=foreground-start,initialization_ns=initialization,
        explicit_flush_ns=flushed-foreground,close_ns=end-flushed,publications=publications,
        queries=records,status=status,checkpoint=checkpoint,resource_before=rb,resource_after=ra,
        database_before=db_before,database_after=db_after,settings=settings,reopen_verified=True,
        source_manifest_sha256=sha(Path(entries[0]['file']).parent/'manifest.json'))
    save(root/'trace.json',row)
    print(json.dumps(dict(event='trace_complete',format=fmt,campaign=campaign,q=q,arm=arm,total_s=(end-start)/1e9)),flush=True)
    return row


def main():
    parser=argparse.ArgumentParser()
    for name in ('work','binary','library','admin-binary'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--source-pid',type=int,required=True);parser.add_argument('--datanode-pid',type=int,required=True)
    parser.add_argument('--pilot',action='store_true');args=parser.parse_args()
    def cancelled(sig,frame):raise RuntimeError('M1 worker cancelled; unwind owned resources')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,cancelled)
    args.work.mkdir(parents=True,exist_ok=False); report=dict(complete=False,protocol='iotdbfs-maintenance-M1/1',traces=[])
    save(args.work/'report.json',report)
    database='v00_head_'+uuid.uuid4().hex[:16]
    env=dict(os.environ,**connection_environment(args.source_pid,database))
    for key in list(env):
        if key.startswith('IOTDBFS_') or key in ('LD_PRELOAD','LD_LIBRARY_PATH','IOTDB_CONFIG_FILE'):env.pop(key,None)
    admin_env=dict(env,IOTDB_V00_PROBE_DATABASE=database,IOTDB_V00_PROBE_PASSWORD=env['IOTDB_PASS'])
    adminpath=args.work/'sql-admin';shutil.copy2(args.admin_binary,adminpath)
    Worker.environment=admin_env; admin=Worker(adminpath)
    before={str(pid):proc_usage(pid) for pid in (args.source_pid,args.datanode_pid)}
    source_root=Path(__file__).resolve().parents[2]
    sources={str(p.relative_to(source_root)):sha(p) for p in source_root.rglob('*.py')}
    save(args.work/'source-closure.json',sources)
    try:
        need(admin.call('init')=='OK','isolated DB initialization failed')
        need(admin.call('settings')=='CONFIG\tSYNC\tfalse','effective durability policy differs')
        save(args.work/'frozen.json',dict(database=database,binary=str(args.binary),binary_sha256=sha(args.binary),library=str(args.library),
            library_sha256=sha(args.library),admin_sha256=sha(adminpath),resource=resource(),services_before=before,
            predicate=client.PREDICATE,pilot=args.pilot,background_database_unlimited=False,database_memory_unlimited=True,database_restarted=False,
            durability='immutable native file + file/directory fsync + SQLite FULL or IoTDB SYNC WAL; explicit checkpoint/FLUSH before close',
            oracle='full values/masks and source identity after timing; all 24 exact files verified after clean reopen; not a power-loss test'))
        # Entire input corpus frozen before any timed arm; all arms consume identical files.
        plans=[]
        for campaign in (range(1) if args.pilot else range(3)):
            for fmt in ('grib','hdf5'):
                for q in ((1,) if args.pilot else (1,10,100)):
                    key=f'{fmt}-c{campaign}-q{q}';entries=prepare(args.work/(key+'-input'),fmt,campaign,q)
                    plans.append((key,entries,fmt,campaign,q))
        for key,entries,fmt,campaign,q in plans:
            order=ARMS[campaign:]+ARMS[:campaign]
            for arm in order:
                root=args.work/(key+'-'+arm)
                row=run_trace(args,root,entries,fmt,campaign,q,arm,env,admin)
                report['traces'].append(str(root/'trace.json'));save(args.work/'report.json',report)
        need(len(report['traces'])==(6 if args.pilot else 54),'trace coverage incomplete')
        need(all(sha(source_root/p)==h for p,h in sources.items()),'runner changed during collection')
        report.update(complete=True,services_after={str(pid):proc_usage(pid) for pid in (args.source_pid,args.datanode_pid)},database=database)
        need(all(report['services_after'][pid]['start_ticks']==before[pid]['start_ticks'] for pid in before),'shared service restarted')
    except BaseException as e:
        report['failure']=dict(type=type(e).__name__,message=str(e));raise
    finally:
        admin.close();save(args.work/'report.json',report)

if __name__=='__main__':main()
