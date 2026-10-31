"""No file publication, copying or index registration in this campaign.

Reopen three archived managed namespaces and cloned local catalog/journals.
Both filesystem arms read exactly the same immutable files. All arms execute
one first query and two warmups per source before independent Q=1/10/100 sessions.
"""
from contextlib import ExitStack
import argparse,json,os,shutil,signal,time,sys,platform
from pathlib import Path
from experiments.lifecycle_table5.run import save,ARMS
from experiments.lifecycle_table5.database import connection_environment
from experiments.lifecycle_table5.qualification import quiesce,live_environment
from experiments.single_file.section42_20261026 import read_matrix as rm,sessions,writes
from experiments.single_file.section42_20261026.read_matrix_native import NativeHdf5,NativeGrib,need
from experiments.single_file.section42_20261026.read_driver import _profile
from experiments.single_file.runner import private_mount
from experiments.cross_version.version_directory.v00_head_probe import Worker
from experiments.cross_version.resource_limited.run import journal_snapshot
from experiments.maintenance_lifecycle.run import resource,proc_usage
from experiments.fixed_matrix import query_task_client
from experiments.resident_table5.head import head
from fs_index.lifecycle import Catalog


def resource_detail(pid):
    result=resource();group=Path(result['cgroup'])
    result['kernel']={role:{name:((folder/name).read_text().strip() if (folder/name).exists() else {'unavailable':'controller file absent'}) for name in ('memory.stat','memory.current','memory.peak','io.stat','cpu.stat')} for role,folder in [('query',group),('database',group.parent/'database')]}
    result['database_process']=proc_usage(pid)
    return result


def main():
    p=argparse.ArgumentParser()
    for n in ('work','prior','database-root','pid-file','head-probe'):p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();a.work.mkdir(parents=True,exist_ok=False)
    def cancel(sig,frame):raise RuntimeError('resident campaign cancelled; unwind owned mount')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,cancel)
    prior=a.prior/'collection';frozen=json.loads((prior/'frozen.json').read_text())
    priorrows=[json.loads(l) for l in (prior/'lifecycles.jsonl').read_text().splitlines()]
    oldenv=json.loads((prior/'environment.json').read_text())
    need(json.loads((a.prior/'supervisor-report.json').read_text())['complete'],'prior incomplete')
    binary=Path(frozen['product']);need(writes.sha_file(binary)==frozen['product_sha256'],'binary changed')
    libs=oldenv['native_libraries']
    for path,digest in libs.items():need(writes.sha_file(Path(path))==digest,'native library changed')
    hlib=next(Path(x) for x in libs if 'libhdf5' in x);glib=next(x for x in libs if 'libeccodes' in x)
    env=dict(os.environ,**connection_environment(a.database_root,frozen['database']))
    for key in list(env):
        if key.startswith('IOTDBFS_') or key in ('LD_PRELOAD','LD_LIBRARY_PATH','IOTDB_CONFIG_FILE'):env.pop(key,None)
    env.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    adminpath=a.work/'sql-admin';shutil.copy2(prior/'sql-admin',adminpath)
    Worker.environment=dict(env,IOTDB_V00_PROBE_DATABASE=frozen['database'],IOTDB_V00_PROBE_PASSWORD=env['IOTDB_PASS'])
    admin=Worker(adminpath,port=int(env['IOTDB_PORT']));pid=int(a.pid_file.read_text())
    rows=[];warmups=[];report=dict(complete=False,protocol='iotdbfs-resident-query/1',records=0,queries=0)
    save(a.work/'report.json',report)
    source_root=Path(__file__).resolve().parents[2]
    closure={str(p.relative_to(source_root)):writes.sha_file(p) for folder in ('fs_index','experiments')
        for p in (source_root/folder).rglob('*.py') if not any(x in p.parts for x in ('work','__pycache__','results'))}
    save(a.work/'source-closure.json',closure)
    try:
        need(admin.call('settings')=='CONFIG\tSYNC\tfalse','durability policy differs')
        import numpy,pyarrow
        save(a.work/'environment.json',dict(native_libraries=libs,python=sys.version,executable=sys.executable,numpy=numpy.__version__,pyarrow=pyarrow.__version__,platform=platform.platform(),head_probe_sha256=writes.sha_file(a.head_probe),live_database=live_environment(pid,int(env['IOTDB_PORT']),a.work)))
        save(a.work/'frozen.json',dict(frozen,protocol=report['protocol'],prior=str(a.prior),
            clock='query session entry through final owned result and reader release',
            outside_clock='already stored data, persisted indexes, service/catalog reopening, first query and two warmups, output audits',
            comparison='conditional on stored and indexed data; no index maintenance or ingestion cost claim',
            warmup_queries_per_source=3,filesystem_paths_shared=True))
        for campaign in range(3):
            qorder=(1,10,100)[campaign:]+(1,10,100)[:campaign]
            for arm in ARMS[campaign:]+ARMS[:campaign]:
                block=a.work/f'c{campaign}-{arm}';block.mkdir()
                quiesce(admin,pid,block/'quiescence-before.json')
                context=None;catalog=None;original_journal=None;original_digest=None;head_before=None;namespace=None
                with ExitStack() as stack:
                    if arm=='IoTDB-FS':
                        previous=prior/f'c{campaign}-IoTDB-FS'
                        settings=json.loads((previous/'settings.json').read_text())
                        original_journal=Path(settings['storage.version_directory.owner_directory'])
                        original_digest=journal_snapshot(original_journal)
                        shutil.copytree(original_journal,block/'owner')
                        need(journal_snapshot(block/'owner')==original_digest,'journal clone differs')
                        settings.update({'storage.version_directory.initialize':'false',
                            'storage.version_directory.owner_directory':str(block/'owner'),'log.dir':str(block/'logs')})
                        save(block/'settings.json',settings)
                        namespace=settings['storage.version_directory.table_prefix']
                        head_before,_=head(a.head_probe,namespace,env,block/'head-before')
                        save(block/'head-before.json',head_before)
                        launch=block/'launcher'
                        launch.write_text('#!/usr/bin/python3\nimport os\nfrom pathlib import Path\nSETTINGS='+repr(settings)+
                            '\nBINARY='+repr(str(binary))+"\nwith Path(os.environ['IOTDB_CONFIG_FILE']).open('a') as f:\n"
                            "    for k,v in SETTINGS.items():f.write(k+'='+v+'\\n')\nos.execv(BINARY,[BINARY,*__import__('sys').argv[1:]])\n")
                        launch.chmod(0o700)
                        reopen_begin=time.perf_counter_ns()
                        context=stack.enter_context(private_mount(launch,block/'mount',env,previous/'namespace.lock',profile=True,startup_timeout_s=900))
                        save(block/'reopen.json',dict(wall_ns=time.perf_counter_ns()-reopen_begin,startup_ns=context['startup_ns'],deadline_s=900,scope='service recovery outside query clocks'))
                        context.update(profile_offset=0,max_result_bytes=67108864,hdf5_mode='iotdb-indexed')
                    elif arm=='FS-index':
                        previous=prior/f'c{campaign}-FS-index'
                        need(not Path(str(previous/'index.sqlite')+'-wal').exists() or Path(str(previous/'index.sqlite')+'-wal').stat().st_size==0,'archived catalog WAL not empty')
                        shutil.copy2(previous/'index.sqlite',block/'index.sqlite')
                        catalog=stack.enter_context(Catalog(block/'index.sqlite'))
                    for case in frozen['cases']:
                        sid=case['source_id'];reader=NativeHdf5(hlib) if case['format_id']=='hdf5' else NativeGrib(glib)
                        saved=next(r for r in priorrows if r['campaign']==campaign and r['source_id']==sid and r['q']==100 and r['arm']==('IoTDB-FS' if context else 'FS-index'))
                        if context:
                            actual=dict(case,**saved['exact_source'])
                        else:
                            raw=saved['destination'];need(writes.sha_file(Path(raw))==case['sha256'],'resident raw changed')
                            actual=dict(case,raw=raw,logical_path=raw,version=1)
                        def measure(q,role):
                            pending=[];rb=resource_detail(pid)
                            need(rb['parent_cpu_max']=='200000 100000' and rb['memory.max']=='2147483648' and rb['memory.swap.max']=='0' and rb['cpuset.cpus.effective']=='2-3','resource envelope changed')
                            if context:r=sessions.run_iotdb_session(context=context,spec=actual,q=q,read_iotdb=rm.read_iotdb,deferred_audits=pending)
                            elif catalog:r=sessions.run_session(q=q,prepare_reuse=0,prepare=lambda:None,read_owned=lambda _,i:catalog.read(reader,actual),release=lambda _:None,deferred_audits=pending)
                            else:r=sessions.run_native_session(reader=reader,spec=actual,q=q,prepare_reuse=0,deferred_audits=pending)
                            for observation,owned in pending:
                                observation['validation']=rm.validate_owned(actual,owned);observation['oracle_verified']=True
                            if context:_profile(context,r['observations'],case,'iotdb-indexed')
                            ra=resource_detail(pid);need('oom_kill 0' in ra['memory.events'] and 'oom 0' in ra['memory.events'],'OOM')
                            r.update(campaign=campaign,arm=arm,source_id=sid,q=q,role=role,
                                source_sha256=case['sha256'],source_binding=actual.get('logical_path'),source_version=actual.get('version'),
                                resource_before=rb,resource_after=ra)
                            output='sessions.jsonl' if role=='measurement' else 'warmups.jsonl'
                            with (a.work/output).open('a') as f:f.write(json.dumps(r)+'\n');f.flush();os.fsync(f.fileno())
                            (rows if role=='measurement' else warmups).append(r)
                        for j in range(3):measure(1,'first' if j==0 else 'warmup')
                        for q in qorder:
                            measure(q,'measurement');report.update(records=len(rows),queries=sum(r['q'] for r in rows));save(a.work/'report.json',report)
                            print(json.dumps(dict(event='complete',campaign=campaign,arm=arm,source=sid,q=q,total_s=rows[-1]['session_wall_ns']/1e9)),flush=True)
                    if context:
                        need(not query_task_client._harvest(context,context['profile_offset'],0,False),'extra profiles')
                        pub=context['root']/'publication.jsonl';need(pub.is_file() and pub.stat().st_size==0,'publication instrumentation missing or nonempty')
                    if catalog:save(block/'catalog-status.json',catalog.stats())
                if original_journal:
                    need(journal_snapshot(original_journal)==original_digest and journal_snapshot(block/'owner')==original_digest,'read changed owner journal')
                    head_after,_=head(a.head_probe,namespace,env,block/'head-after')
                    need(head_after==head_before,'database head changed')
                    save(block/'head-after.json',head_after)
                    save(block/'journal-check.json',dict(unchanged=True,digest=original_digest,head_unchanged=head_after==head_before))
                save(block/'closed.json',dict(complete=True,unmounted=not context or not os.path.ismount(context['mount'])))
                quiesce(admin,pid,block/'quiescence-after.json')
        need(len(rows)==135 and sum(r['q'] for r in rows)==4995 and len(warmups)==135,'matrix incomplete')
        need(all(writes.sha_file(source_root/p)==sha for p,sha in closure.items()),'source changed')
        report.update(complete=True,warmup_and_first_queries=len(warmups),data_publications=0)
    except BaseException as error:
        report['failure']=dict(type=type(error).__name__,message=str(error));raise
    finally:admin.close();save(a.work/'report.json',report)

if __name__=='__main__':main()
