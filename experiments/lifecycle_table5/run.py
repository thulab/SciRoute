"""Original immutable Table 5 inputs, freshly measured scan/index/managed paths.

Database/service setup is outside copy-to-result clocks. Index registration is
inside them. Independent Q reads own output; oracle and hashes follow clocks.
No cached old timing enters this campaign, and no original corpus is modified.
"""
from contextlib import ExitStack
import argparse
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import statistics
import sqlite3
import time
import uuid

from experiments.single_file.section42_20261026 import read_matrix as rm, sessions, writes
from experiments.single_file.section42_20261026.lifecycle_collection import BASE_CASES, _native_locator, _product_locator
from experiments.single_file.section42_20261026.read_matrix_native import NativeHdf5, NativeGrib, need
from experiments.single_file.section42_20261026.read_driver import _profile
from experiments.single_file.runner import private_mount
from experiments.cross_version.version_directory.v00_head_probe import Worker
from experiments.fixed_matrix import query_task_client
from experiments.maintenance_lifecycle.run import resource, proc_usage
from experiments.lifecycle_table5.database import connection_environment
from experiments.lifecycle_table5.qualification import quiesce, live_environment
from fs_index.lifecycle import Catalog

ARMS = ('FS-scan', 'FS-index', 'IoTDB-FS')


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n')


def launcher(path, binary, root, namespace):
    settings = {'storage.version_directory.mode':'owner',
        'storage.version_directory.table_prefix':namespace,
        'storage.version_directory.owner_directory':str(root/'owner'),
        'storage.version_directory.initialize':'true',
        'storage.native_file_root':str(root/'native-files'),
        'query.hdf5.mode':'iotdb-indexed', 'query.hdf5.native_cache':'false',
        'query.hdf5.read_parallelism':'1','query.tasks.worker_threads':'4',
        'query.tasks.max_batch_size':'4','query.tasks.max_queued_tasks':'64',
        'query.result_fetch.sender_threads':'1','query.cache.memory_budget_bytes':'268435456',
        'query.cache.max_entries':'65536','storage.version_directory.reader_pool_size':'4',
        'query.frontend.max_registrations':'4096','log.dir':str(root/'logs')}
    path.write_text('#!/usr/bin/python3\nimport os\nfrom pathlib import Path\nSETTINGS='+repr(settings)+
        '\nBINARY='+repr(str(binary))+"\nwith Path(os.environ['IOTDB_CONFIG_FILE']).open('a') as f:\n"
        "    for k,v in SETTINGS.items(): f.write(k+'='+v+'\\n')\n"
        "os.execv(BINARY,[BINARY,*__import__('sys').argv[1:]])\n")
    path.chmod(0o700)
    return settings


def summarize(rows):
    result=[]
    for source in sorted({r['source_id'] for r in rows}):
        for q in sorted({r['q'] for r in rows}):
            for arm in ARMS:
                group=[r for r in rows if (r['source_id'],r['q'],r['arm'])==(source,q,arm)]
                if not group: continue
                fields={'total_s':'lifecycle_wall_ns','publication_s':None,'copy_s':None,
                        'locator_s':'source_locator_ns','query_session_s':None}
                cell=dict(source_id=source,q=q,arm=arm,count=len(group),campaigns=[r['campaign'] for r in group])
                for output,key in fields.items():
                    values=[(r[key] if key else r['write']['total_ns']+r['source_locator_ns'] if output=='publication_s'
                        else r['write']['total_ns'] if output=='copy_s' else r['session']['session_wall_ns'])/1e9 for r in group]
                    cell[output]=dict(mean=statistics.mean(values),median=statistics.median(values),min=min(values),max=max(values),raw=values)
                result.append(cell)
    return result


def main():
    parser=argparse.ArgumentParser()
    for name in ('work','repo','binary','admin-binary','database-root','pid-file'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--pilot',action='store_true')
    parser.add_argument('--qualified-pilot',type=Path)
    a=parser.parse_args()
    def cancel(sig,frame): raise RuntimeError('Table 5 worker cancelled; unwind owned resources')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,cancel)
    a.work.mkdir(parents=True,exist_ok=False)
    report=dict(complete=False,protocol='iotdbfs-table5-three-path/1',pilot=a.pilot,records=0)
    save(a.work/'report.json',report)
    registry=rm.main_cases(a.repo); cases={c['case_id']:c for c in registry['cases']}
    sources=registry['sources']
    for s in sources.values():
        need(Path(s['raw']).stat().st_size==s['bytes'] and writes.sha_file(Path(s['raw']))==s['sha256'],'frozen raw changed')
    capacity=shutil.disk_usage(a.work)
    required_free=6*9*sum(s['bytes'] for s in sources.values())+32*1024**3
    need(capacity.free>required_free,'insufficient space for retained copies, database/WAL allowance and reserve')
    admission=dict(free_bytes=capacity.free,required_free_bytes=required_free,
                   space_policy='six full representations times nine cells per source plus 32GiB reserve',worker_deadline_s=12600)
    if not a.pilot:
        need(a.qualified_pilot is not None,'completed qualification evidence required')
        prior=json.loads((a.qualified_pilot/'supervisor-report.json').read_text())
        old=json.loads((a.qualified_pilot/'collection/report.json').read_text())
        frozen=json.loads((a.qualified_pilot/'collection/frozen.json').read_text())
        need(prior['complete'] and prior['resources_restored'] and old['complete'] and old['pilot'] and old['records']==15,'qualification incomplete')
        need(frozen['product_sha256']==writes.sha_file(a.binary) and frozen['sources']==sources,'qualified inputs/product differ')
        observed=[json.loads(line) for line in (a.qualified_pilot/'collection/lifecycles.jsonl').read_text().splitlines()]
        output_bytes=max(r['session']['observations'][0]['validation']['owned_output_bytes'] for r in observed)
        extra_queries_s=sum(r['session']['session_wall_ns']/1e9 for r in observed)*3*108
        estimate=(prior['elapsed_s']*9+extra_queries_s)*1.2+360
        need(output_bytes*100<1024*1024,'Q100 retained outputs exceed the admitted small-output profile')
        need(estimate<12600,'qualification runtime projection exceeds formal deadline')
        admission.update(qualification=str(a.qualified_pilot),qualification_elapsed_s=prior['elapsed_s'],
            estimated_full_seconds_with_margin=estimate,max_q100_owned_output_bytes=100*output_bytes,
            route_count_per_managed_block=555,configured_route_capacity=4096)
    save(a.work/'admission.json',admission)
    database='v00_head_'+uuid.uuid4().hex[:16]
    env=dict(os.environ,**connection_environment(a.database_root,database))
    for key in list(env):
        if key.startswith('IOTDBFS_') or key in ('LD_PRELOAD','LD_LIBRARY_PATH','IOTDB_CONFIG_FILE'):env.pop(key,None)
    env.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    adminenv=dict(env,IOTDB_V00_PROBE_DATABASE=database,IOTDB_V00_PROBE_PASSWORD=env['IOTDB_PASS'])
    adminpath=a.work/'sql-admin';shutil.copy2(a.admin_binary,adminpath)
    Worker.environment=adminenv; admin=Worker(adminpath,port=int(env['IOTDB_PORT']))
    pid=int(a.pid_file.read_text())
    rows=[]
    source_root=Path(__file__).resolve().parents[2]
    closure={str(p.relative_to(source_root)):writes.sha_file(p) for folder in ('fs_index','experiments')
        for p in (source_root/folder).rglob('*.py') if not any(x in p.parts for x in ('work','__pycache__','results'))}
    save(a.work/'source-closure.json',closure)
    try:
        init_begin=time.perf_counter_ns()
        need(admin.call('init')=='OK','isolated database initialization failed')
        save(a.work/'database-setup.json',dict(init_ns=time.perf_counter_ns()-init_begin))
        need(admin.call('settings')=='CONFIG\tSYNC\tfalse','database durability policy differs')
        import numpy, pyarrow
        dependencies={}
        for path in (a.repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310',
                     Path('/lib/x86_64-linux-gnu/libeccodes.so.0')):
            dependencies[str(path.resolve())]=writes.sha_file(path)
        save(a.work/'environment.json',dict(platform=platform.platform(),python=platform.python_version(),
            numpy=numpy.__version__,pyarrow=pyarrow.__version__,sqlite=sqlite3.sqlite_version,
            native_libraries=dependencies,cpu_count=os.cpu_count(),meminfo=Path('/proc/meminfo').read_text(),
            kernel=Path('/proc/version').read_text(),database_deployment=json.loads((a.database_root/'deployment.json').read_text()),
            live_database=live_environment(pid,int(env['IOTDB_PORT']),a.work)))
        save(a.work/'frozen.json',dict(database=database,product=str(a.binary),product_sha256=writes.sha_file(a.binary),
            admin_sha256=writes.sha_file(adminpath),sources=sources,cases=[cases[c] for c in BASE_CASES],
            resources=resource(),database_process=proc_usage(pid),arms=ARMS,q_values=[1] if a.pilot else [1,10,100],
            campaigns=1 if a.pilot else 3,aggregation='arithmetic mean of three directly measured independent lifecycle walls',
            clock='copy entry through file fsync/close, exact source binding, index FULL commit where applicable, final owned query result',
            outside_clock='service/catalog initialization; parent directory fsync; postmeasurement full byte/hash and oracle checks; final checkpoint/DB flush',
            cache='OS caches retained; fresh native handles each Q; no FS-index encoded/decoded value cache; managed cache256MiB',
            hdf5_mode='iotdb-indexed',original_table5_archive='single-file-20261026',
            database_namespace_isolated=True,database_service_isolated=False,
            service_memory='existing licensed DataNode unchanged; actual JVM limits in environment.json'))
        for campaign in range(1 if a.pilot else 3):
            order=ARMS[campaign:]+ARMS[:campaign]
            qvalues=(1,) if a.pilot else ((1,10,100)[campaign:]+(1,10,100)[:campaign])
            for arm in order:
                block=a.work/f'c{campaign}-{arm}';block.mkdir()
                quiesce(admin,pid,block/'quiescence-before.json')
                with ExitStack() as stack:
                    context=None;catalog=None
                    if arm=='IoTDB-FS':
                        namespace='vd_t5_'+uuid.uuid4().hex[:16]
                        settings=launcher(block/'launcher',a.binary,block,namespace)
                        save(block/'settings.json',settings)
                        setup_begin=time.perf_counter_ns()
                        context=stack.enter_context(private_mount(block/'launcher',block/'mount',env,block/'namespace.lock',profile=True))
                        save(block/'managed-setup.json',dict(setup_ns=time.perf_counter_ns()-setup_begin,mount_startup_ns=context['startup_ns']))
                        context.update(profile_offset=0,max_result_bytes=67108864,hdf5_mode='iotdb-indexed')
                        destination_root=context['mount']/f'c{campaign}';destination_root.mkdir()
                    else:
                        destination_root=block/'files';destination_root.mkdir()
                        if arm=='FS-index':
                            setup_begin=time.perf_counter_ns()
                            catalog=stack.enter_context(Catalog(block/'index.sqlite',create=True))
                            save(block/'catalog-setup.json',dict(setup_ns=time.perf_counter_ns()-setup_begin,**catalog.stats()))
                    for base_id in BASE_CASES:
                        case=cases[base_id];source=sources[case['source_id']]
                        reader=(NativeHdf5(a.repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310')
                            if case['format_id']=='hdf5' else NativeGrib('/lib/x86_64-linux-gnu/libeccodes.so.0'))
                        for q in qvalues:
                            need(shutil.disk_usage(a.work).free>64*1024**3,'retained-data capacity reserve reached')
                            destination=destination_root/(case['source_id']+f'-q{q}'+Path(source['raw']).suffix)
                            pending=[];state={};rb=resource();db_before=proc_usage(pid)
                            need(rb['parent_cpu_max']=='200000 100000' and rb['memory.max']=='2147483648' and rb['memory.swap.max']=='0' and rb['cpuset.cpus.effective']=='2-3','resource envelope differs')
                            need(Path('/proc',str(pid),'cgroup').read_text().strip().endswith('/'+Path(rb['cgroup']).parent.name+'/database'),'DataNode outside shared quota')
                            print(json.dumps(dict(event='start',campaign=campaign,arm=arm,source=case['source_id'],q=q)),flush=True)
                            def locator(path):
                                if context:return _product_locator(context,path,case)
                                actual=dict(case,raw=str(path),logical_path=str(path),version=1 if catalog else None)
                                state['case']=actual
                                if catalog:
                                    receipt=catalog.register(actual,reader,sha256=source['sha256'],version=1)
                                    state['index_registration']=receipt
                                    return receipt
                                return _native_locator(path,source)
                            def query_runner(identity):
                                if context:
                                    actual=dict(case,**identity);state['case']=actual
                                    return sessions.run_iotdb_session(context=context,spec=actual,q=q,read_iotdb=rm.read_iotdb,deferred_audits=pending)
                                actual=state['case']
                                if catalog:
                                    return sessions.run_session(q=q,prepare_reuse=0,prepare=lambda:None,
                                        read_owned=lambda _,i:catalog.read(reader,actual),release=lambda _:None,deferred_audits=pending)
                                return sessions.run_native_session(reader=reader,spec=actual,q=q,prepare_reuse=0,deferred_audits=pending)
                            row=writes.run_lifecycle(source=source['raw'],destination=destination,expected_bytes=source['bytes'],
                                expected_sha256=source['sha256'],locator=locator,run_queries=query_runner)
                            need(len(pending)==q and not row['includes_query_callback_audit_overhead'],'query/oracle clock differs')
                            # Preserve old publication boundary: parent directory sync is post-clock.
                            directory_begin=time.perf_counter_ns()
                            fd=os.open(destination.parent,os.O_RDONLY)
                            try:os.fsync(fd)
                            finally:os.close(fd)
                            row['post_clock_directory_fsync_ns']=time.perf_counter_ns()-directory_begin
                            for observation,owned in pending:
                                observation['validation']=rm.validate_owned(state['case'],owned)
                                observation['oracle_verified']=True
                            pending.clear()
                            if context:
                                profiles=_profile(context,row['session']['observations'],case,'iotdb-indexed',leading_catalogs=2)
                                row['source_visibility_profiles']=[p for p in profiles if p['operation']!='typed'][:2]
                            ra=resource();need('oom_kill 0' in ra['memory.events'] and 'oom 0' in ra['memory.events'],'query OOM')
                            row.update(campaign=campaign,arm=arm,source_id=case['source_id'],q=q,oracle_verified=True,
                                resource_before=rb,resource_after=ra,database_before=db_before,database_after=proc_usage(pid),
                                index_registration=state.get('index_registration'))
                            with (a.work/'lifecycles.jsonl').open('a') as f:f.write(json.dumps(row)+'\n');f.flush();os.fsync(f.fileno())
                            rows.append(row);report['records']=len(rows);save(a.work/'report.json',report)
                            print(json.dumps(dict(event='complete',campaign=campaign,arm=arm,source=case['source_id'],q=q,total_s=row['lifecycle_wall_ns']/1e9)),flush=True)
                    if catalog:save(block/'catalog-status.json',catalog.stats())
                    if context:need(not query_task_client._harvest(context,context['profile_offset'],0,False),'extra managed profiles')
                if arm=='FS-index':
                    with Catalog(block/'index.sqlite') as reopened:
                        reverified=0
                        for prior in (r for r in rows if r['campaign']==campaign and r['arm']=='FS-index'):
                            original=cases[next(c for c in BASE_CASES if cases[c]['source_id']==prior['source_id'])]
                            spec=dict(original,raw=prior['destination'],logical_path=prior['destination'],version=1)
                            native=NativeHdf5(a.repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310') if spec['format_id']=='hdf5' else NativeGrib('/lib/x86_64-linux-gnu/libeccodes.so.0')
                            _,owned=reopened.read(native,spec);rm.validate_owned(spec,owned);reverified+=1
                        save(block/'catalog-reopened.json',dict(**reopened.stats(),reverified=reverified))
                save(block/'closed.json',dict(complete=True,unmounted=not context or not os.path.ismount(context['mount'])))
                quiesce(admin,pid,block/'quiescence-after.json')
        need(len(rows)==(15 if a.pilot else 135) and sum(r['q'] for r in rows)==(15 if a.pilot else 4995),'incomplete matrix')
        keys=[(r['campaign'],r['arm'],r['source_id'],r['q']) for r in rows]
        expected={(c,arm,s,q) for c in range(1 if a.pilot else 3) for arm in ARMS
                  for s in sources for q in ((1,) if a.pilot else (1,10,100))}
        need(len(keys)==len(set(keys)) and set(keys)==expected,'duplicate or missing matrix cell')
        need(admin.call('flush')=='OK','postmeasurement database flush failed')
        need(all(writes.sha_file(source_root/p)==sha for p,sha in closure.items()),'runner changed during collection')
        save(a.work/'summary.json',dict(complete=True,records=len(rows),queries=sum(r['q'] for r in rows),cells=summarize(rows)))
        report.update(complete=True,queries=sum(r['q'] for r in rows))
    except BaseException as error:
        report['failure']=dict(type=type(error).__name__,message=str(error));raise
    finally:
        admin.close();save(a.work/'report.json',report)


if __name__=='__main__':main()
