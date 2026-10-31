"""Resource-bounded paired queries on three existing, independently bound cases.

The supervisor never enters the measured cgroup. The worker, daemon and native
worker share a 2 GiB leaf; the existing DataNode has its own leaf in the SAME
two-CPU partition. Existing service control owns WAL transitions/restoration.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
import uuid

from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json, cpus

WORK = Path('experiments/cross_version/work')
BINARIES = {
    'baseline': ('hdf5-fee-batch-opt-20261020', 'bff1dd7a6fcc42af0f95f529010a04583e2618df460ef95c473a6ed3bbd77467',
                 '372d41672596f0169458b151abe9c7aeb3425d63819b3756338a1ea620360f57'),
    'candidate': ('grib-global-index-20261020', '36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9',
                  'cc545bfd2d5698ddc59357825fb5c0f37944ea68922e73c09f03dec33eea10c1'),
}
DIAGNOSTIC = ('IOTDBFS_EXPERIMENT_EXCLUSIVE_TIMING', 'IOTDBFS_EXPERIMENT_COST_SIDECAR',
              'IOTDBFS_EXPERIMENT_FROZEN_TYPED')
INSPECTOR_SHA = 'f8a3792ddfde12fd06c7e94108c11465786f4ad48e20603c57800dc6b1ada7a7'


def need(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def append(path, value):
    with Path(path).open('a') as stream:
        os.chmod(path, 0o600)
        stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')
        stream.flush(); os.fsync(stream.fileno())


def source_closure(repo):
    paths = sorted((repo/'experiments/identity').glob('*.py'))
    paths += sorted((repo/'experiments/cross_version/resource_limited').glob('*.py'))
    for folder in ('pilot','hdf5_route3','version_matrix'):
        paths += sorted((repo/'experiments/cross_version'/folder).glob('*.py'))
    for name in ('v00_head_probe','v03_n128','v03_startup_wrapper','v04_n128'):
        paths.append(repo/'experiments/cross_version/version_directory'/(name+'.py'))
    return {str(p.relative_to(repo)): file_sha(p) for p in paths}


def verify_binaries(repo):
    result = {}
    for variant, (folder, sha, binding_sha) in BINARIES.items():
        root = repo/WORK/folder
        binary, binding = root/'candidate-clean-iotdb_fs', root/'candidate-clean-binding.json'
        need(file_sha(binary) == sha and file_sha(binding) == binding_sha, 'accepted binary/build binding changed')
        doc = read_json(binding)
        need(doc.get('complete') is True and doc.get('full_clean_build') is True and
             doc.get('compiled_cpp_units', 0) >= 80 and doc['binary_sha256'] == sha and
             Path(doc['binary']).resolve() == binary.resolve(), 'accepted clean build differs')
        source = root/'source'
        for relative, expected in doc['source_files'].items():
            path = source/relative
            need(path.resolve().is_relative_to(source.resolve()) and file_sha(path) == expected,
                 'accepted build source changed: '+relative)
        result[variant] = dict(path=str(binary), sha256=sha, binding=str(binding),
                               binding_sha256=binding_sha, source_files_sha256=digest(doc['source_files']))
    return result


def journal_snapshot(root):
    root = Path(root)
    paths = sorted(root.iterdir())
    need({p.name for p in paths} == {'state', 'owner.lock'} and
         all(p.is_file() and not p.is_symlink() for p in paths), 'owner journal pending/unsafe or incomplete')
    return {p.name: file_sha(p) for p in paths}


def launcher(path, binary, expected_sha, prefix, journal):
    """Only the current public engine setting; no obsolete catalog switch."""
    need(file_sha(binary) == expected_sha, 'daemon SHA differs')
    settings = {'storage.version_directory.mode': 'owner',
                'storage.version_directory.table_prefix': prefix,
                'storage.version_directory.owner_directory': str(journal),
                'storage.version_directory.initialize': 'false',
                'query.hdf5.mode': 'iotdb-indexed'}
    source = '''#!/usr/bin/python3
from pathlib import Path
import hashlib,json,os,sys
sha=lambda path:hashlib.sha256(Path(path).read_bytes()).hexdigest()
if sha(BINARY)!=DIGEST:raise RuntimeError("pinned binary changed")
for key in DIAGNOSTIC:os.environ.pop(key,None)
config=Path(os.environ["IOTDB_CONFIG_FILE"])
with config.open("a") as stream:
    for key,value in SETTINGS.items():stream.write(key+"="+value+"\\n")
config.with_suffix(".launch.json").write_text(json.dumps(dict(actual_binary=BINARY,actual_binary_sha256=DIGEST,settings=SETTINGS,config_sha256=sha(config),pid=os.getpid(),before_query_clock=True),sort_keys=True)+"\\n")
os.execv(BINARY,[BINARY,*sys.argv[1:]])
'''
    definitions = '\n'.join(k+'='+repr(v) for k, v in {
        'BINARY': str(binary), 'DIGEST': expected_sha, 'SETTINGS': settings, 'DIAGNOSTIC': DIAGNOSTIC}.items())+'\n'
    path.write_text(source.split('\n', 1)[0]+'\n'+definitions+source.split('\n', 1)[1]); path.chmod(0o700)
    return dict(path=str(path), sha256=file_sha(path), binary_sha256=expected_sha, settings=settings)


@contextmanager
def database_start_in_leaf(resources_script, leaf, resource_journal):
    """Intercept only ExistingInstance's exact startup command, before JVM exec."""
    from experiments.cross_version.version_directory import v00_head_probe as control_module
    original = control_module.subprocess
    def run(command, *args, **kwargs):
        if isinstance(command, list) and command[:2] == ['bash', 'sbin/start-datanode.sh']:
            need(len(command) == 5 and command[2:4] == ['-d', '-p'], 'unexpected original service start argv')
            command = [sys.executable, str(resources_script), 'enter-leaf-exec', '--leaf', str(leaf),
                       '--journal', str(resource_journal), '--', *command]
        return original.run(command, *args, **kwargs)
    proxy = SimpleNamespace(**{key: getattr(original, key) for key in dir(original)})
    proxy.run = run
    control_module.subprocess = proxy
    try:
        yield
    finally:
        control_module.subprocess = original


def resource_identity(selected):
    cpu = {}
    for number in selected:
        root = Path('/sys/devices/system/cpu')/('cpu%d'%number)/'topology'
        cpu[str(number)] = {name: (root/name).read_text().strip() for name in
                           ('physical_package_id', 'core_id', 'thread_siblings_list')}
    return dict(protocol='exclusive-balanced-shared-two-logical-cpu/2',
        journal_protocol=CgroupV2Resources.JOURNAL_PROTOCOL, cpus=sorted(selected), topology=cpu,
        parent_cpu_max=CgroupV2Resources.CPU_MAX, query_memory_max=int(CgroupV2Resources.QUERY_MEMORY_MAX),
        database_heap={'Xms_MiB':15232, 'Xmx_MiB':15232, 'MaxDirectMemorySize_MiB':2176},
        database_memory_cap='unchanged', partition=CgroupV2Resources.PARTITION,
        exclusive_cpus=sorted(selected), scheduler_load_balancing=True,
        participants=['client','IoTDB-FS','FS-index','original DataNode'],
        supervisor='outside measured group; setup/recovery/observation only',
        cache_reset=False, synthetic_background_load=False,
        native_threads={'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1','MKL_NUM_THREADS':'1','NUMEXPR_NUM_THREADS':'1'},
        kernel=os.uname().release, cpu_model=next(line.split(':',1)[1].strip() for line in
            Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')))


def observe(paths, *, proc_root=Path('/proc')):
    """Outside every query clock; CPU includes legacy oracle/profile harvesting."""
    groups = {}
    for name, raw in paths.items():
        path = Path(raw)
        groups[name] = {field:(path/field).read_text().strip() for field in
            ('cpu.stat','cpu.pressure','memory.current','memory.peak','memory.events','memory.pressure',
             'cpuset.cpus.effective','cpuset.cpus.exclusive.effective','cpuset.cpus.partition','memory.max','cpu.max')
            if (path/field).exists()}
        processes = []
        for pid in (path/'cgroup.procs').read_text().split():
            proc = Path(proc_root)/pid
            try:
                tids = list((proc/'task').iterdir())
                sums = [0,0,0]
                for tid in tids:
                    try:
                        for index, value in enumerate((tid/'schedstat').read_text().split()): sums[index] += int(value)
                    except FileNotFoundError: pass
                status = dict(line.split(':',1) for line in (proc/'status').read_text().splitlines() if ':' in line)
                processes.append(dict(pid=int(pid),threads=len(tids),schedstat=sums,
                    affinity=sorted(os.sched_getaffinity(int(pid))),
                    memory={k:status[k].strip() for k in ('VmRSS','VmHWM') if k in status}))
            except (FileNotFoundError,ProcessLookupError): pass
        groups[name]['processes'] = processes
    need(groups['parent']['cpuset.cpus.partition']==CgroupV2Resources.PARTITION and
         cpus(groups['parent']['cpuset.cpus.effective'])=={2,3} and
         cpus(groups['parent']['cpuset.cpus.exclusive.effective'])=={2,3},
         'lost exclusive load-balanced root partition')
    need(groups['parent']['cpu.max']==CgroupV2Resources.CPU_MAX and groups['parent']['memory.max']=='max' and
         groups['query']['memory.max']==str(2*1024**3) and groups['database']['memory.max']=='max', 'resource limits changed')
    for name in ('query','database'):
        need(groups[name]['cpuset.cpus.partition']=='member' and groups[name]['cpu.max']=='max 100000' and
             not cpus(groups[name]['cpuset.cpus.exclusive.effective']), 'leaf scheduling policy changed')
        need(groups[name]['cpuset.cpus.effective']==groups['parent']['cpuset.cpus.effective'], 'leaf escaped CPU set')
        for proc in groups[name]['processes']:
            need(set(proc['affinity']) <= cpus(groups[name]['cpuset.cpus.effective']),
                 'process affinity escaped partition')
    return dict(protocol=CgroupV2Resources.JOURNAL_PROTOCOL,time_ns=time.time_ns(), groups=groups,
                scope='outer query plus legacy oracle/profile work; not exclusive query CPU time')


def protocol_for(scene, arm):
    hdf = scene['format']=='hdf5'
    return dict(engine=('native-hdf5-SQLite-index' if hdf else 'native-ecCodes-SQLite-index') if arm=='FS-index'
        else ('iotdb-indexed-HDF5' if hdf else 'iotdb-indexed-GRIB'),
        clock={'kind':'owned-full-query','includes':'discovery, selected values, validity mask',
               'excludes':'setup, index build, independent oracle validation, profile harvesting'},
        cache={'kind':'retained process/context; existing prebuilt index; OS/DB not flushed',
               'warmup_pairs':2,'measured_pairs':10,'ROI_or_query_answer_cache':False},
        delivery='NumPy owned Float32/mask including client IPC' if arm!='FS-index' or hdf
                 else 'C++ owned Float32/mask; digest/JSON/pipe excluded; worker wall retained separately')


def prepare(args):
    from experiments.cross_version.resource_limited import scenes
    args.work.mkdir(mode=0o700,parents=True,exist_ok=False)
    registry = Registry(args.registry)
    entries = scenes.prepare(args.repo,registry)
    need(len(entries)==3, 'approved matrix requires exactly three scenes')
    validations = []
    for scene in entries:
        validations.append(scenes.inspect(scene,registry))
        scene['journal_digest'] = journal_snapshot(scene['original_owner'])
    binaries = verify_binaries(args.repo)
    inspector = args.repo/WORK/'large-file-upper-r3-20261017/store-inspector/large_store_probe'
    need(file_sha(inspector)==INSPECTOR_SHA, 'existing head inspector changed')
    prepared = dict(protocol='iotdbfs-resource-ratio-prepared/1', scenes=entries, validations=validations,
                    binaries=binaries, inspector=str(inspector), inspector_sha256=INSPECTOR_SHA,
                    sources=source_closure(args.repo), scientific_publication=False,index_rebuilt=False)
    atomic_json(args.work/'prepared.json',prepared)
    return prepared


def worker(args):
    need(os.read(args.gate_fd,1)==b'1','supervisor did not admit worker')
    os.close(args.gate_fd)
    def interrupt(signum, frame):
        raise RuntimeError('bounded query worker cancelled; unwind its owned mount')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupt)
    from experiments.cross_version.resource_limited import scenes, analysis
    from experiments.cross_version.hdf5_route3 import route3_control
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    prepared = read_json(args.work/'prepared.json')
    need(source_closure(args.repo)==prepared['sources'], 'prepared runner/source closure changed')
    resource = read_json(args.work/'resource-identity.json')
    paths = read_json(args.work/'resource-paths.json')
    observe(paths)
    paired = startup.load_runner(args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner']=paired
    from experiments.cross_version.pilot import publish
    from experiments.cross_version.version_matrix.n8_matrix_prepare import _probe
    _,_,env=publish.runtime(args.repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publish.DATABASE)
    need(env['IOTDB_PORT']=='33667' and env['IOTDB_HOST'] in ('127.0.0.1','localhost'),'original endpoint changed')
    for name in DIAGNOSTIC: env.pop(name,None)
    env.update(resource['native_threads'])
    args.env=env
    args.runtime=args.repo.parent/'runtime'; args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text()); args.report=args.work/'report.json'
    # Verify current actual JVM flags, not just their declared budget.
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(flag in argv for flag in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')),
         'original database fixed heap/direct-memory settings differ')
    startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    registry=Registry(args.registry); runner_sha=digest(prepared['sources'])
    design=dict(scenes=[s['scene_id'] for s in prepared['scenes']],variants=list(BINARIES),
                warmup_repeats=[0,1], measured_repeats=list(range(2,12)),
                arms={'fs':'FS-index','iotdb':'IoTDB-FS'},blocks=[])
    for scene in prepared['scenes']:
        for variant,binary in prepared['binaries'].items():
            block=dict(scene_id=scene['scene_id'],variant=variant,block_id=scene['scene_id']+'-'+variant,
                       corpus_id=scene['corpus_id'],case_id=scene['case_id'],resource_identity=resource,
                       resource_id=digest(resource),arm_protocols={})
            for arm in ('FS-index','IoTDB-FS'):
                protocol=protocol_for(scene,arm)
                run=registry.register_run(build_run(registry.get_case(scene['case_id']),
                    execution=str(args.work)+'/'+block['block_id'],arm=arm,
                    implementation=dict(binary_sha256=binary['sha256'] if arm=='IoTDB-FS' else scene['native_binary_sha256'],
                                        runner_sha256=runner_sha,engine=protocol['engine']),
                    clock=protocol['clock'],cache=protocol['cache'],environment=resource,
                    replication={'block':block['block_id'],'warmup':[0,1],'measured':list(range(2,12)),
                                 'arm_order':'FS first on even repeat, IoTDB first on odd repeat',
                                 'independent_replication':False}))
                block['arm_protocols'][arm]=dict(protocol,run_id=run['run_id'])
            design['blocks'].append(block)
    atomic_json(args.work/'design.json',design)
    report=dict(protocol='iotdbfs-resource-ratio-run/1',complete=False,query_records=0,expected_queries=144,
                inherited_query_rows=0,resource_id=digest(resource),mounts=[],credentials_recorded=False,
                prepared_sha256=file_sha(args.work/'prepared.json'),design_sha256=file_sha(args.work/'design.json'))
    atomic_json(args.report,report);rows=[]
    writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    try:
        for scene in prepared['scenes']:
            for variant,binary in prepared['binaries'].items():
                import shutil
                block=next(b for b in design['blocks'] if b['scene_id']==scene['scene_id'] and b['variant']==variant)
                root=args.work/block['block_id'];root.mkdir(mode=0o700)
                need(journal_snapshot(scene['original_owner'])==scene['journal_digest'],'original journal changed')
                journal=root/'owner';shutil.copytree(scene['original_owner'],journal)
                need(journal_snapshot(journal)==scene['journal_digest'],'journal copy changed')
                for name in ('head-before','head-after'):(root/name).mkdir(mode=0o700)
                before,proof=_probe(Path(prepared['inspector']),'head',scene['prefix'],env,root/'head-before')
                need(before==scene['head'],'scientific head changed before queries')
                launch=launcher(root/'launcher',Path(binary['path']),binary['sha256'],scene['prefix'],journal)
                mount=dict(block_id=block['block_id'],head_before=before,head_before_receipt=proof,launcher=launch)
                report['mounts'].append(mount);atomic_json(args.report,report)
                print(json.dumps(dict(event='mount_start',block=block['block_id'])),flush=True)
                with paired.sf.private_mount(Path(launch['path']),root/'mount',env,writer,profile=True) as context:
                    mount.update(daemon_pid=context['pid'],startup_ns=context['startup_ns'])
                    need(file_sha(Path('/proc')/str(context['pid'])/'exe')==binary['sha256'],'running daemon differs')
                    launch_receipt=read_json(context['root']/'profile.launch.json')
                    need(launch_receipt['settings']==launch['settings'] and launch_receipt['actual_binary_sha256']==binary['sha256'] and
                         launch_receipt['pid']==context['pid'],'actual daemon launcher settings differ')
                    offset=0;native=scenes.open_native(scene,paired,env)
                    try:
                        for repeat in range(12):
                            for arm in (('FS-index','IoTDB-FS') if repeat%2==0 else ('IoTDB-FS','FS-index')):
                                observed_before=observe(paths)
                                if arm=='FS-index':row=scenes.query_native(native,scene)
                                else:row,offset=scenes.query_iotdb(paired,context,scene,offset)
                                observed_after=observe(paths)
                                need(row.get('oracle_verified') is True,'query not verified against existing native oracle')
                                row.update({k:block[k] for k in ('scene_id','variant','block_id','corpus_id','case_id','resource_id','resource_identity')})
                                row.update(block['arm_protocols'][arm]);row.update(arm=arm,repeat=repeat,
                                    phase='warmup' if repeat<2 else 'measurement',ok=True,
                                    resource_observations=dict(before=observed_before,after=observed_after))
                                append(args.work/'queries.jsonl',row);rows.append(row)
                                report['query_records']=len(rows);atomic_json(args.report,report)
                            print(json.dumps(dict(event='pair',block=block['block_id'],repeat=repeat,rows=len(rows))),flush=True)
                    finally:
                        native.close()
                after,proof=_probe(Path(prepared['inspector']),'head',scene['prefix'],env,root/'head-after')
                need(after==before and journal_snapshot(journal)==journal_snapshot(scene['original_owner'])==scene['journal_digest'],
                     'scientific head/journals changed')
                mount.update(complete=True,head_after=after,head_after_receipt=proof,scientific_head_unchanged=True,journals_unchanged=True)
                atomic_json(args.report,report)
        result=analysis.summarize(rows,design);atomic_json(args.work/'analysis.json',result)
        # Full scientific/raw/FS/index checks remain outside query clocks.
        final=[scenes.inspect(scene,registry) for scene in prepared['scenes']]
        atomic_json(args.work/'final-input-verification.json',final)
        need(verify_binaries(args.repo)==prepared['binaries'],'accepted binaries changed during experiment')
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),
                      analysis_sha256=file_sha(args.work/'analysis.json'))
    except BaseException as error:
        report.update(complete=False,failure={'type':type(error).__name__,'message':str(error)[:500]})
        raise
    finally:
        atomic_json(args.report,report)


def finish_query_children(process, manager, work):
    """Before service recovery: unwind the worker, then only its owned survivors."""
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            # This exact Popen query child, never the database/control plane.
            process.kill();process.wait(timeout=10)
    owned=[]
    for raw in Path(manager.paths['query']).joinpath('cgroup.procs').read_text().split():
        pid=int(raw)
        need(not manager.ops.is_database(pid),'database unexpectedly entered query leaf')
        owned.append(manager.ops.identity(pid))
    for path in work.glob('*-*/mount/mount'):
        if os.path.ismount(path):
            subprocess.run(['fusermount','-u',str(path)],stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=30)
    for identity in owned:
        try:
            if manager.ops.identity(identity['pid'])==identity:os.kill(identity['pid'],signal.SIGTERM)
        except ProcessLookupError:pass
    deadline=time.monotonic()+30
    while Path(manager.paths['query']).joinpath('cgroup.procs').read_text().strip() and time.monotonic()<deadline:time.sleep(.1)
    remaining={int(p) for p in Path(manager.paths['query']).joinpath('cgroup.procs').read_text().split()}
    for identity in owned:
        if identity['pid'] in remaining:
            try:
                if manager.ops.identity(identity['pid'])==identity:os.kill(identity['pid'],signal.SIGKILL)
            except ProcessLookupError:pass
    deadline=time.monotonic()+10
    while Path(manager.paths['query']).joinpath('cgroup.procs').read_text().strip() and time.monotonic()<deadline:time.sleep(.1)
    need(not Path(manager.paths['query']).joinpath('cgroup.procs').read_text().strip(),'owned query children still present')


def supervise(args):
    prepared=prepare(args)
    if args.prepare_only:
        print(json.dumps(dict(prepared=True,service_modified=False,cgroup_modified=False,scenes=[s['scene_id'] for s in prepared['scenes']])));return
    need(sys.platform=='linux' and os.geteuid()==0,'original Linux/root host required')
    selected=[int(x) for x in args.cpus.split(',')]
    manager=CgroupV2Resources(selected,args.work/'resources.json',database_pid_file=args.repo.parent/'runtime/run/datanode.pid',keeper_seconds=14400)
    identity=resource_identity(selected)
    atomic_json(args.work/'resource-identity.json',identity);atomic_json(args.work/'resource-paths.json',manager.paths)
    from experiments.cross_version.hdf5_route3 import route3_control
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    paired=startup.load_runner(args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner']=paired
    from experiments.cross_version.pilot import publish
    _,_,args.env=publish.runtime(args.repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publish.DATABASE)
    args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text())
    args.report=args.work/'service-report.json'; service_report={}
    process=None;write_fd=None;old_handlers={};cancelled=False
    def cancel(signum,frame):
        nonlocal cancelled
        cancelled=True
        if process is not None and process.poll() is None:process.send_signal(signal.SIGTERM)
    try:
        for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):old_handlers[sig]=signal.signal(sig,cancel)
        manager.create();atomic_json(args.work/'resource-before.json',manager.snapshot())
        # The control plane owns service recovery even if the bounded worker is OOM-killed.
        with database_start_in_leaf(Path(__file__).with_name('resources.py'),manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service_report):
                try:
                    read_fd,write_fd=os.pipe()
                    worker_command=[sys.executable,'-m',__package__+'.run','--worker','--repo',str(args.repo),'--work',str(args.work),
                                    '--registry',str(args.registry),'--gate-fd',str(read_fd)]
                    # Enter BEFORE the second exec so its allocations are charged to query memory.max.
                    command=[sys.executable,str(Path(__file__).with_name('resources.py')),'enter-leaf-exec',
                             '--leaf',manager.paths['query'],'--journal',str(args.work/'resources.json'),'--',*worker_command]
                    with (args.work/'worker.stdout.log').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        env=dict(os.environ,**identity['native_threads'],PYTHONDONTWRITEBYTECODE='1')
                        process=subprocess.Popen(command,cwd=args.repo,env=env,pass_fds=(read_fd,),stdin=subprocess.DEVNULL,
                                                 stdout=out,stderr=err,start_new_session=True)
                        os.close(read_fd);manager.attach_query(process.pid)
                        if cancelled:process.terminate()
                        else:os.write(write_fd,b'1')
                        os.close(write_fd);write_fd=None
                        start=time.monotonic();deadline=start+10800;cancel_sent=False
                        while process.poll() is None:
                            append(args.work/'resource-samples.jsonl',manager.snapshot())
                            if time.monotonic()>deadline and not cancel_sent:
                                process.send_signal(signal.SIGTERM);cancel_sent=True
                            time.sleep(5)
                        atomic_json(args.work/'worker-exit.json',dict(returncode=process.returncode,cancelled=cancelled,
                                                                    elapsed_seconds=time.monotonic()-start))
                    need(process.returncode==0,'worker failed; public report and private diagnostic retained')
                    need(read_json(args.work/'report.json').get('complete'),'worker scientific matrix incomplete')
                finally:
                    finish_query_children(process,manager,args.work)
    finally:
        if write_fd is not None:os.close(write_fd)
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            # Existing protected service control completes recovery; never SIGKILL a JVM/worker.
            process.wait()
        try:
            try:
                atomic_json(args.work/'resource-after-measurement.json',manager.snapshot())
            except BaseException as error:
                atomic_json(args.work/'resource-final-snapshot-failure.json',{'type':type(error).__name__,'message':str(error)[:300]})
            finally:
                manager.close()
                atomic_json(args.work/'resource-restored.json',manager.state)
        finally:
            for sig,handler in old_handlers.items():signal.signal(sig,handler)
            path=args.work/'report.json'
            report=read_json(path) if path.exists() else {'complete':False,'query_records':0}
            scientific_complete=report.get('complete') is True
            report.update(service_report,resources_restored=manager.state['phase']=='closed')
            report.update(scientific_complete=scientific_complete,complete=bool(scientific_complete and
                report.get('original_service_restored') and report.get('controller_restoration_verified') and
                report['resources_restored']))
            atomic_json(path,report)
    print(json.dumps(dict(complete=True,report=str(args.work/'report.json'),resources_restored=manager.state['phase']=='closed')))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--cpus',default='2,3')
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--gate-fd',type=int,help=argparse.SUPPRESS)
    args=parser.parse_args();args.repo=args.repo.resolve(strict=True);args.work=args.work.resolve();args.registry=args.registry.resolve()
    if args.worker:worker(args)
    else:supervise(args)


if __name__=='__main__':main()
