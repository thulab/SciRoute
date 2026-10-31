"""Section 4.2 experiment controller; original service and cgroups restore on exit.

The supervisor stays outside measured leaves. Its worker owns only private
mounts and the new section42 namespace; existing scientific sources are read-only.
"""
from pathlib import Path
import argparse, hashlib, inspect, json, os, shutil, signal, subprocess, sys, time
from types import SimpleNamespace
from experiments.fixed_matrix import run, telemetry
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.identity.registry import file_sha, read_json
from experiments.single_file.section42_20261026 import driver

REPO=Path('/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C')
STAGE=REPO/'experiments/cross_version/work/section42-20261026'
PRODUCT=REPO/'experiments/cross_version/work/native-read-20261026/product-r1/build/bin/iotdb_fs'
SHA='64672d18371ce3c734e836d53f3a96dde00cd976b9e0ce6c23e998713a715357'
MODULE='experiments.single_file.section42_20261026.orchestrator'

def closure(repo):
    result=run.closure(repo)
    for folder in ('experiments/single_file','tests/benchmarks/hdf5_query','tests/benchmarks/arrow_training'):
        for p in (repo/folder).rglob('*.py'):
            if any(x in p.parts for x in ('work','results','__pycache__')):continue
            result[str(p.relative_to(repo))]=file_sha(p)
    return result

def preflight(args):
    assert file_sha(PRODUCT)==SHA,'pinned product changed'
    proof=driver.preflight(REPO,args.stage)
    proof.update(product=str(PRODUCT),product_sha256=SHA,sources=closure(REPO),
        resources=run.infrastructure.resource_identity([2,3]),
        controller_limits=dict(worker_seconds=12600),
        protocol='iotdbfs-section42-20261026/1')
    return proof

def worker(args):
    def stop(sig,frame):raise RuntimeError('section42 worker interrupted; unwind private mount')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,stop)
    proof=read_json(args.work/'preflight.json')
    assert proof==preflight(args),'execution source closure changed'
    paired,_,env,startup=driver.dependencies(REPO)
    startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    paths=read_json(args.work/'resource-paths.json')
    report=dict(complete=False,product_sha256=SHA,query_records=0)
    atomic_json(args.work/'report.json',report)
    try:
        driver.collect(REPO,args.work,proof,env,paired,paths,PRODUCT,report)
        assert preflight(args)==proof,'immutable scientific inputs changed'
        report['complete']=True
    except BaseException as e:
        report['failure']=dict(type=type(e).__name__,message=str(e)[:800])
        raise
    finally:atomic_json(args.work/'report.json',report)

def supervise(args):
    proof=preflight(args)
    args.work.mkdir(mode=0o700,parents=True,exist_ok=False)
    atomic_json(args.work/'preflight.json',proof)
    for name,sha in proof['sources'].items():
        p=args.work/'source'/name;p.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(REPO/name,p);assert file_sha(p)==sha
    args.repo=REPO;args.binary=PRODUCT;args.baseline_binary=PRODUCT
    args.library=REPO/'experiments/cross_version/work/fixed-matrix-20261021/native-build/libfixed_native.so'
    args.inspector=REPO/'experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe'
    # Retain the shared bootstrap exactly, changing only its obsolete PID-based
    # connection source; this wrapper itself is included in the source closure.
    source=inspect.getsource(run.bootstrap)
    old='from experiments.fixed_matrix.run import dependencies'
    assert source.count(old)==1,'bootstrap dependency import changed'
    namespace=dict(run.__dict__)
    exec(compile(source.replace(old,'from experiments.single_file.section42_20261026.driver import dependencies'),
        __file__+':post-reboot-bootstrap','exec'),namespace)
    namespace['bootstrap'](args)
    _,_,args.env,_=driver.dependencies(REPO)
    args.runtime=REPO.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text());args.report=args.work/'service-report.json'
    software=run.software_inventory(REPO);atomic_json(args.work/'software.json',software)
    atomic_json(args.work/'environment.json',telemetry.environment(REPO,artifact_paths=[PRODUCT,args.library,args.inspector],
        process_roles=software['process_roles'],data_path=args.work))
    atomic_json(args.work/'support-roles.json',{k:v for k,v in software['process_roles'].items() if k!='datanode'})
    worker_seconds=proof['controller_limits']['worker_seconds']
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=worker_seconds+1800)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    process=sampler=None;service={};failure=None;handlers={}
    def cancel(sig,frame):
        if process is not None and process.poll() is None:process.terminate()
        raise RuntimeError('section42 controller cancelled; restore service/resources')
    for sig in (signal.SIGTERM,signal.SIGHUP):handlers[sig]=signal.signal(sig,cancel)
    handlers[signal.SIGINT]=signal.signal(signal.SIGINT,lambda sig,frame:os.kill(os.getpid(),signal.SIGTERM))
    try:
        manager.create();atomic_json(args.work/'resource-before.json',manager.snapshot())
        with (args.work/'sampler.private.stderr').open('x') as err:
            sampler=subprocess.Popen([sys.executable,'-m','experiments.fixed_matrix.telemetry','--paths',str(args.work/'resource-paths.json'),
                '--roles',str(args.work/'support-roles.json'),'--output',str(args.work/'resource-samples.jsonl'),
                '--stop-file',str(args.work/'sampler.stop'),'--interval','1','--max-seconds',str(worker_seconds+1400)],
                cwd=REPO,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=err)
        resources_script=Path(run.infrastructure.__file__).with_name('resources.py')
        with run.infrastructure.database_start_in_leaf(resources_script,manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service):
                try:
                    command=[sys.executable,str(resources_script),'enter-leaf-exec','--leaf',manager.paths['query'],
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m',MODULE,'worker','--work',str(args.work),'--stage',str(args.stage)]
                    with (args.work/'worker.stdout').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=REPO,env=run.worker_environment(),stdin=subprocess.DEVNULL,stdout=out,stderr=err,start_new_session=True)
                        manager.attach_query(process.pid);started=time.monotonic()
                        while process.poll() is None:
                            assert sampler.poll() is None,'resource sampler stopped'
                            run.infrastructure.observe(manager.paths)
                            assert time.monotonic()-started<worker_seconds,'section42 worker timeout'
                            time.sleep(5)
                    assert process.returncode==0,'worker failed; preserve all failed data'
                finally:run.infrastructure.finish_query_children(process,manager,args.work)
    except BaseException as e:
        failure=dict(type=type(e).__name__,message=str(e)[:800]);raise
    finally:
        try:manager.close();atomic_json(args.work/'resources-restored.json',manager.state)
        finally:
            (args.work/'sampler.stop').touch()
            if sampler is not None:
                try:sampler.wait(timeout=10)
                except subprocess.TimeoutExpired:sampler.terminate();sampler.wait(timeout=10)
            report=read_json(args.work/'report.json') if (args.work/'report.json').exists() else dict(complete=False)
            science=report.get('complete') is True
            report.update(service,scientific_complete=science,resources_restored=manager.state.get('restoration_complete',False))
            if failure:report['controller_failure']=failure
            report['ready_for_analysis']=bool(science and not failure and sampler is not None and sampler.returncode==0 and
                report.get('original_service_restored') and report.get('controller_restoration_verified') and report['resources_restored'])
            report['complete']=False;atomic_json(args.work/'report.json',report)
            for sig,handler in handlers.items():signal.signal(sig,handler)
    assert report['ready_for_analysis'],'science/resources/service incomplete'
    analysis=driver.analyze(args.work)
    atomic_json(args.work/'analysis.json',analysis)
    report.update(complete=True,analysis_sha256=file_sha(args.work/'analysis.json'))
    atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=True,report=str(args.work/'report.json'))))

def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['preflight','launch','worker']);p.add_argument('--work',type=Path,required=True);p.add_argument('--stage',default='all')
    args=p.parse_args()
    if args.mode=='worker':worker(args)
    elif args.mode=='launch':supervise(args)
    else:
        proof=preflight(args);args.work.mkdir(parents=True,exist_ok=False)
        atomic_json(args.work/'preflight.json',proof)
        print(json.dumps(dict(complete=True,preflight=str(args.work/'preflight.json'),service_modified=False)))
if __name__=='__main__':main()
