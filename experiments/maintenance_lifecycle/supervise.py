"""Shared two-CPU M1 window with unchanged live DataNode and exact restoration."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--pid-file',type=Path,required=True)
    a,worker=p.parse_known_args();a.work.mkdir(parents=True,exist_ok=False)
    if not worker or worker[0]!='--':raise RuntimeError('explicit worker arguments after -- required')
    manager=CgroupV2Resources([2,3],a.work/'resources.json',database_pid_file=a.pid_file,keeper_seconds=14400)
    proc=None;cancelled=False;report=dict(complete=False,worker_returncode=None,resources_restored=False)
    def cancel(sig,frame):
        nonlocal cancelled
        cancelled=True
        if proc and proc.poll() is None:proc.terminate()
    for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGHUP):signal.signal(sig,cancel)
    try:
        manager.create()
        (Path(manager.paths['query'])/'memory.swap.max').write_text('0\n')
        atomic_json(a.work/'resource-before.json',manager.snapshot())
        command=[sys.executable,'-m','experiments.cross_version.resource_limited.resources','enter-leaf-exec',
                 '--leaf',manager.paths['query'],'--journal',str(a.work/'resources.json'),'--',sys.executable,'-u',
                 '-m','experiments.maintenance_lifecycle.run','--work',str(a.work/'collection'),*worker[1:]]
        with (a.work/'worker.stdout.log').open('x') as out,(a.work/'worker.private.stderr').open('x') as err:
            proc=subprocess.Popen(command,stdout=out,stderr=err,stdin=subprocess.DEVNULL)
            start=time.monotonic();term_at=None
            with (a.work/'resource-samples.jsonl').open('x') as samples:
                while proc.poll() is None:
                    snap=manager.snapshot();samples.write(json.dumps(snap)+'\n');samples.flush()
                    if (cancelled or time.monotonic()-start>10800) and term_at is None:
                        proc.terminate();term_at=time.monotonic()
                    if term_at is not None and time.monotonic()-term_at>120:
                        proc.kill()
                    time.sleep(5)
            report.update(worker_returncode=proc.returncode,cancelled=cancelled,elapsed_s=time.monotonic()-start)
        atomic_json(a.work/'resource-after.json',manager.snapshot())
    finally:
        # Stop only this worker and descendants in the owned query leaf, never the DB leaf.
        if proc and proc.poll() is None:
            proc.terminate()
            try:proc.wait(timeout=120)
            except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=10)
        leaf=Path(manager.paths['query'])
        if leaf.exists():
            # The worker normally closes both mounts and the admin helper itself.
            # Exceptional cleanup is restricted to this freshly created cgroup.
            for pid in [int(x) for x in (leaf/'cgroup.procs').read_text().split()]:
                try:os.kill(pid,signal.SIGTERM)
                except ProcessLookupError:pass
            until=time.monotonic()+30
            while (leaf/'cgroup.procs').read_text().strip() and time.monotonic()<until:time.sleep(.1)
            if (leaf/'cgroup.procs').read_text().strip():
                for pid in [int(x) for x in (leaf/'cgroup.procs').read_text().split()]:
                    try:os.kill(pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                time.sleep(.2)
        for mount in (a.work/'collection').glob('*/mount/mount'):
            if os.path.ismount(mount):subprocess.run(['fusermount','-uz',str(mount)],check=True,timeout=30)
        for mount in (a.work/'collection').glob('*/reopen-mount/mount'):
            if os.path.ismount(mount):subprocess.run(['fusermount','-uz',str(mount)],check=True,timeout=30)
        try:
            manager.close();report['resources_restored']=manager.state['phase']=='closed'
            atomic_json(a.work/'resources-restored.json',manager.state)
        finally:
            report['complete']=bool(report['worker_returncode']==0 and report['resources_restored'])
            atomic_json(a.work/'supervisor-report.json',report)
    if not report['complete']:raise RuntimeError('M1 worker or restoration incomplete; all evidence retained')

if __name__=='__main__':main()
