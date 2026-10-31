"""Original-service control with deferred cancellation during transitions.

The frozen legacy control remains unchanged. A SIGTERM during its stop wait
could previously leave changed=False and mistake a still-stopping PID for a
restored service. This experiment owns the full stop/start transition and checks
effective settings before reporting restoration.
"""
from contextlib import contextmanager
from pathlib import Path
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

from experiments.cross_version.version_directory.v00_head_probe import ExistingInstance


def require(value,message):
    if not value:raise RuntimeError(message)


class CancellationGuard:
    def __init__(self):self.depth=0;self.requested=False

    def interrupt(self,signum,frame):
        self.requested=True
        if not self.depth:raise RuntimeError('route3 canceled; restore original service')

    @contextmanager
    def critical(self):
        self.depth+=1
        try:yield
        finally:self.depth-=1

    def checkpoint(self):
        if self.requested:raise RuntimeError('route3 canceled after safe service transition')


class ProtectedExistingInstance(ExistingInstance):
    def __init__(self,*args,**kwargs):
        self.stop_requested=False
        super().__init__(*args,**kwargs)

    def stop(self,crash=False):
        if self.live():self.stop_requested=True
        return super().stop(crash=crash)

    def restore(self):
        self.adopt_started()
        if self.stop_requested and not self.changed:
            # Finish an in-flight original stop, even if enable_sync failed
            # before it could set changed=True. live() alone is not readiness.
            self.stop()
        return super().restore()


def verify_effective(args,instance,expected):
    require(instance.live(),'restored DataNode is not live')
    require(int(instance.pid_file.read_text())==instance.pid,'restored PID file differs')
    require(instance.process_identity(instance.pid)==instance.identity,'restored DataNode identity differs')
    instance.check_protected()
    worker=args.repo/'experiments/cross_version/work/v00-head-effective-20261015-r3/v00_head_worker'
    digest=hashlib.sha256(worker.read_bytes()).hexdigest()
    require(digest=='2575ba1f3c5b151a00521bd94cc90fdc30e1f91d1fdcd0490a80f985102e8221','settings worker changed')
    env=dict(args.env)
    env.update(IOTDB_V00_PROBE_PASSWORD=env['IOTDB_PASS'],IOTDB_V00_PROBE_DATABASE='v00_head_0000000000000000')
    log=args.work/'control'/('settings-'+str(time.time_ns())+'.private.stderr')
    with log.open('x') as errors:
        result=subprocess.run([str(worker),'33667'],input='settings\n',text=True,env=env,
            stdout=subprocess.PIPE,stderr=errors,timeout=45)
    mode,partial=expected
    require(result.returncode==0 and result.stdout.splitlines()==['READY',f'CONFIG\t{mode}\t{partial}'],
        'original effective settings were not verified; private worker log retained')
    return {'pid':instance.pid,'config_sha256':hashlib.sha256(instance.config.read_bytes()).hexdigest(),
        'effective_settings':mode+'/'+partial,'protected_hashes_verified':True,
        'operation':'SHOW ALL CONFIGURATION only','settings_worker_sha256':digest}


@contextmanager
def sync_window(args,report,*,instance_factory=ProtectedExistingInstance,verifier=verify_effective):
    require(sys.platform=='linux' and os.geteuid()==0,'original-service control requires Linux/root')
    require(args.release==(args.runtime/'current').resolve(),'original release differs')
    control=args.work/'control';control.mkdir(mode=0o700)
    def event(kind,**fields):
        with (control/'operations.jsonl').open('a') as stream:
            stream.write(json.dumps({'event':kind,'time_ns':time.time_ns(),**fields},sort_keys=True)+'\n')
            stream.flush();os.fsync(stream.fileno())
        # The fsynced operation journal is authoritative. A disconnected SSH
        # progress stream must not prevent the next stop/start or rollback.
        try:print(json.dumps({'event':kind}),flush=True)
        except BrokenPipeError:pass
    def checkpoint():
        from experiments.cross_version.hdf5_route3.route3_data import save
        save(args.report,report)
    lock=(args.runtime/'run/v00-head-experiment.lock').open('a+b')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    old_handlers={};guard=CancellationGuard();instance=None
    report['original_service_restored']=False
    try:
        for sig in (signal.SIGTERM,signal.SIGHUP):old_handlers[sig]=signal.signal(sig,guard.interrupt)
        with guard.critical():
            instance=instance_factory(SimpleNamespace(runtime=args.runtime,release=args.release,
                work=control,datanode_pid=args.datanode_pid),event)
            instance.enable_sync()
            proof=verifier(args,instance,('SYNC','false'))
            event('route3_effective_sync_verified',**proof)
            report['sync_datanode_pid']=instance.pid;checkpoint()
        guard.checkpoint()
        yield
    finally:
        try:
            if instance is not None:
                # Keep repeated cancellation pending through restore AND the
                # actual settings check, not merely until stop/start returns.
                with guard.critical():
                    instance.restore()
                    proof=verifier(args,instance,('ASYNC','true'))
                    require(instance.config.read_bytes()==instance.original,'original config bytes differ')
                    event('route3_service_restored_verified',**proof)
                    report.update(original_service_restored=True,controller_restoration_verified=True,
                        final_datanode_pid=instance.pid,restored_config_sha256=proof['config_sha256'],
                        effective_settings=proof['effective_settings'],cancel_requested=guard.requested)
                    checkpoint()
        except BaseException as error:
            report.update(original_service_restored=False,restoration_failure={'type':type(error).__name__})
            checkpoint()
            raise
        finally:
            for sig,handler in old_handlers.items():signal.signal(sig,handler)
            lock.close();checkpoint()
