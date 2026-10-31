"""Fail-closed cgroup-v2 resources for the approved two-CPU ratio experiment.

The supervisor stays outside these groups. Query children enter query before
exec; DataNode startup must enter database BEFORE allocating its existing heap.
No service operations, process killing, or global affinity changes occur here.
Kernel interface reference: https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid


class ResourceError(RuntimeError):
    pass


def require(ok, reason):
    if not ok:
        raise ResourceError(reason)


def cpus(value):
    result = set()
    for part in str(value).strip().split(','):
        if not part:
            continue
        require(bool(re.fullmatch(r'\d+(?:-\d+)?', part)), 'invalid CPU/node list')
        bounds = list(map(int, part.split('-')))
        lo, hi = bounds[0], bounds[-1]
        require(0 <= lo <= hi <= 1048575, 'invalid CPU/node range')
        result.update(range(lo, hi + 1))
    return result


def cpu_text(values):
    return ','.join(map(str, sorted(values)))


def atomic_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.pending-' + uuid.uuid4().hex)
    try:
        with temporary.open('x') as stream:
            os.chmod(temporary, 0o600)
            json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        if temporary.exists(): temporary.unlink()


class LinuxOps:
    """OS seam; tests replace it with a synthetic filesystem/process model."""
    platform = sys.platform
    euid = staticmethod(os.geteuid)
    manager_pid = staticmethod(os.getpid)
    read = staticmethod(lambda path: Path(path).read_text().strip())
    write = staticmethod(lambda path, value: Path(path).write_text(str(value) + '\n'))
    exists = staticmethod(lambda path: Path(path).exists())
    mkdir = staticmethod(lambda path: Path(path).mkdir())
    rmdir = staticmethod(lambda path: Path(path).rmdir())
    affinity = staticmethod(lambda tid: set(os.sched_getaffinity(tid)))
    set_affinity = staticmethod(lambda tid, values: os.sched_setaffinity(tid, values))

    def __init__(self, proc_root):
        self.proc = Path(proc_root)
        self.keepers = {}

    def is_cgroup2_mount(self, root):
        # A mount root is required: no delegated/non-root topology shortcuts.
        for line in (self.proc / 'self/mountinfo').read_text().splitlines():
            before, sep, after = line.partition(' - ')
            if sep and after.split()[0] == 'cgroup2':
                fields = before.split()
                if fields[3] == '/' and Path(fields[4]) == Path(root): return True
        return False

    def groups(self, root):
        return sorted(p.parent for p in Path(root).rglob('cgroup.procs'))

    def task_ids(self, pid):
        return sorted(int(p.name) for p in (self.proc / str(pid) / 'task').iterdir() if p.name.isdigit())

    def identity(self, pid):
        stat = (self.proc / str(pid) / 'stat').read_text()
        # Field 22 after a comm that may contain spaces/parentheses.
        return {'pid': int(pid), 'start_ticks': int(stat[stat.rfind(')') + 2:].split()[19])}

    def cgroup(self, pid):
        entries = (self.proc / str(pid) / 'cgroup').read_text().splitlines()
        unified = [line[3:] for line in entries if line.startswith('0::')]
        require(len(unified) == 1 and unified[0].startswith('/'), 'unified cgroup membership required')
        return unified[0]

    def is_database(self, pid):
        argv = (self.proc / str(pid) / 'cmdline').read_bytes().split(b'\0')
        return any(arg == b'com.timecho.iotdb.DataNode' for arg in argv)

    def spawn_keeper(self, seconds):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(%d)' % seconds],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        self.keepers[child.pid] = child
        return child.pid

    def stop_keeper(self, identity):
        child = self.keepers.get(identity['pid'])
        require(child is not None, 'keeper is not owned by this supervisor')
        if child.poll() is None:
            require(self.identity(child.pid) == identity, 'keeper PID identity changed')
            child.terminate()
            try: child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait(timeout=5)  # Only this exact owned sleep child.
        self.keepers.pop(identity['pid'], None)


class CgroupV2Resources:
    JOURNAL_PROTOCOL = 'iotdbfs-cgroup-ratio/2'
    PARTITION = 'root'
    CPU_MAX = '200000 100000'
    QUERY_MEMORY_MAX = str(2 * 1024**3)
    CONTROLLERS = {'cpuset', 'cpu', 'memory'}
    PREFIX = 'iotdbfs-ratio-'

    def __init__(self, selected_cpus, journal_path, *, cgroup_root='/sys/fs/cgroup',
                 proc_root='/proc', cpu_root='/sys/devices/system/cpu',
                 database_pid_file=None, keeper_seconds=7200, ops=None):
        self.selected = cpus(selected_cpus) if isinstance(selected_cpus, str) else set(selected_cpus)
        require(len(self.selected) == 2 and all(type(c) is int and c >= 0 for c in self.selected),
                'exactly two explicit logical CPUs required')
        require(type(keeper_seconds) is int and 60 <= keeper_seconds <= 14400, 'bounded keeper duration required')
        self.root = Path(cgroup_root).resolve(); self.proc = Path(proc_root)
        self.cpu_root = Path(cpu_root); self.ops = ops or LinuxOps(self.proc)
        self.journal = Path(journal_path).resolve()
        require(not self.journal.is_relative_to(self.root), 'cleanup journal must be outside experimental cgroups')
        require(not self.journal.exists(), 'fresh cleanup journal required')
        self.events = self.journal.with_suffix(self.journal.suffix + '.events.jsonl')
        self.name = self.PREFIX + uuid.uuid4().hex
        self.parent = self.root / self.name
        self.query = self.parent / 'query'; self.database = self.parent / 'database'
        self.paths = {k: str(v) for k, v in [('parent', self.parent), ('query', self.query), ('database', self.database)]}
        self.pid_file = Path(database_pid_file).resolve() if database_pid_file else None
        self.keeper_seconds = keeper_seconds
        self.state = {'protocol': self.JOURNAL_PROTOCOL, 'partition': self.PARTITION,
            'scheduler_load_balancing': True, 'name': self.name,
            'paths': self.paths, 'cgroup_root': str(self.root), 'cpus': sorted(self.selected),
            'manager_identity': self.ops.identity(self.ops.manager_pid()),
            'database_pid_file': str(self.pid_file) if self.pid_file else None,
            'phase': 'new', 'created_groups': [], 'added_root_controllers': [],
            'database_original': None, 'database_restored': False, 'keeper': None,
            'query_processes': [], 'database_processes': [], 'restoration_complete': False}

    def _event(self, event, **fields):
        self.events.parent.mkdir(parents=True, exist_ok=True)
        with self.events.open('a') as stream:
            os.chmod(self.events, 0o600)
            stream.write(json.dumps({'time_ns': time.time_ns(), 'event': event, **fields}, sort_keys=True) + '\n')
            stream.flush(); os.fsync(stream.fileno())

    def _save(self, event, **fields):
        atomic_json(self.journal, self.state); self._event(event, **fields)

    def _read(self, path): return self.ops.read(path)
    def _write(self, path, value): self.ops.write(path, value)
    def _membership_path(self, membership):
        require(membership.startswith('/') and '..' not in membership.split('/'), 'unsafe cgroup membership')
        path = self.root / membership.lstrip('/')
        require(path.is_relative_to(self.root), 'cgroup escaped unified root')
        return path

    def _foreign_states(self):
        result = {}
        for group in self.ops.groups(self.root):
            if group == self.root or group.is_relative_to(self.parent): continue
            result[str(group)] = {f: self._read(group / f) for f in (
                'cgroup.type', 'cgroup.subtree_control', 'cpuset.cpus.partition',
                'cpuset.cpus.exclusive', 'cpuset.cpus.exclusive.effective', 'cpu.max', 'memory.max')
                if self.ops.exists(group / f)}
        return result

    def inspect(self):
        require(self.ops.platform == 'linux' and self.ops.euid() == 0, 'Linux/root required; no taskset fallback')
        require(self.ops.is_cgroup2_mount(self.root), 'unified cgroup-v2 mount root required')
        # cgroup.type is a non-root interface; the verified mount root is exempt.
        # Every newly created non-root parent/leaf is checked as a domain below.
        available = set(self._read(self.root / 'cgroup.controllers').split())
        require(self.CONTROLLERS <= available, 'cpuset/cpu/memory controllers unavailable')
        effective = cpus(self._read(self.root / 'cpuset.cpus.effective'))
        online = cpus(self._read(self.cpu_root / 'online'))
        require(self.selected <= effective & online and bool(effective - self.selected),
                'two online CPUs and at least one remaining host CPU required')
        mems = self._read(self.root / 'cpuset.mems.effective')
        require(bool(cpus(mems)), 'effective memory nodes unavailable')
        require(self.ops.exists(self.root / 'cpuset.cpus.isolated'), 'isolated partition verification unavailable')
        require(not (self.selected & cpus(self._read(self.root / 'cpuset.cpus.isolated'))),
                'requested CPUs overlap an existing isolated partition')
        foreign = self._foreign_states()
        for group, files in foreign.items():
            partition = files.get('cpuset.cpus.partition', 'member')
            require('invalid' not in partition, 'existing invalid cpuset partition: ' + group)
            reserved = cpus(files.get('cpuset.cpus.exclusive', '')) | cpus(files.get('cpuset.cpus.exclusive.effective', ''))
            require(not self.selected & reserved, 'requested CPUs overlap existing exclusive reservation: ' + group)
        manager_group = self._membership_path(self.ops.cgroup(self.ops.manager_pid()))
        require(not manager_group.is_relative_to(self.parent), 'supervisor must remain outside experimental groups')
        return {'controllers': sorted(available), 'enabled': self._read(self.root / 'cgroup.subtree_control').split(),
            'effective_cpus': sorted(effective), 'selected_cpus': sorted(self.selected),
            'mems': mems, 'foreign_states': foreign, 'manager_cgroup': str(manager_group)}

    def create(self):
        require(self.state['phase'] == 'new', 'resource groups already started')
        before = self.inspect(); self.state['before'] = before
        require(not self.ops.exists(self.parent), 'fresh UUID cgroup path required')
        require(self.pid_file is not None, 'trusted runtime DataNode pidfile required before isolation')
        text = self.pid_file.read_text().strip()
        require(text.isdigit() and int(text) > 0, 'invalid initial DataNode pidfile')
        initial_pid = int(text)
        # Capture BEFORE partition creation changes effective affinity in its old group.
        self._capture_database(initial_pid)
        self.state['added_root_controllers'] = sorted(self.CONTROLLERS - set(before['enabled']))
        self.state['phase'] = 'creating'; self._save('create_planned')
        if self.state['added_root_controllers']:
            self._write(self.root / 'cgroup.subtree_control', ' '.join('+' + c for c in self.state['added_root_controllers']))
        require(self.CONTROLLERS <= set(self._read(self.root/'cgroup.subtree_control').split()),
                'root controllers did not enable')
        self.state['enabled_foreign_states'] = self._foreign_states()
        self._save('root_controllers_verified')
        for path in (self.parent, self.query, self.database):
            self.state['pending_mkdir'] = str(path); self._save('mkdir_planned', path=str(path))
            self.ops.mkdir(path)
            self.state['created_groups'].append(str(path)); self.state.pop('pending_mkdir', None)
            self._save('mkdir_completed', path=str(path))
            if path == self.parent:
                require(self._read(path / 'cgroup.type') == 'domain' and not self._read(path / 'cgroup.procs'),
                        'parent must be a domain without internal processes')
                self._write(path / 'cpuset.mems', before['mems'])
                self._write(path / 'cpuset.cpus', cpu_text(self.selected))
                require(self.ops.exists(path / 'cpuset.cpus.exclusive'), 'exclusive cpuset interface required')
                self._write(path / 'cpuset.cpus.exclusive', cpu_text(self.selected))
                self._write(path / 'cpuset.cpus.partition', self.PARTITION)
                self._write(path / 'cpu.max', self.CPU_MAX)
                self._write(path / 'memory.max', 'max')
                self._write(path / 'cgroup.subtree_control', '+cpuset +cpu +memory')
            else:
                self._write(path / 'cpuset.mems', before['mems'])
                self._write(path / 'cpuset.cpus', cpu_text(self.selected))
                self._write(path / 'memory.max', self.QUERY_MEMORY_MAX if path == self.query else 'max')
                self._write(path / 'cpu.max', 'max 100000')
        self.state['phase'] = 'active'; self._assert_active(); self._save('created')
        self.attach_database(initial_pid)
        return self.snapshot()

    def _assert_active(self):
        require(self.state['phase'] == 'active', 'measurement resource window is not active')
        require(self.state.get('protocol') == self.JOURNAL_PROTOCOL and
                self.state.get('partition') == self.PARTITION and
                self.state.get('scheduler_load_balancing') is True, 'resource journal policy changed')
        assert_balanced_policy(self.ops, self.parent, self.selected)
        require(not self._membership_path(self.ops.cgroup(self.ops.manager_pid())).is_relative_to(self.parent),
                'supervisor entered bounded cgroup')

    def _set_process_affinity(self, pid, values, originals=None):
        identity = self.ops.identity(pid)
        for _ in range(8):
            tids = self.ops.task_ids(pid)
            for tid in tids:
                try: self.ops.set_affinity(tid, set((originals or {}).get(str(tid), values)))
                except ProcessLookupError: pass
            if self.ops.identity(pid) != identity: raise ResourceError('process PID identity changed')
            current = self.ops.task_ids(pid)
            try:
                if all(self.ops.affinity(tid) == set((originals or {}).get(str(tid), values)) for tid in current): return
            except ProcessLookupError: continue
        raise ResourceError('could not verify all live thread affinities')

    def _move(self, pid, destination):
        identity = self.ops.identity(pid)
        self._event('move_planned', identity=identity, destination=str(destination))
        self._write(destination / 'cgroup.procs', str(pid))
        require(self.ops.identity(pid) == identity and self._membership_path(self.ops.cgroup(pid)) == destination,
                'process cgroup move did not verify')
        self._event('moved', identity=identity, destination=str(destination))

    def attach_query(self, pid):
        self._assert_active(); pid = int(pid)
        require(pid != self.ops.manager_pid() and not self.ops.is_database(pid), 'cannot bound supervisor/database as query')
        identity = self.ops.identity(pid)
        self.state['query_processes'].append(identity); self._save('query_attach_planned', identity=identity)
        self._move(pid, self.query); self._set_process_affinity(pid, self.selected)
        self._save('query_attached', identity=identity)

    def _capture_database(self, pid):
        require(self.ops.is_database(pid), 'expected initial actual DataNode PID')
        identity = self.ops.identity(pid)
        original = self._membership_path(self.ops.cgroup(pid))
        require(not original.is_relative_to(self.parent), 'capture original DataNode before bounded startup')
        thread_affinities = {str(tid): sorted(self.ops.affinity(tid)) for tid in self.ops.task_ids(pid)}
        self.state['database_original'] = {'identity': identity, 'cgroup': str(original),
            'affinity': sorted(self.ops.affinity(pid)), 'thread_affinities': thread_affinities}
        self._save('database_original_captured')
        keeper_pid = self.ops.spawn_keeper(self.keeper_seconds)
        self.state['keeper'] = self.ops.identity(keeper_pid)
        self._save('keeper_started', lifetime_seconds=self.keeper_seconds)
        self._move(keeper_pid, original)
        require(self.ops.identity(keeper_pid) == self.state['keeper'], 'original cgroup keeper exited')

    def attach_database(self, pid):
        self._assert_active(); pid = int(pid)
        require(pid != self.ops.manager_pid() and self.ops.is_database(pid), 'expected actual DataNode PID')
        require(self.state['database_original'] is not None, 'original placement must be captured before isolation')
        identity = self.ops.identity(pid)
        self.state['database_processes'].append(identity); self._save('database_attach_planned', identity=identity)
        self._move(pid, self.database); self._set_process_affinity(pid, self.selected)
        self._save('database_attached', identity=identity)

    def snapshot(self):
        if self.state['phase'] == 'active': self._assert_active()
        result = {'protocol': self.JOURNAL_PROTOCOL, 'partition': self.PARTITION,
                  'scheduler_load_balancing': True,
                  'time_ns': time.time_ns(), 'phase': self.state['phase'], 'paths': self.paths,
                  'cpus': sorted(self.selected), 'groups': {}}
        fields = ('cgroup.type', 'cgroup.procs', 'cgroup.events', 'cgroup.threads', 'cgroup.subtree_control',
            'cpuset.cpus', 'cpuset.cpus.effective', 'cpuset.cpus.exclusive.effective', 'cpuset.cpus.partition',
            'cpu.max', 'cpu.stat', 'cpu.pressure', 'memory.max', 'memory.current', 'memory.peak',
            'memory.events', 'memory.pressure', 'memory.swap.current', 'memory.swap.max', 'io.pressure')
        for name, path in [('parent',self.parent), ('query',self.query), ('database',self.database)]:
            if self.ops.exists(path): result['groups'][name] = {f:self._read(path/f) for f in fields if self.ops.exists(path/f)}
        self._event('snapshot', snapshot=result)
        return result

    def restore_database(self, pid):
        original = self.state['database_original']
        require(original is not None, 'no captured original database placement')
        pid = int(pid); require(self.ops.is_database(pid), 'restoration PID is not DataNode')
        current = self.ops.identity(pid)
        for known in self.state['database_processes']:
            if known['pid'] == pid: require(known == current, 'DataNode PID was reused')
        self.state['phase'] = 'restoring'; self._save('restore_database_planned', identity=current)
        # Restore exact original affinity only after returning exclusive CPUs to host.
        if self.ops.exists(self.parent / 'cpuset.cpus.partition'):
            self._write(self.parent / 'cpuset.cpus.partition', 'member')
            self._write(self.parent / 'cpuset.cpus.exclusive', '')
            require(self._read(self.parent / 'cpuset.cpus.partition') == 'member', 'partition release failed')
        destination = Path(original['cgroup'])
        require(self.ops.exists(destination / 'cgroup.procs'), 'original cgroup vanished; keeper did not preserve it')
        self._move(pid, destination)
        masks = original['thread_affinities'] if current == original['identity'] else None
        self._set_process_affinity(pid, set(original['affinity']), masks)
        self.state['database_restored'] = True; self.state['restored_database_identity'] = current
        self._save('database_restored', original_cgroup=str(destination), affinity=original['affinity'])
        if self.state['keeper'] is not None:
            self.ops.stop_keeper(self.state['keeper']); self.state['keeper'] = None; self._save('keeper_released')
        return {'identity':current, 'cgroup':str(destination), 'affinity':original['affinity']}

    def close(self):
        if self.state['phase'] == 'closed': return
        try:
            if self.state['database_original'] is not None:
                require(self.pid_file is not None, 'current runtime DataNode pidfile required for automatic restoration')
                text = self.pid_file.read_text().strip()
                require(text.isdigit() and int(text) > 0, 'invalid current DataNode pidfile')
                self.restore_database(int(text))
            for raw in reversed(self.state['created_groups']):
                path = Path(raw)
                if not self.ops.exists(path): continue
                require(path in (self.parent,self.query,self.database), 'refusing to remove unowned group')
                require(not self._read(path/'cgroup.procs') and not self._read(path/'cgroup.threads') and
                        re.search(r'(?:^|\n)populated 0(?:\n|$)', self._read(path/'cgroup.events')),
                        'owned group still has live processes; leave it for supervisor retry')
                if path == self.parent:
                    self._write(path/'cpuset.cpus.partition','member')
                self.ops.rmdir(path); self._event('removed', path=str(path))
            pending = self.state.get('pending_mkdir')
            require(not pending or not self.ops.exists(Path(pending)),
                    'unconfirmed mkdir remains; preserve group for ownership review')
            added = self.state['added_root_controllers']
            if added:
                require(self._foreign_states() == self.state.get('enabled_foreign_states', self.state['before']['foreign_states']),
                        'foreign cgroup configuration changed; refusing shared-controller rollback')
                enabled = set(self._read(self.root/'cgroup.subtree_control').split())
                require(set(self.state['before']['enabled']) <= enabled,
                        'root controller state changed externally')
                commands = ['-' + c for c in added if c in enabled]
                if commands: self._write(self.root/'cgroup.subtree_control',' '.join(commands))
                require(set(self._read(self.root/'cgroup.subtree_control').split()) == set(self.state['before']['enabled']),
                        'root controller restoration did not verify')
            self.state['phase']='closed'; self.state['restoration_complete']=True; self._save('closed')
        except BaseException as error:
            self.state['cleanup_error'] = type(error).__name__ + ': ' + str(error)
            self._save('cleanup_blocked', reason=self.state['cleanup_error'])
            raise


def assert_balanced_policy(operations, parent, selected):
    """One live policy check shared by supervisor admission and pre-exec entry."""
    read = lambda path: operations.read(path)
    require(read(parent/'cpuset.cpus.partition') == CgroupV2Resources.PARTITION,
            'load-balanced root partition became invalid')
    require(all(cpus(read(parent/field)) == selected for field in (
        'cpuset.cpus', 'cpuset.cpus.effective', 'cpuset.cpus.exclusive',
        'cpuset.cpus.exclusive.effective')), 'effective/exclusive CPUs differ from approved two-CPU set')
    require(read(parent/'cgroup.type') == 'domain' and not read(parent/'cgroup.procs') and
            read(parent/'cpu.max') == CgroupV2Resources.CPU_MAX,
            'shared CPU quota changed or parent acquired internal processes')
    require(read(parent/'memory.max') == 'max' and
            read(parent/'query/memory.max') == CgroupV2Resources.QUERY_MEMORY_MAX and
            read(parent/'database/memory.max') == 'max', 'query/database memory boundary changed')
    for leaf in (parent/'query', parent/'database'):
        require(read(leaf/'cpu.max') == 'max 100000', 'unexpected per-leaf CPU quota')
        require(read(leaf/'cgroup.type') == 'domain' and read(leaf/'cpuset.cpus.partition') == 'member' and
                cpus(read(leaf/'cpuset.cpus')) == selected and
                cpus(read(leaf/'cpuset.cpus.effective')) == selected and
                not cpus(read(leaf/'cpuset.cpus.exclusive.effective')), 'invalid leaf domain/cpuset')


def enter_leaf_exec(leaf_path, argv, *, journal_path, cgroup_root='/sys/fs/cgroup', ops=None):
    """Fresh single-threaded child trampoline, before exec of the JVM launcher.

    The external supervisor must already have captured the original DataNode.
    This never adopts/initializes an experiment or changes its policy.
    """
    root=Path(cgroup_root).resolve(); leaf=Path(leaf_path).resolve()
    state=json.loads(Path(journal_path).read_text()); name=state.get('name','')
    require(re.fullmatch(r'iotdbfs-ratio-[0-9a-f]{32}',name) is not None and
            state.get('protocol')==CgroupV2Resources.JOURNAL_PROTOCOL and state.get('phase')=='active' and
            state.get('partition')==CgroupV2Resources.PARTITION and
            state.get('scheduler_load_balancing') is True, 'invalid active resource journal')
    expected={key:str(root/name/key) for key in ('query','database')};expected['parent']=str(root/name)
    require(state.get('cgroup_root')==str(root) and state.get('paths')==expected and
            leaf in (root/name/'query',root/name/'database'), 'unowned cgroup leaf')
    require(bool(argv) and isinstance(argv,list) and all(isinstance(x,str) and x for x in argv), 'explicit exec argv required')
    operations=ops or LinuxOps('/proc')
    selected=state.get('cpus',[])
    require(isinstance(selected,list) and len(selected)==2 and all(type(c) is int and c>=0 for c in selected)
            and len(set(selected))==2, 'invalid resource journal CPUs')
    assert_balanced_policy(operations,root/name,set(selected))
    if leaf.name=='database': require(state.get('database_original') is not None,'capture original database before restart')
    pid=operations.manager_pid()
    require(pid != state['manager_identity']['pid'], 'supervisor cannot exec into bounded leaf')
    operations.write(leaf/'cgroup.procs',str(pid))
    operations.set_affinity(pid,set(state['cpus']))
    require(operations.cgroup(pid)=='/'+str(leaf.relative_to(root)) and operations.affinity(pid)==set(state['cpus']),
            'pre-exec cgroup/affinity did not verify')
    if ops is not None: return {'pid':pid,'leaf':str(leaf),'argv':list(argv)}
    os.execvp(argv[0],argv)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation',choices=['enter-leaf-exec'])
    parser.add_argument('--leaf',type=Path,required=True);parser.add_argument('--journal',type=Path,required=True)
    parser.add_argument('--cgroup-root',type=Path,default=Path('/sys/fs/cgroup'))
    raw=sys.argv[1:]; split=raw.index('--') if '--' in raw else len(raw)
    args=parser.parse_args(raw[:split]); argv=raw[split+1:]
    enter_leaf_exec(args.leaf,argv,journal_path=args.journal,cgroup_root=args.cgroup_root)


if __name__=='__main__': main()
