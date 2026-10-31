"""Read-only, credential-free Linux evidence for the fixed experiment matrix.

Samples are cumulative kernel counters, with wall and monotonic timestamps.
The supplied groups' envelope includes audit/profile work outside query clocks;
neither these samples nor their deltas identify query-exclusive CPU usage.
No environment, full command line, configuration, or license is exported.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import subprocess
import sys
import time

PROC = Path('/proc')
SYS = Path('/sys')
CGROUP_FILES = (
    'cgroup.type', 'cgroup.events', 'cgroup.procs', 'cgroup.threads',
    'cpuset.cpus', 'cpuset.cpus.effective', 'cpuset.mems.effective',
    'cpuset.cpus.partition', 'cpuset.cpus.exclusive.effective',
    'cpu.max', 'cpu.stat', 'cpu.pressure', 'cpu.weight',
    'memory.current', 'memory.peak', 'memory.max', 'memory.swap.current',
    'memory.swap.max', 'memory.events', 'memory.stat', 'memory.pressure',
    'io.stat', 'io.pressure', 'pids.current', 'pids.max',
)
_JVM = re.compile(r'(?:-X(?:ms|mx|ss)\d+[kKmMgG]?|'
                  r'-XX:(?:MaxDirectMemorySize|ActiveProcessorCount|'
                  r'ParallelGCThreads|ConcGCThreads|G1ConcRefinementThreads|'
                  r'CICompilerCount|MaxGCPauseMillis)=\d+[kKmMgG]?|'
                  r'-XX:[+-](?:UseG1GC|UseParallelGC|UseSerialGC|'
                  r'UseZGC|UseContainerSupport|AlwaysPreTouch))\Z')


def _read(path):
    try:
        return Path(path).read_text(errors='replace').strip()
    except OSError as exc:
        return {'unavailable': type(exc).__name__}


def _stamp():
    return {'utc': datetime.now(timezone.utc).isoformat(),
            'wall_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}


def digest_file(path):
    """Hash only an explicitly named artifact, never return its contents."""
    path = Path(path)
    try:
        h = hashlib.sha256()
        with path.open('rb') as stream:
            before = os.fstat(stream.fileno())
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                h.update(block)
            info = os.fstat(stream.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                info.st_size, info.st_mtime_ns, info.st_ctime_ns):
            return {'path': str(path), 'unavailable': 'artifact changed while hashing'}
        return {'path': str(path.resolve()), 'sha256': h.hexdigest(),
                'bytes': info.st_size, 'mtime_ns': info.st_mtime_ns,
                'inode': info.st_ino, 'device': info.st_dev}
    except OSError as exc:
        return {'path': str(path), 'unavailable': type(exc).__name__}


def _numbers(path):
    raw = _read(path)
    if not isinstance(raw, str):
        return raw
    result = {}
    for line in raw.splitlines():
        key, _, value = line.partition(':')
        fields = value.strip().split()
        if fields and fields[0].isdigit():
            result[key] = {'value': int(fields[0]),
                           'unit': fields[1] if len(fields) > 1 else None}
    return result


def process(pid, *, inventory=False):
    """Numeric process counters and explicit safe identity, with PID reuse guard."""
    if type(pid) is not int or pid <= 0:
        raise ValueError('process PID must be a positive integer')
    root = PROC / str(pid)
    stat = _read(root / 'stat')
    if not isinstance(stat, str):
        return {'pid': pid, **stat}
    fields = stat[stat.rfind(')') + 2:].split()
    result = {'pid': pid, 'start_ticks': int(fields[19]),
              'state': fields[0], 'utime_ticks': int(fields[11]),
              'stime_ticks': int(fields[12]), 'threads': int(fields[17]),
              'vsize_bytes': int(fields[20]), 'rss_pages': int(fields[21]),
              'rss_bytes': int(fields[21]) * os.sysconf('SC_PAGE_SIZE'),
              'cgroup': _read(root / 'cgroup'), 'io_raw': _read(root / 'io')}
    status = _read(root / 'status')
    if isinstance(status, str):
        permitted = {'VmPeak', 'VmSize', 'VmHWM', 'VmRSS', 'RssAnon',
                     'RssFile', 'RssShmem', 'VmSwap', 'Threads',
                     'Cpus_allowed_list', 'Mems_allowed_list',
                     'voluntary_ctxt_switches', 'nonvoluntary_ctxt_switches'}
        result['status'] = {k: v.strip() for line in status.splitlines()
                            for k, sep, v in [line.partition(':')]
                            if sep and k in permitted}
    if inventory:
        # Open the actual executable through procfs, even if its original
        # filename has since been replaced or unlinked.
        result['executable'] = digest_file(root / 'exe')
        # Whitelist individual numeric/boolean JVM flags. Never export argv.
        try:
            argv = (root / 'cmdline').read_bytes().split(b'\0')
            result['jvm_safe_flags'] = sorted({arg.decode('ascii') for arg in argv
                if _JVM.fullmatch(arg.decode('ascii', errors='ignore')) and
                arg.isascii()})
        except OSError as exc:
            result['jvm_safe_flags'] = {'unavailable': type(exc).__name__}
        maps = _read(root / 'maps')
        if isinstance(maps, str):
            paths = {}
            for line in maps.splitlines():
                parts = line.split(None, 5)
                if len(parts) == 6 and parts[5].startswith('/'):
                    target = parts[5]
                    # Executable mappings and .so objects, not database pages.
                    if 'x' in parts[1] or re.search(r'\.so(?:\.|$)', target):
                        paths[target] = (parts[3], int(parts[4]))
            binaries = []
            for target, (device, inode) in sorted(paths.items()):
                if target.endswith(' (deleted)'):
                    binaries.append({'path': target, 'unavailable': 'mapped file deleted'})
                    continue
                item = digest_file(target)
                if 'sha256' in item:
                    major, minor = (int(v, 16) for v in device.split(':'))
                    if item['inode'] != inode or item['device'] != os.makedev(major, minor):
                        item = {'path': target, 'unavailable': 'mapped filename replaced'}
                binaries.append(item)
            result['mapped_binaries'] = binaries
        else:
            result['mapped_binaries'] = maps
    end = _read(root / 'stat')
    if not isinstance(end, str) or int(end[end.rfind(')') + 2:].split()[19]) != result['start_ticks']:
        return {'pid': pid, 'unavailable': 'process exited or PID reused during sample'}
    return result


def _command(argv):
    """Only callers' fixed, read-only inventory commands; never a shell."""
    if shutil.which(argv[0]) is None:
        return {'unavailable': 'command not installed'}
    try:
        reply = subprocess.run(argv, capture_output=True, text=True,
                               timeout=15, check=False)
        return {'argv': argv, 'returncode': reply.returncode,
                'stdout': reply.stdout[:256000], 'stderr': reply.stderr[:2000]}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {'unavailable': type(exc).__name__}


def _mount(path):
    raw = _read(PROC / 'self/mountinfo')
    if not isinstance(raw, str):
        return raw
    candidates = []
    for line in raw.splitlines():
        left, sep, right = line.partition(' - ')
        if not sep:
            continue
        before, after = left.split(), right.split()
        mountpoint = before[4].replace('\\040', ' ')
        if str(path) == mountpoint or str(path).startswith(mountpoint.rstrip('/') + '/'):
            # Some network filesystems embed credentials in mount options.
            # Record only common nonsecret options; hash the entire record.
            safe = {'rw', 'ro', 'relatime', 'noatime', 'nodiratime', 'nosuid',
                    'nodev', 'noexec', 'exec', 'sync', 'async', 'discard',
                    'nobarrier', 'seclabel', 'lazytime', 'iversion'}
            opts = before[5].split(',') + after[2].split(',')
            candidates.append({'mountpoint': mountpoint, 'device': before[2],
                'filesystem': after[0], 'safe_options': sorted(set(opts) & safe),
                'mount_record_sha256': hashlib.sha256(line.encode()).hexdigest(),
                'unexported_options': True})
    return max(candidates, key=lambda v: len(v['mountpoint'])) if candidates else {'unavailable': 'mount not found'}


def environment(repo, artifact_paths=(), process_roles=None, data_path=None):
    """One untimed host/software snapshot. Missing fields remain explicit."""
    repo = Path(repo).resolve()
    data = Path(data_path or repo).resolve()
    result = {'protocol': 'iotdbfs-fixed-environment/1', **_stamp(),
        'client_timezone': 'Asia/Shanghai', 'host_timezone': time.tzname,
        'hostname': platform.node(), 'platform': platform.platform(),
        'uname': platform.uname()._asdict(), 'python': sys.version,
        'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
        'page_size_bytes': os.sysconf('SC_PAGE_SIZE'),
        'logical_cpus': os.cpu_count(), 'memory': _numbers(PROC / 'meminfo'),
        'loadavg_raw': _read(PROC / 'loadavg'),
        'virtualization': _command(['systemd-detect-virt']),
        'cpu_inventory': _command(['lscpu', '--json']),
        'numa_inventory': _command(['numactl', '--hardware']),
        'disk_inventory': _command(['lsblk', '--json', '--bytes', '--output',
                                  'NAME,TYPE,SIZE,ROTA,FSTYPE,MOUNTPOINTS,MODEL']),
        'os_release': {}, 'data_path': str(data), 'mount': _mount(data),
        'compiler_versions': {name: _command([name, '--version'])
                              for name in ('cc', 'c++', 'cmake')},
        'artifacts': [digest_file(p) for p in artifact_paths],
        'processes': {role: process(pid, inventory=True)
                      for role, pid in (process_roles or {}).items()}}
    os_release = _read('/etc/os-release')
    if isinstance(os_release, str):
        for line in os_release.splitlines():
            key, sep, value = line.partition('=')
            if sep and key in {'NAME', 'VERSION', 'ID', 'VERSION_ID', 'PRETTY_NAME'}:
                result['os_release'][key] = value.strip('"')
    else:
        result['os_release'] = os_release
    try:
        fs = os.statvfs(data)
        result['filesystem_capacity'] = {'total_bytes': fs.f_blocks * fs.f_frsize,
            'available_bytes': fs.f_bavail * fs.f_frsize,
            'free_bytes': fs.f_bfree * fs.f_frsize, 'free_inodes': fs.f_ffree}
    except OSError as exc:
        result['filesystem_capacity'] = {'unavailable': type(exc).__name__}
    result['cpu_sysfs'] = {}
    cpu_root = SYS / 'devices/system/cpu'
    for cpu in sorted(cpu_root.glob('cpu[0-9]*')):
        result['cpu_sysfs'][cpu.name] = {name: _read(cpu / name) for name in (
            'topology/physical_package_id', 'topology/core_id',
            'topology/thread_siblings_list', 'topology/core_siblings_list',
            'cpufreq/scaling_driver', 'cpufreq/scaling_governor',
            'cpufreq/scaling_cur_freq', 'cpufreq/scaling_min_freq',
            'cpufreq/scaling_max_freq')}
    result['numa_sysfs'] = {node.name: {'cpulist': _read(node / 'cpulist'),
        'distance': _read(node / 'distance'), 'meminfo': _read(node / 'meminfo')}
        for node in (SYS / 'devices/system/node').glob('node[0-9]*')}
    result['package_versions'] = {}
    for name in ('numpy', 'h5py', 'pyarrow', 'apache-iotdb', 'eccodes'):
        try:
            result['package_versions'][name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            result['package_versions'][name] = {'unavailable': 'not in this interpreter'}
    result['disclosure'] = ('Mapped libraries describe these processes at this instant; '
        'helper library runtime versions and database jar/config digests must also '
        'be supplied by the controller. No environment or private command lines recorded.')
    return result


def sample(cgroup_paths, process_roles=None, prev=None):
    """Read raw host/group/process counters. Never reset peaks or kernel counters."""
    started = _stamp()
    groups = {}
    pids = set((process_roles or {}).values())
    for role, supplied in cgroup_paths.items():
        path = Path(supplied)
        if not path.is_absolute():
            raise ValueError('cgroup paths must be absolute')
        values = {name: _read(path / name) for name in CGROUP_FILES}
        members = set()
        try:
            for procs in path.rglob('cgroup.procs'):
                raw = _read(procs)
                if isinstance(raw, str):
                    members.update(int(v) for v in raw.split() if v.isdigit())
        except OSError:
            pass
        pids.update(members)
        groups[role] = {'path': str(path), 'files': values,
                        'recursive_pids': sorted(members)}
    me = resource.getrusage(resource.RUSAGE_SELF)
    result = {'protocol': 'iotdbfs-fixed-resource-sample/1', **started,
        'host': {name: _read(PROC / name) for name in
                 ('stat', 'loadavg', 'diskstats', 'vmstat')},
        'host_memory': _numbers(PROC / 'meminfo'),
        'host_pressure': {name: _read(PROC / 'pressure' / name)
                          for name in ('cpu', 'memory', 'io')},
        'cgroups': groups,
        'process_roles': dict(process_roles or {}),
        'processes': {str(pid): process(pid) for pid in sorted(pids)},
        'sampler': {'pid': os.getpid(), 'user_seconds': me.ru_utime,
            'system_seconds': me.ru_stime, 'maxrss_native': me.ru_maxrss,
            'maxrss_unit': 'KiB on Linux; bytes on macOS',
            'minor_faults': me.ru_minflt, 'major_faults': me.ru_majflt,
            'voluntary_switches': me.ru_nvcsw,
            'involuntary_switches': me.ru_nivcsw},
        'scope': 'cumulative envelope counters; includes untimed audit/profile work; not query-exclusive CPU'}
    if prev is not None:
        result['previous_monotonic_ns'] = prev['monotonic_ns']
        result['sample_start_delta_ns'] = started['monotonic_ns'] - prev['monotonic_ns']
    result['sample_end_monotonic_ns'] = time.monotonic_ns()
    result['sampler']['read_wall_ns'] = result['sample_end_monotonic_ns'] - started['monotonic_ns']
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--paths', type=Path, required=True,
                        help='JSON object of supplied cgroup role:path mappings')
    parser.add_argument('--roles', type=Path,
                        help='Optional JSON object of role:PID mappings')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--stop-file', type=Path, required=True)
    parser.add_argument('--max-seconds', type=float, default=14400)
    args = parser.parse_args(argv)
    if not 1 <= args.interval <= 60 or not 1 <= args.max_seconds <= 86400:
        parser.error('interval must be 1–60 s and runtime 1–86400 s')
    paths = json.loads(args.paths.read_text())
    roles = json.loads(args.roles.read_text()) if args.roles else {}
    if not isinstance(paths, dict) or not paths:
        parser.error('paths must be a nonempty JSON object')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    deadline = time.monotonic() + args.max_seconds
    previous = None
    with os.fdopen(fd, 'w') as stream:
        while not args.stop_file.exists() and time.monotonic() < deadline:
            tick = time.monotonic()
            previous = sample(paths, roles, previous)
            stream.write(json.dumps(previous, sort_keys=True, allow_nan=False) + '\n')
            stream.flush()
            remaining = min(args.interval - (time.monotonic() - tick),
                            deadline - time.monotonic())
            if remaining > 0:
                time.sleep(remaining)


if __name__ == '__main__':
    main()
