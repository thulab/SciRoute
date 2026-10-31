"""Owned, restartable Table 5 database; never mutates the shared installation.

Run this module on the experiment server. ``prepare`` creates a fresh home;
``start`` and ``stop`` verify ownership and PID start ticks. The private
connection file must not be included in public evidence archives.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time


PORTS = dict(cn_internal_port=34610, cn_consensus_port=34620,
             dn_internal_port=34630, dn_mpp_data_exchange_port=34640,
             dn_schema_region_consensus_port=34650,
             dn_data_region_consensus_port=34660, dn_rpc_port=34667,
             cn_metric_prometheus_reporter_port=34691,
             dn_metric_prometheus_reporter_port=34692)
MARKER = "iotdbfs-table5-isolated-database/1"


def save(path, value, private=False):
    temporary = path.with_suffix(path.suffix + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
    if not private:
        temporary.chmod(0o644)
    temporary.replace(path)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def identity(pid, work):
    proc = Path('/proc') / str(pid)
    if not proc.exists():
        return None
    stat = (proc / 'stat').read_text().split(')', 1)[1].split()
    if stat[0] == 'Z':
        return None
    command = (proc / 'cmdline').read_bytes().split(b'\0')
    homes = [Path(arg.split(b'=',1)[1].decode()).resolve() for arg in command
             if arg.startswith((b'-DIOTDB_HOME=',b'-DCONFIGNODE_HOME='))]
    if work.resolve() not in homes:
        # Do not expose the actual command on a mismatch.
        raise RuntimeError('PID does not belong to the owned database home')
    memory = [arg.decode() for arg in command
              if arg.startswith((b'-Xms', b'-Xmx', b'-XX:MaxDirectMemorySize='))]
    return dict(pid=pid, start_ticks=int(stat[19]), memory_options=memory,
                executable=str((proc / 'exe').resolve()),
                cgroup=(proc / 'cgroup').read_text().strip())


def manifest(work):
    result = json.loads((work / 'deployment.json').read_text())
    if result['protocol'] != MARKER or result['home'] != str(work.resolve()):
        raise RuntimeError('invalid owned database manifest')
    return result


def prepare(work, source, source_pid):
    source = source.resolve()
    from experiments.single_file.environment import connection_environment as source_connection
    # This customized release's initial root credential is configured outside
    # ordinary properties; reuse the authorized daemon's connection in memory.
    credentials = source_connection(source_pid, 'v00_head_0000000000000000')
    if work.exists():
        raise RuntimeError('prepare requires a fresh database home')
    for port in PORTS.values():
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', port))
    work.mkdir(parents=True)
    # Local copies protect logs, script side effects, plugins and activation state.
    for name in ('conf', 'sbin', 'activation', 'ext'):
        shutil.copytree(source / name, work / name)
    # Preserve the same machine's existing licensed release identity in an
    # independent private copy. Never regenerate or edit the original license.
    if (source / '.env').is_file():
        shutil.copy2(source / '.env', work / '.env')
        (work / '.env').chmod(0o600)
    (work / 'lib').symlink_to(source / 'lib', target_is_directory=True)
    for name in ('logs', 'state', 'data'):
        (work / name).mkdir()
    settings = {**PORTS, 'cluster_name': 'table5_three_path_20261031',
                'cn_seed_config_node': '127.0.0.1:34610',
                'dn_seed_config_node': '127.0.0.1:34610',
                'cn_internal_address': '127.0.0.1',
                'dn_internal_address': '127.0.0.1', 'dn_rpc_address': '127.0.0.1',
                'schema_replication_factor': 1, 'data_replication_factor': 1,
                'wal_mode': 'SYNC', 'enable_partial_insert': 'false'}
    for node in ('cn', 'dn'):
        for kind in ('system', 'consensus'):
            settings[f'{node}_{kind}_dir'] = str(work / 'state' / node / kind)
    settings['dn_data_dirs'] = str(work / 'state/dn/data')
    settings['dn_wal_dirs'] = str(work / 'state/dn/wal')
    config = work / 'conf/iotdb-system.properties'
    lines = config.read_text().splitlines()
    lines = [line for line in lines if line.split('=', 1)[0].strip() not in settings]
    lines += ['# Isolated Table 5 overrides; shared service files untouched.']
    lines += [f'{key}={value}' for key, value in settings.items()]
    config.write_text('\n'.join(lines) + '\n')
    # Reject surviving shared absolute data paths rather than silently sharing them.
    for line in lines:
        if line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        if ('dir' in key or 'path' in key) and value.startswith('/') and not value.startswith(str(work)):
            raise RuntimeError('configuration retains an unowned absolute path')
    jars = {path.name: digest(path) for path in sorted((source / 'lib').glob('*.jar'))}
    result = dict(protocol=MARKER, home=str(work), source_release=str(source),
                  settings=settings, datanode_heap_mib=15232,
                  datanode_direct_mib=2176, confignode_heap_mib=3584,
                  confignode_direct_mib=512, jar_sha256=jars,
                  config_sha256=digest(config), processes={},
                  credential_source_pid=source_pid,
                  credential_policy='authorized daemon settings; private connection file only',
                  license_policy='unchanged same-host release identity and license; no generated activation',
                  shared_installation_modified=False)
    save(work / 'deployment.json', result)
    save(work / 'connection.private.json',
         dict(IOTDB_HOST='127.0.0.1', IOTDB_PORT=str(PORTS['dn_rpc_port']),
              IOTDB_USER=credentials['IOTDB_USER'],
              IOTDB_PASS=credentials['IOTDB_PASS']), private=True)
    return result


def connection_environment(work, database):
    work = Path(work).resolve()
    manifest(work)
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', database):
        raise RuntimeError('invalid experiment database name')
    result = json.loads((work / 'connection.private.json').read_text())
    result['IOTDB_DB'] = database
    return result


def start(work):
    record = manifest(work)
    if record.get('kind') == 'shared-binding':
        raise RuntimeError('shared binding is read-only; cannot start service')
    for role in ('confignode', 'datanode'):
        old = record['processes'].get(role)
        if old and identity(old['pid'], work):
            raise RuntimeError('owned process is already running')
    for role, port, heap, direct in [('confignode',34610,3584,512),
                                    ('datanode',34667,15232,2176)]:
        env = dict(os.environ)
        for key in list(env):
            if key.startswith(('IOTDB_', 'CONFIGNODE_', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS')):
                env.pop(key)
        env.update(IOTDB_HOME=str(work), IOTDB_DATA_HOME=str(work),
                   IOTDB_CONF=str(work / 'conf'), IOTDB_LOG_DIR=str(work / 'logs'),
                   CONFIGNODE_HOME=str(work), CONFIGNODE_DATA_HOME=str(work),
                   CONFIGNODE_CONF=str(work / 'conf'),
                   CONFIGNODE_LOG_DIR=str(work / 'logs'), MEMORY_SIZE=f'{heap+direct}M')
        option = 'IOTDB_JMX_OPTS' if role == 'datanode' else 'CONFIGNODE_JMX_OPTS'
        env[option] = f'-Xms{heap}M -Xmx{heap}M -XX:MaxDirectMemorySize={direct}M'
        log = work / f'{role}.private.log'
        with log.open('ab') as output:
            log.chmod(0o600)
            proc = subprocess.Popen(['bash', str(work / 'sbin' / f'start-{role}.sh'), '-f'],
                                    cwd=work, env=env, stdout=output, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, start_new_session=True)
        (work / f'{role}.pid').write_text(str(proc.pid) + '\n')
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f'owned {role} exited; inspect private log locally')
            try:
                with socket.create_connection(('127.0.0.1',port), timeout=1):
                    break
            except OSError:
                time.sleep(1)
        else:
            raise RuntimeError(f'owned {role} startup timed out')
        record['processes'][role] = identity(proc.pid, work)
        save(work / 'deployment.json', record)
    return dict(ready=True, **record['processes'], rpc_port=PORTS['dn_rpc_port'])


def stop(work):
    record = manifest(work)
    if record.get('kind') == 'shared-binding':
        raise RuntimeError('shared binding is read-only; cannot stop service')
    report = {}
    for role in ('datanode', 'confignode'):
        saved = record['processes'].get(role)
        if not saved:
            continue
        actual = identity(saved['pid'], work)
        if actual:
            if saved['start_ticks'] != actual['start_ticks']:
                raise RuntimeError('owned PID reused; refusing signal')
            os.kill(saved['pid'], signal.SIGTERM)
            deadline = time.monotonic() + 180
            while identity(saved['pid'], work) and time.monotonic() < deadline:
                time.sleep(1)
            if identity(saved['pid'], work):
                raise RuntimeError('owned database did not stop; no forced shared cleanup')
        report[role] = dict(pid=saved['pid'], stopped=True)
    save(work / 'stopped.json', report)
    return report


def bind_shared(work, source_pid, datanode_pid):
    """Observe an already authorized instance without modifying any service."""
    from experiments.single_file.environment import connection_environment as source_connection
    if work.exists():
        raise RuntimeError('shared binding requires a fresh evidence directory')
    connection = source_connection(source_pid, 'v00_head_0000000000000000')
    if connection['IOTDB_HOST'] not in ('127.0.0.1','localhost') or connection['IOTDB_PORT'] != '33667':
        raise RuntimeError('source daemon does not use the authorized shared RPC')
    proc = Path('/proc') / str(datanode_pid)
    command = (proc / 'cmdline').read_bytes().split(b'\0')
    if proc.stat().st_uid != os.geteuid() or b'com.timecho.iotdb.DataNode' not in command:
        raise RuntimeError('shared binding target is not the owned DataNode')
    fields = (proc / 'stat').read_text().split(')',1)[1].split()
    process = dict(pid=datanode_pid,start_ticks=int(fields[19]),
                   executable=str((proc/'exe').resolve()),cwd=str((proc/'cwd').resolve()),
                   memory_options=[x.decode() for x in command if x.startswith((b'-Xms',b'-Xmx',b'-XX:MaxDirectMemorySize='))],
                   cgroup=(proc/'cgroup').read_text().strip(),
                   affinities={p.name:sorted(os.sched_getaffinity(int(p.name))) for p in (proc/'task').iterdir()})
    work.mkdir(parents=True)
    (work/'datanode.pid').write_text(str(datanode_pid)+'\n')
    record=dict(protocol=MARKER,kind='shared-binding',home=str(work),
                credential_source_pid=source_pid,processes={'datanode':process},
                shared_installation_modified=False,service_started=False,
                resource_policy='retain existing heap/direct memory; experiment supervisor shares CPU quota; exact restoration required',
                isolated_attempt='db-001 authenticated but activation denied schema/write; stopped without altering license')
    save(work/'deployment.json',record)
    connection.pop('IOTDB_DB')
    save(work/'connection.private.json',connection,private=True)
    return dict(kind='shared-binding',pid=datanode_pid,start_ticks=process['start_ticks'],
                memory_options=process['memory_options'],pid_file=str(work/'datanode.pid'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare','start','stop','status','bind-shared'])
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--source', type=Path)
    parser.add_argument('--source-pid', type=int)
    parser.add_argument('--datanode-pid', type=int)
    args = parser.parse_args()
    work = args.work.resolve()
    if args.action == 'prepare':
        if not args.source or not args.source_pid:
            parser.error('prepare requires --source and --source-pid')
        result = prepare(work, args.source, args.source_pid)
        result = {k: result[k] for k in ('protocol','home','settings')}
    elif args.action == 'bind-shared':
        if not args.source_pid or not args.datanode_pid:
            parser.error('bind-shared requires --source-pid and --datanode-pid')
        result=bind_shared(work,args.source_pid,args.datanode_pid)
    elif args.action == 'start':
        result = start(work)
    elif args.action == 'stop':
        result = stop(work)
    else:
        record = manifest(work)
        result = {role: identity(info['pid'], Path(info['cwd']) if record.get('kind') == 'shared-binding' else work)
                  for role,info in record['processes'].items()}
    print(json.dumps(result,sort_keys=True))


if __name__ == '__main__':
    main()
