#!/usr/bin/env python3
"""V00 head recovery on the explicitly authorized existing TimechoDB instance.

Only a unique v00_head_* database is written. The existing DataNode is restarted;
ConfigNode, license and node identity are preserved. Original config is restored.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def public_failure(error):
    # Only our controlled RuntimeError messages are included. Compiler, filesystem,
    # SDK, and activation diagnostics stay in private logs; never serialize secrets.
    if isinstance(error, RuntimeError):
        return {"type": type(error).__name__, "message": str(error)}
    return {"type": type(error).__name__, "message": "probe failed; inspect owned private logs"}


def identities(args, worker_source):
    java_root = args.server_source / "iotdb-core/datanode/src/main/java/org/apache/iotdb/db"
    sources = [
        java_root / "storageengine/dataregion/memtable/TsFileProcessor.java",
        java_root / "storageengine/dataregion/wal/buffer/WALBuffer.java",
        java_root / "storageengine/dataregion/wal/buffer/WALInfoEntry.java",
        java_root / "storageengine/dataregion/wal/io/WALReader.java",
        java_root / "storageengine/dataregion/wal/io/LogWriter.java",
        java_root / "utils/datastructure/AlignedTVList.java",
        args.server_source / "iotdb-client/client-cpp/src/session/Session.cpp",
        args.server_source / "iotdb-client/client-cpp/src/session/TableSession.cpp",
    ]
    def entries(paths, root):
        return [{"path": str(path.relative_to(root)), "sha256": sha(path)}
                for path in sorted(paths) if path.is_file()]
    revision = subprocess.run(["git", "-C", str(args.server_source), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    require(re.fullmatch(r"[0-9a-f]{40,64}", revision), "invalid server source Git identity")
    return {
        "release_jars": entries((args.release / "lib").glob("*.jar"), args.release),
        "release_bootstrap": entries((args.release / "lib/bootstrap").rglob("*.class"), args.release),
        "sdk_libraries": entries((args.sdk / "lib").glob("*iotdb*"), args.sdk),
        "sdk_headers": entries((args.sdk / "include").rglob("*.h"), args.sdk),
        "source_revision": revision,
        "source_files": entries(sources, args.server_source),
        "probe_source_sha256": sha(Path(__file__)),
        "worker_source_sha256": sha(worker_source),
        "build_binding": "source revision and hashes are observations; no source/release equivalence inferred",
    }


class Worker:
    environment = None

    def __init__(self, binary, port=33667):
        self.log = Path(str(binary) + f".client-{time.time_ns()}.log").open("wb")
        self.log_offset = 0
        self.process = subprocess.Popen([str(binary), str(port)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.log, text=True, bufsize=1, env=self.environment)
        try:
            require(self.receive() == "READY", "SDK worker startup failed")
        except BaseException:
            self.close()
            raise

    def receive(self, timeout=30):
        require(select.select([self.process.stdout], [], [], timeout)[0], "SDK response deadline")
        line = self.process.stdout.readline(140000).strip()
        self.log.flush()
        log_size = Path(self.log.name).stat().st_size
        if not line or line == "ERROR":
            with Path(self.log.name).open("rb") as stream:
                stream.seek(max(self.log_offset, log_size - 32768))
                diagnostic = stream.read().lower()
            self.log_offset = log_size
            if b"not allowed until this node is activated" in diagnostic or b"unactivated" in diagnostic:
                raise RuntimeError("activation_required: existing node rejected schema/write before activation")
            if b"906:" in diagnostic and b"no available schemaregiongroups" in diagnostic:
                raise RuntimeError("schema_regions_not_ready")
            raise RuntimeError("SDK worker failed; inspect private client log")
        self.log_offset = log_size
        return line

    def send(self, command, blob=None):
        self.process.stdin.write(command + ("\t" + blob.hex() if blob is not None else "") + "\n")
        self.process.stdin.flush()

    def call(self, command, blob=None):
        self.send(command, blob)
        response = self.receive()
        if response.startswith("VALUE\t"):
            return bytes.fromhex(response.split("\t", 1)[1])
        return response

    def close(self):
        if self.log.closed:
            return
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait(timeout=10)
        self.process.stdout.close(); self.log.close()


class AckDropProxy:
    """Transparent TCP proxy: arm after SDK open/use, discard the next response."""
    def __init__(self):
        self.armed = threading.Event(); self.dropped = threading.Event()
        self.closed = threading.Event(); self.sockets = []
        self.listen = socket.socket(); self.listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen.bind(("127.0.0.1", 34668)); self.listen.listen(1)
        self.thread = threading.Thread(target=self.run, daemon=True); self.thread.start()

    def run(self):
        try:
            client, _ = self.listen.accept(); server = socket.create_connection(("127.0.0.1", 33667))
            self.sockets = [client, server]
            while not self.closed.is_set():
                for source in select.select(self.sockets, [], [], .1)[0]:
                    data = source.recv(1048576)
                    if not data:
                        return
                    if source is server and self.armed.is_set():
                        self.dropped.set()
                        continue  # Keep client blocked; no retry or ambiguity from synthetic success.
                    (server if source is client else client).sendall(data)
        except OSError:
            pass

    def close(self):
        self.closed.set()
        for sock in [*self.sockets, self.listen]:
            sock.close()
        self.thread.join(timeout=2)


def record(seq, size=4096):
    # Atomicity envelope only, deliberately independent of future production codec.
    prefix = b"iotdbfs-v00-atomic/1\0" + seq.to_bytes(8, "big")
    body = prefix + bytes([seq % 251]) * (size - len(prefix) - 32)
    return body + hashlib.sha256(body).digest()


def sequence(blob):
    require(isinstance(blob, bytes) and len(blob) in (256, 4096), "head cell missing/truncated")
    require(hashlib.sha256(blob[:-32]).digest() == blob[-32:], "torn head digest")
    prefix = b"iotdbfs-v00-atomic/1\0"
    require(blob.startswith(prefix), "head protocol mismatch")
    seq = int.from_bytes(blob[len(prefix):len(prefix)+8], "big")
    require(blob == record(seq, len(blob)), "mixed generation in head body")
    return seq


class ExistingInstance:
    """Control only the identified existing DataNode; preserve its launch environment."""
    def __init__(self, args, event):
        self.release, self.work, self.event = args.release, args.work, event
        self.pid_file = args.runtime / "run/datanode.pid"
        self.pid = int(self.pid_file.read_text().strip())
        require(self.pid == args.datanode_pid, "existing DataNode PID changed before experiment")
        self.identity = self.process_identity(self.pid)
        raw = (Path('/proc') / str(self.pid) / 'environ').read_bytes()
        self.environment = dict(item.decode(errors='surrogateescape').split('=', 1)
                                for item in raw.split(b'\0') if b'=' in item)
        self.config = self.release / 'conf/iotdb-system.properties'
        self.original = self.config.read_bytes()
        self.changed = False
        self.launch_pending = False
        self.launch_start_ticks = 0
        self.protected = {str(p): sha(p) for p in (
            self.release / '.env', self.release / 'activation/license',
            self.release / 'activation/system_info')}
        self.event('original_instance', pid=self.pid, config_sha256=sha(self.config),
                   protected_hashes=self.protected, process_identity=self.identity)
        backup = self.work / 'original-iotdb-system.properties'
        backup.write_bytes(self.original); backup.chmod(0o600)
        with backup.open('rb') as stream:
            os.fsync(stream.fileno())

    def process_identity(self, pid):
        process = Path('/proc') / str(pid)
        require(process.is_dir(), 'DataNode process is absent')
        command = (process / 'cmdline').read_bytes()
        require(b'com.timecho.iotdb.DataNode' in command and
                (process / 'cwd').resolve() == self.release,
                'refusing to signal a process outside the original release/DataNode')
        fields = (process / 'stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': pid, 'start_ticks': fields[19],
                'command_sha256': hashlib.sha256(command).hexdigest()}

    def live(self):
        try:
            state = (Path('/proc') / str(self.pid) / 'stat').read_text().rsplit(')', 1)[1].split()[0]
            return state != 'Z'
        except FileNotFoundError:
            return False

    def check_protected(self):
        require(all(sha(Path(p)) == digest for p, digest in self.protected.items()),
                'original license or node identity changed')

    def adopt_started(self, timeout=5):
        if not self.launch_pending:
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                candidate = int(self.pid_file.read_text().strip())
                if candidate != self.pid:
                    identity = self.process_identity(candidate)
                    require(int(identity['start_ticks']) >= self.launch_start_ticks,
                            'PID file identifies a process predating this launch')
                    self.pid, self.identity = candidate, identity
                    self.launch_pending = False
                    return
            except (OSError, ValueError, RuntimeError):
                pass
            time.sleep(.1)

    def stop(self, crash=False):
        if not self.live():
            return
        require(self.process_identity(self.pid) == self.identity, 'DataNode PID identity changed')
        self.check_protected()
        self.event('signal_datanode', pid=self.pid, signal='SIGKILL' if crash else 'SIGTERM')
        os.kill(self.pid, signal.SIGKILL if crash else signal.SIGTERM)
        deadline = time.monotonic() + 60
        while self.live() and time.monotonic() < deadline:
            time.sleep(.1)
        if self.live() and not crash:
            require(self.process_identity(self.pid) == self.identity, 'DataNode PID identity changed')
            self.event('graceful_stop_timeout', pid=self.pid, signal='SIGKILL')
            os.kill(self.pid, signal.SIGKILL)
            deadline = time.monotonic() + 15
            while self.live() and time.monotonic() < deadline:
                time.sleep(.1)
        require(not self.live(), 'DataNode did not stop; manual inspection required')

    def start(self):
        require(not self.live(), 'refusing to start a duplicate DataNode')
        self.check_protected()
        # A closed RPC listener alone does not prove that the original
        # consensus/internal ports can be rebound after a quick startup/stop.
        ports = (33630, 33640, 33650, 33660, 33667)
        waited = time.monotonic()
        deadline = waited + 180
        while True:
            probes = []
            try:
                for port in ports:
                    sock = socket.socket()
                    probes.append(sock)
                    sock.bind(('127.0.0.1', port))
                break
            except OSError:
                require(time.monotonic() < deadline, 'original DataNode ports cannot yet be rebound')
            finally:
                for sock in probes:
                    sock.close()
            time.sleep(.2)
        self.event('datanode_ports_rebindable', ports=list(ports), waited_seconds=time.monotonic() - waited)
        command = ['bash', 'sbin/start-datanode.sh', '-d', '-p', str(self.pid_file)]
        self.event('start_datanode', command=command, cwd=str(self.release), config_sha256=sha(self.config))
        self.launch_start_ticks = int(float(Path('/proc/uptime').read_text().split()[0]) * os.sysconf('SC_CLK_TCK'))
        self.launch_pending = True
        try:
            with (self.work / f'datanode-start-{time.time_ns()}.log').open('wb') as log:
                subprocess.run(command, cwd=self.release, env=self.environment,
                               stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                               check=True, timeout=60, start_new_session=True)
        finally:
            self.adopt_started()
        deadline = time.monotonic() + 180
        self.adopt_started(timeout=30)
        require(not self.launch_pending, 'cannot identify launched DataNode')
        require(self.live(), 'restarted DataNode missing')
        stable_since = None
        while time.monotonic() < deadline:
            require(self.live(), 'restarted DataNode exited')
            try:
                with socket.create_connection(('127.0.0.1', 33667), timeout=.5):
                    pass
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since >= 10:
                    self.event('datanode_listening', pid=self.pid, stable_rpc_seconds=10)
                    return
            except OSError:
                stable_since = None
            time.sleep(.2)
        raise RuntimeError('DataNode readiness timeout')

    def enable_sync(self):
        self.stop()
        # Only the two durability-related properties change, with byte-exact backup.
        lines = self.original.decode().splitlines()
        lines = [line for line in lines if not re.match(r'^\s*(wal_mode|enable_partial_insert)\s*=', line)]
        configured = ('\n'.join(lines) + '\n# V00 temporary head primitive validation\n'
                      'wal_mode=SYNC\nenable_partial_insert=false\n').encode()
        self.changed = True
        self.config.write_bytes(configured)
        with self.config.open('rb') as stream:
            os.fsync(stream.fileno())
        self.event('temporary_config', config_sha256=sha(self.config), wal_mode='SYNC', enable_partial_insert=False)
        self.start()

    def restore(self):
        self.adopt_started()
        if self.changed:
            self.stop()
            self.config.write_bytes(self.original)
            with self.config.open('rb') as stream:
                os.fsync(stream.fileno())
            self.event('restore_original_config', config_sha256=sha(self.config))
            self.start()
        elif not self.live():
            self.start()
        require(self.config.read_bytes() == self.original, 'original config was not restored')
        self.check_protected()
        self.event('original_service_restored', pid=self.pid, config_sha256=sha(self.config))


def run(args):
    require(args.allow_existing_restart, 'explicit existing-instance restart authorization required')
    for name in ('work', 'release', 'sdk', 'server_source', 'output', 'runtime'):
        setattr(args, name, getattr(args, name).resolve())
    require(args.release == (args.runtime / 'current').resolve(), 'release differs from original runtime/current')
    require(not args.output.exists(), 'report already exists')
    def interrupted(signum, frame):
        raise RuntimeError(f'experiment interrupted by signal {signum}; restoring original service')
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    args.work.mkdir(parents=True, exist_ok=False, mode=0o700)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    lock = (args.runtime / 'run/v00-head-experiment.lock').open('a+b')
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    report = {'protocol': 'iotdbfs-v00-head-existing/1', 'complete': False, 'cases': [],
              'database': 'v00_head_' + uuid.uuid4().hex[:16],
              'cell_tag': uuid.uuid4().hex[:16],
              'authorization': 'user explicitly authorized original database stop/restart on 2026-10-15',
              'scope': 'existing single-DataNode build; SYNC WAL; process crash only, no power-loss or multi-writer CAS claim',
              'original_service_restored': False}
    if args.reuse_probe_report:
        previous = json.loads(args.reuse_probe_report.read_text())
        require(previous.get('protocol') == report['protocol'] and previous.get('complete') and
                previous.get('original_service_restored') and
                re.fullmatch(r'v00_head_[0-9a-f]{16}', previous.get('database', '')),
                'reuse requires a successful V00 probe report')
        report['database'] = previous['database']
        report['reused_probe_report'] = str(args.reuse_probe_report.resolve())
        report['reused_probe_report_sha256'] = sha(args.reuse_probe_report)
    def save():
        save_json(args.output, report)
    def event(operation, **fields):
        entry = dict(time_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), operation=operation, **fields)
        with (args.work / 'operations.jsonl').open('a') as log:
            log.write(json.dumps(entry, sort_keys=True) + '\n'); log.flush(); os.fsync(log.fileno())
        report['last_operation'] = operation
        save()
    instance = None
    clients, proxies = [], []
    binary = args.work / 'v00_head_worker'
    def worker(port=33667, use=True):
        value = Worker(binary, port); clients.append(value)
        if use:
            value.call('use')
            # Listening RPC does not imply all recovered schema regions are ready.
            # Retry a read only; never blindly retry a result-unknown head write.
            deadline = time.monotonic() + 90
            while True:
                try:
                    value.call('read')
                    break
                except RuntimeError as error:
                    if str(error) != 'schema_regions_not_ready' or time.monotonic() >= deadline:
                        raise
                    time.sleep(.5)
        return value
    def recovered(expected):
        instance.start()
        value = worker(); actual = sequence(value.call('read')); value.close()
        require(actual in expected, f'recovery head outside allowed generations: {actual}')
        return actual
    try:
        worker_source = Path(__file__).parent / 'native/v00_head_worker.cpp'
        report['identity'] = identities(args, worker_source)
        # Credentials stay in child memory; no new administrator or certificate.
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'single_file'))
        from environment import connection_environment
        connection = connection_environment(args.source_pid, report['database'])
        require(connection['IOTDB_HOST'] in ('127.0.0.1', 'localhost') and
                connection['IOTDB_PORT'] == '33667' and connection['IOTDB_USER'] == 'root',
                'trusted connection is not the existing loopback database')
        Worker.environment = dict(os.environ, IOTDB_V00_PROBE_PASSWORD=connection['IOTDB_PASS'],
                                  IOTDB_V00_PROBE_DATABASE=report['database'],
                                  IOTDB_V00_PROBE_CELL=report['cell_tag'])
        with (args.work / 'worker-build.log').open('wb') as log:
            subprocess.run(['g++', '-std=c++17', '-O2', str(worker_source),
                '-I' + str(args.sdk / 'include'), '-L' + str(args.sdk / 'lib'),
                '-Wl,-rpath,' + str(args.sdk / 'lib'), '-liotdb_session', '-o', str(binary)],
                check=True, stdout=log, stderr=subprocess.STDOUT)
        report['worker_sha256'] = sha(binary)
        # Validate authentication and reserve a unique test DB before touching service configuration.
        initial = worker(use=False); initial.call('use' if args.reuse_probe_report else 'init')
        require(initial.call('read') == 'EMPTY', 'new test cell already exists')
        report['original_effective_configuration'] = initial.call('settings')
        initial.close()
        # Allocate the test data region before the maintenance restart. This
        # setup value is explicitly flushed and excluded from durability cases.
        initial = worker(); initial.call('write', record(0)); initial.call('flush'); initial.close()
        event('test_cell_reserved', database=report['database'], cell_tag=report['cell_tag'],
              reused_database=bool(args.reuse_probe_report))
        instance = ExistingInstance(args, event)
        instance.enable_sync()
        initial = worker()
        report['experiment_effective_configuration'] = initial.call('settings')
        require(report['experiment_effective_configuration'] == 'CONFIG\tSYNC\tfalse', 'effective durability settings differ')
        initial.call('write', record(0)); initial.close()
        if args.durability_smoke:
            event('effective_sync_ack_then_sigkill')
            value = worker(); value.call('write', record(1)); value.close()
            instance.stop(crash=True); actual = recovered({1})
            value = worker()
            after = value.call('settings'); value.close()
            require(after == 'CONFIG\tSYNC\tfalse', 'recovered effective durability settings differ')
            report['cases'].append({'name': 'effective_sync_ack_then_sigkill',
                'recovered': actual, 'effective_configuration_after_crash': after})
        else:
            event('concurrent_atomic_visibility')
            writer = worker(); readers = [worker(), worker()]
            observations = [[], []]; done = threading.Event()
            def read_loop(index):
                while not done.is_set() or len(observations[index]) < 50:
                    observations[index].append(sequence(readers[index].call('read')))
                    require(len(observations[index]) <= 10000, 'reader observation cap')
            with ThreadPoolExecutor(2) as pool:
                pending = [pool.submit(read_loop, i) for i in range(2)]
                try:
                    for seq in range(1, 201):
                        require(writer.call('write', record(seq, 256 if seq % 2 else 4096)) == 'OK', 'write not acknowledged')
                finally:
                    done.set()
                for future in pending:
                    future.result()
            require(sequence(writer.call('read')) == 200, 'last acknowledged head missing')
            report['cases'].append({'name': 'concurrent_atomic_visibility', 'writes': 200,
                'reader_observations': [len(x) for x in observations], 'torn': 0,
                'observed_sequences': [sorted(set(x)) for x in observations]})
            for client in clients: client.close()
            save()
            for seq in (201, 202, 203):
                event('ack_then_sigkill', sequence=seq)
                value = worker(); value.call('write', record(seq)); value.close()
                instance.stop(crash=True); actual = recovered({seq})
                report['cases'].append({'name': 'ack_then_sigkill', 'expected': seq, 'recovered': actual}); save()
            event('prepared_object_before_head_crash')
            orphan = bytes(range(256)) * 256
            value = worker(); value.call('object', orphan); value.close()
            instance.stop(crash=True); actual = recovered({203})
            value = worker(); require(value.call('read_object') == orphan, 'prepared object did not survive'); value.close()
            report['cases'].append({'name': 'prepared_object_before_head_crash', 'recovered': actual,
                                    'object_bytes': len(orphan), 'object_recovered': True}); save()
            event('lost_actual_rpc_ack')
            proxy = AckDropProxy(); proxies.append(proxy)
            lost = worker(34668)
            proxy.armed.set(); lost.send('write', record(204))
            require(proxy.dropped.wait(timeout=30), 'no server response intercepted')
            value = worker(); require(sequence(value.call('read')) == 204, 'ACK-loss write not visible'); value.close()
            require(not select.select([lost.process.stdout], [], [], .1)[0], 'client unexpectedly received ACK')
            lost.process.kill(); lost.process.wait(timeout=10); lost.close(); proxy.close()
            instance.stop(crash=True); actual = recovered({204})
            report['cases'].append({'name': 'lost_actual_rpc_ack', 'client_received_ack': False,
                'response_dropped': True, 'recovered': actual, 'reconcile': 'read complete head identity before retry'}); save()
            previous = 204
            for seq, delay in ((205, 0), (206, .001), (207, .01)):
                event('inflight_sigkill', sequence=seq, delay_seconds=delay)
                value = worker(); value.send('write', record(seq))
                if delay: time.sleep(delay)
                instance.stop(crash=True); value.close(); actual = recovered({previous, seq})
                report['cases'].append({'name': 'inflight_sigkill', 'before': previous, 'attempted': seq,
                    'delay_seconds': delay, 'recovered': actual}); previous = actual; save()
            event('post_flush_restart')
            value = worker(); value.call('flush'); value.close()
            instance.stop(crash=True); actual = recovered({previous})
            report['cases'].append({'name': 'post_flush_restart', 'recovered': actual})
        report['complete'] = True
    except BaseException as error:
        report['error'] = public_failure(error)
        report['complete'] = False
    finally:
        # Let restoration finish after the first external termination request.
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        for resource in [*proxies, *clients]:
            try:
                resource.close()
            except Exception as error:
                report.setdefault('cleanup_errors', []).append(public_failure(error))
        if instance is not None:
            try:
                instance.restore()
                value = worker(); value.call('read')
                report['restored_effective_configuration'] = value.call('settings'); value.close()
                require(report['restored_effective_configuration'] == report['original_effective_configuration'],
                        'restored effective configuration differs')
                report['original_service_restored'] = True
                report['restored_datanode_pid'] = instance.pid
            except BaseException as error:
                report['restore_error'] = public_failure(error)
                report['complete'] = False
        save(); lock.close()
    print(json.dumps({k: report.get(k) for k in ('complete', 'original_service_restored', 'error', 'restore_error')}))
    require(report['complete'] and report['original_service_restored'], 'head experiment incomplete; inspect report')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('release', 'runtime', 'sdk', 'server-source', 'work', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-pid', type=int, required=True)
    parser.add_argument('--datanode-pid', type=int, required=True)
    parser.add_argument('--allow-existing-restart', action='store_true')
    parser.add_argument('--durability-smoke', action='store_true',
                        help='one ACK/crash case with effective configuration checks')
    parser.add_argument('--reuse-probe-report', type=Path,
                        help='reuse a successful V00 test database with a fresh cell tag; no new region')
    run(parser.parse_args())


