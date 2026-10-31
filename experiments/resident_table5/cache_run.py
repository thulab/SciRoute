"""HDF5-only paired follow-up: reopen controls versus FS-index handle reuse.

No product mount, import, database query or managed backend change. The SQL
helper is used only for the existing database-scoped quiet/resource gate.
"""
from contextlib import ExitStack
import argparse
import ctypes as C
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time

from experiments.lifecycle_table5.run import save
from experiments.lifecycle_table5.database import connection_environment
from experiments.lifecycle_table5.qualification import quiesce, live_environment
from experiments.single_file.section42_20261026 import read_matrix as rm, sessions, writes
from experiments.single_file.section42_20261026.read_matrix_native import NativeHdf5, need
from experiments.cross_version.version_directory.v00_head_probe import Worker
from experiments.resident_table5.run import resource_detail
from fs_index.lifecycle import Catalog
from fs_index.hdf5_handles import Hdf5HandleCache

ARMS = ('FS-scan', 'FS-index-reopen', 'FS-index-handles')


def chunk_cache_settings(reader, cache):
    lib = reader.lib
    lib.H5Dget_access_plist.restype, lib.H5Dget_access_plist.argtypes = C.c_int64, [C.c_int64]
    lib.H5Pget_chunk_cache.restype = C.c_int
    lib.H5Pget_chunk_cache.argtypes = [C.c_int64, C.POINTER(C.c_size_t), C.POINTER(C.c_size_t), C.POINTER(C.c_double)]
    lib.H5Pclose.restype, lib.H5Pclose.argtypes = C.c_int, [C.c_int64]
    result = []
    for key, session in cache.entries.items():
        props = reader._check(lib.H5Dget_access_plist(session['dataset_id']), 'access properties')
        try:
            slots, size, weight = C.c_size_t(), C.c_size_t(), C.c_double()
            reader._check(lib.H5Pget_chunk_cache(props, C.byref(slots), C.byref(size), C.byref(weight)), 'chunk cache properties')
            result.append(dict(source=key[0], version=key[2], dataset=key[3],
                               slots=slots.value, bytes=size.value, w0=weight.value))
        finally:
            reader._check(lib.H5Pclose(props), 'close access properties')
    return result


def main():
    parser = argparse.ArgumentParser()
    for key in ('work', 'prior', 'resident', 'database-root', 'pid-file'):
        parser.add_argument('--'+key, type=Path, required=True)
    args = parser.parse_args()
    work = args.work
    work.mkdir(parents=True, exist_ok=False)
    def cancel(sig, frame):
        raise RuntimeError('HDF5 follow-up cancelled; release owned handles')
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, cancel)
    prior = args.prior/'collection'
    frozen = json.loads((prior/'frozen.json').read_text())
    previous = [json.loads(line) for line in (prior/'lifecycles.jsonl').read_text().splitlines()]
    resident_env = json.loads((args.resident/'collection/environment.json').read_text())
    need(json.loads((args.resident/'supervisor-report.json').read_text())['complete'], 'resident run incomplete')
    cases = [case for case in frozen['cases'] if case['format_id']=='hdf5']
    need(len(cases)==3, 'HDF5 cases differ')
    libs = resident_env['native_libraries']
    hlib = next(Path(path) for path in libs if 'libhdf5' in path)
    need(writes.sha_file(hlib)==libs[str(hlib)], 'HDF5 library changed')
    env = dict(os.environ, **connection_environment(args.database_root, frozen['database']))
    Worker.environment = dict(env, IOTDB_V00_PROBE_DATABASE=frozen['database'],
                              IOTDB_V00_PROBE_PASSWORD=env['IOTDB_PASS'])
    helper = work/'sql-admin'
    shutil.copy2(prior/'sql-admin', helper)
    need(writes.sha_file(helper)==frozen['admin_sha256'], 'admin differs')
    pid = int(args.pid_file.read_text())
    source = Path(__file__).resolve().parents[2]
    closure = {str(p.relative_to(source)): writes.sha_file(p)
               for folder in ('experiments', 'fs_index') for p in (source/folder).rglob('*.py')
               if not any(part in p.parts for part in ('work', '__pycache__', 'results'))}
    save(work/'source-closure.json', closure)
    rows, warmups = [], []
    report = dict(complete=False, protocol='iotdbfs-resident-hdf5-handles/1', records=0, queries=0)
    save(work/'report.json', report)
    admin = Worker(helper, port=int(env['IOTDB_PORT']))
    try:
        need(admin.call('settings')=='CONFIG\tSYNC\tfalse', 'database policy differs')
        live = live_environment(pid, int(env['IOTDB_PORT']), work)
        for key in ('pid', 'process_start_ticks', 'config_sha256', 'jar_sha256', 'jvm_memory_flags'):
            need(live[key]==resident_env['live_database'][key], 'live database differs: '+key)
        import numpy, pyarrow
        save(work/'environment.json', dict(python=sys.version, executable=sys.executable,
             numpy=numpy.__version__, pyarrow=pyarrow.__version__, native_libraries=libs, live_database=live))
        save(work/'frozen.json', dict(protocol=report['protocol'], prior=str(args.prior), resident=str(args.resident),
             cases=cases, arms=ARMS, rounds=3, q=[1,10,100], cache_capacity=8,
             clock='direct session wall / Q; every request includes catalog lookup, identity check and H5Dread',
             first='one first query including cache miss and two warmups per source/path/round',
             cache='native HDF5 default chunk cache retained with handles; no application value/result cache',
             lifetime='handle cache persists within arm/round; first open in first-query clock; close separately timed',
             comparison='paired native follow-up; original IoTDB-FS iotdb-indexed measurements remain a separate campaign'))
        for campaign in range(3):
            qorder = (1,10,100)[campaign:] + (1,10,100)[:campaign]
            for arm in ARMS[campaign:] + ARMS[:campaign]:
                block = work/f'c{campaign}-{arm}'
                block.mkdir()
                quiesce(admin, pid, block/'quiescence-before.json')
                reader = NativeHdf5(hlib)
                catalog = cache = None
                old_catalog = prior/f'c{campaign}-FS-index/index.sqlite'
                original_digest = writes.sha_file(old_catalog)
                with ExitStack() as stack:
                    if arm!='FS-scan':
                        shutil.copy2(old_catalog, block/'index.sqlite')
                        catalog = stack.enter_context(Catalog(block/'index.sqlite'))
                    if arm=='FS-index-handles':
                        cache = stack.enter_context(Hdf5HandleCache(catalog, reader, capacity=8))
                    for case in cases:
                        sid = case['source_id']
                        saved = next(row for row in previous if row['campaign']==campaign and
                                     row['source_id']==sid and row['q']==100 and row['arm']=='FS-index')
                        raw = saved['destination']
                        need(writes.sha_file(Path(raw))==case['sha256'], 'source hash differs')
                        actual = dict(case, raw=raw, logical_path=raw, version=1)
                        def measure(q, role):
                            pending = []
                            rb = resource_detail(pid)
                            need(rb['parent_cpu_max']=='200000 100000' and rb['memory.max']=='2147483648' and
                                 rb['memory.swap.max']=='0' and rb['cpuset.cpus.effective']=='2-3', 'resource envelope differs')
                            cb = cache.stats() if cache else None
                            if cache:
                                result = sessions.run_session(q=q, prepare_reuse=0, prepare=lambda: None,
                                    read_owned=lambda _, i: cache.read(actual), release=lambda _: None, deferred_audits=pending)
                            elif catalog:
                                result = sessions.run_session(q=q, prepare_reuse=0, prepare=lambda: None,
                                    read_owned=lambda _, i: catalog.read(reader, actual), release=lambda _: None, deferred_audits=pending)
                            else:
                                result = sessions.run_native_session(reader=reader, spec=actual, q=q,
                                    prepare_reuse=0, deferred_audits=pending)
                            for observation, owned in pending:
                                observation['validation'] = rm.validate_owned(actual, owned)
                                observation['oracle_verified'] = True
                                need(observation['native_value_read_calls']==1, 'native value read missing')
                                if catalog:
                                    need(observation['persistent_index'] and observation['metadata_reused'], 'index bypassed')
                                if cache:
                                    need(observation['handles_reused'] and observation['handle_cache_hit']==(role!='first'), 'unexpected handle state')
                            ra = resource_detail(pid)
                            need('oom_kill 0' in ra['memory.events'] and 'oom 0' in ra['memory.events'], 'OOM')
                            result.update(campaign=campaign, arm=arm, source_id=sid, q=q, role=role,
                                source_sha256=case['sha256'], source_binding=raw, source_version=1,
                                resource_before=rb, resource_after=ra, cache_before=cb,
                                cache_after=cache.stats() if cache else None)
                            destination = 'sessions.jsonl' if role=='measurement' else 'warmups.jsonl'
                            with (work/destination).open('a') as stream:
                                stream.write(json.dumps(result)+'\n'); stream.flush(); os.fsync(stream.fileno())
                            (rows if role=='measurement' else warmups).append(result)
                        for index in range(3):
                            measure(1, 'first' if index==0 else 'warmup')
                        for q in qorder:
                            measure(q, 'measurement')
                            report.update(records=len(rows), queries=sum(row['q'] for row in rows))
                            save(work/'report.json', report)
                        need(writes.sha_file(Path(raw))==case['sha256'], 'source changed after reading')
                    if cache:
                        properties = chunk_cache_settings(reader, cache)
                        before = cache.stats()
                        start = time.perf_counter_ns(); cache.close(); elapsed = time.perf_counter_ns()-start
                        save(block/'handles.json', dict(before=before, after=cache.stats(), close_ns=elapsed,
                            native_chunk_cache=properties))
                    if catalog:
                        save(block/'catalog-status.json', catalog.stats())
                need(writes.sha_file(old_catalog)==original_digest, 'original catalog changed')
                save(block/'closed.json', dict(complete=True, original_catalog_sha256=original_digest,
                                              cache_closed=not cache or cache.closed))
                quiesce(admin, pid, block/'quiescence-after.json')
        need(len(rows)==81 and sum(row['q'] for row in rows)==2997 and len(warmups)==81, 'incomplete follow-up')
        need(all(writes.sha_file(source/path)==digest for path, digest in closure.items()), 'source code changed')
        report.update(complete=True, warmup_and_first_queries=len(warmups), data_publications=0)
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__, message=str(error))
        raise
    finally:
        admin.close()
        save(work/'report.json', report)


if __name__=='__main__':
    main()
