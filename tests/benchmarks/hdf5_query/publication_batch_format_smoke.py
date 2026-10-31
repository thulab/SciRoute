#!/usr/bin/env python3
"""Live batch-publication acceptance in an existing experiment database.

Creates an isolated logical prefix, never creates/deletes a database, and keeps
the published versions and local evidence. All full-payload verification and
typed queries run after every timed copyfile interval has finished.
"""
import argparse
import ctypes
import ctypes.util
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

import geos_pilot as pilot

np, pa = pilot.np, pilot.pa
MIB = 1 << 20


def make_grib_message(raw):
    if raw is not None:
        with raw.open('rb') as stream:
            header = stream.read(16)
            pilot.check(header[:4] == b'GRIB' and len(header) == 16,
                        'GRIB input must start with a complete message')
            edition = header[7]
            size = (int.from_bytes(header[8:16], 'big') if edition == 2 else
                    int.from_bytes(header[4:7], 'big') if edition == 1 else 0)
            pilot.check(16 <= size <= 16 * MIB, 'GRIB first-message size out of bounds')
            stream.seek(0)
            message = stream.read(size)
        pilot.check(len(message) == size and message[-4:] == b'7777',
                    'GRIB first message is incomplete')
        return message, {'kind': 'first_message', 'raw': str(raw.resolve()),
                         'edition': edition}
    # Generate a genuine, moderately sized ecCodes message rather than tens of
    # thousands of tiny templates. ecCodes is used only in this test process.
    name = ctypes.util.find_library('eccodes')
    pilot.check(name is not None, 'ecCodes library unavailable; supply --grib-raw')
    lib = ctypes.CDLL(name)
    lib.codes_handle_new_from_samples.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.codes_handle_new_from_samples.restype = ctypes.c_void_p
    lib.codes_set_long.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_long]
    lib.codes_set_long.restype = ctypes.c_int
    lib.codes_set_double_array.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                         ctypes.POINTER(ctypes.c_double), ctypes.c_size_t]
    lib.codes_set_double_array.restype = ctypes.c_int
    lib.codes_get_message.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
                                     ctypes.POINTER(ctypes.c_size_t)]
    lib.codes_get_message.restype = ctypes.c_int
    lib.codes_handle_delete.argtypes = [ctypes.c_void_p]
    lib.codes_handle_delete.restype = ctypes.c_int
    handle = lib.codes_handle_new_from_samples(None, b'regular_ll_sfc_grib2')
    pilot.check(handle, 'ecCodes regular_ll_sfc_grib2 sample unavailable')
    try:
        for key, value in ((b'Ni', 1024), (b'Nj', 256), (b'bitsPerValue', 24)):
            pilot.check(lib.codes_set_long(handle, key, value) == 0,
                        'cannot configure ecCodes sample grid')
        values = np.arange(1024 * 256, dtype=np.float64) / 32.0
        pilot.check(lib.codes_set_double_array(handle, b'values',
                    values.ctypes.data_as(ctypes.POINTER(ctypes.c_double)), values.size) == 0,
                    'cannot encode ecCodes sample values')
        pointer, length = ctypes.c_void_p(), ctypes.c_size_t()
        pilot.check(lib.codes_get_message(handle, ctypes.byref(pointer),
                                         ctypes.byref(length)) == 0,
                    'cannot obtain encoded ecCodes message')
        message = ctypes.string_at(pointer, length.value)
        pilot.check(message[:4] == b'GRIB' and message[-4:] == b'7777',
                    'ecCodes generated an invalid message')
        return message, {'kind': 'eccodes_sample', 'sample': 'regular_ll_sfc_grib2',
                         'grid': [256, 1024], 'edition': 2}
    finally:
        lib.codes_handle_delete(handle)


def table_from_response(response):
    with pa.memory_map(response['path'], 'r') as stream:
        # Retain heap bytes, not mapped Arrow buffers that keep the mount busy
        # until the last catalog table or exception traceback is destroyed.
        encoded = stream.read()
    return pa.ipc.open_stream(pa.BufferReader(encoded)).read_all()


def selector(source):
    return {'paths': [{'kind': 'exact', 'path': source['logical_path']}],
            'versions': {'kind': 'exact', 'exact': source['version']}}


def start_mount(daemon, mount_path, env, socket_path):
    """Keep the process handle available even when startup fails."""
    mount_path.mkdir()
    log = (mount_path.parent / 'mount.log').open('wb')
    process = subprocess.Popen([str(daemon), '-f', '-o',
        'attr_timeout=0,entry_timeout=0', str(mount_path)], env=env,
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 90
        while not (os.path.ismount(mount_path) and socket_path.exists()):
            pilot.check(process.poll() is None, 'mount exited; see mount.log')
            pilot.check(time.monotonic() < deadline, 'mount startup timed out')
            time.sleep(.1)
        return process, log
    except BaseException:
        if not os.path.ismount(mount_path) and process.poll() is None:
            process.terminate()
        pilot.unmount(mount_path, process, log)
        raise


def arrow_checks(socket_path, source, expected):
    observations = []
    for name, lo, hi in (('head', 0, 65), ('one_mib', 262000, 262300),
                         ('eight_mib', 2097000, 2097300),
                         ('tail', len(expected) - 65, len(expected))):
        response, elapsed = pilot.control(socket_path, {'kind': 'typed', 'request': {
            'sources': selector(source), 'subset': {
                'projection': ['index', 'value'],
                'physical_array': {'field_id': 'target', 'record_batch_index': 0,
                                   'row_index': 0, 'values': [{'child_indices': []}]},
                'slices': [{'dimension': 'index',
                    'lower': {'kind': 'inclusive', 'value': {'type': 'uint64', 'value': lo}},
                    'upper': {'kind': 'exclusive', 'value': {'type': 'uint64', 'value': hi}}}]}}})
        table = table_from_response(response)
        pilot.check(table.num_rows == hi - lo and table.column('value').null_count == 0,
                    'Arrow typed result shape/null mismatch')
        pilot.check(np.array_equal(table.column('index').to_numpy(), np.arange(lo, hi)) and
                    np.array_equal(table.column('value').to_numpy(), expected[lo:hi]),
                    'Arrow typed values differ at ' + name)
        observations.append({'case': name, 'begin': lo, 'end': hi,
                             'rows': table.num_rows, 'registration_ns': elapsed})
    return observations


def grib_checks(socket_path, source, message, repetitions):
    crossing = (8 * MIB) // len(message)
    indices = sorted({0, crossing, repetitions - 1})
    observations = []
    for index in indices:
        offset = index * len(message)
        response, elapsed = pilot.control(socket_path, {'kind': 'typed', 'request': {
            'sources': selector(source), 'subset': {
                'projection': ['grib.message.bytes'], 'output': {'mode': 'binary_view'},
                'predicate': {'kind': 'compare', 'field': 'grib.message.offset',
                    'operator': 'eq', 'value': {'type': 'uint64', 'value': offset}}}}})
        table = table_from_response(response)
        pilot.check(table.num_rows == 1 and table.num_columns == 1 and
                    table.column(0)[0].as_py() == message,
                    'GRIB exact message bytes differ')
        observations.append({'message_index': index, 'offset': offset,
                             'bytes': len(message), 'registration_ns': elapsed})
    return observations


def main(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    raw_dir = output / 'raw'
    raw_dir.mkdir()
    arrow_values = np.arange(3_000_003, dtype=np.float32)
    target = pa.ListArray.from_arrays(pa.array([0, len(arrow_values)], type=pa.int32()),
                                     pa.array(arrow_values))
    batch = pa.record_batch([target], names=['target'])
    arrow = raw_dir / 'large-stream.arrow'
    with pa.OSFile(str(arrow), 'wb') as stream:
        with pa.ipc.new_stream(stream, batch.schema) as writer:
            writer.write_batch(batch)
    message, grib_generation = make_grib_message(args.grib_raw)
    repetitions = math.ceil((9 * MIB + 123) / len(message))
    pilot.check(2 <= repetitions <= 65536, 'use a GRIB message smaller than 9 MiB')
    grib = raw_dir / 'large.grib'
    with grib.open('wb') as stream:
        for _ in range(repetitions):
            stream.write(message)
    tiny, empty = raw_dir / 'tiny.txt', raw_dir / 'empty'
    tiny.write_bytes(b'abc')
    empty.write_bytes(b'')
    pilot.check(arrow.stat().st_size > 8 * MIB and grib.stat().st_size > 8 * MIB,
                'large fixtures must cross the batch boundary')
    prefix = '/publication-batch-smoke-' + uuid.uuid4().hex[:12]
    sources = [{'raw': str(path), 'logical_path': prefix + '/' + path.name,
                'bytes': path.stat().st_size, 'sha256': pilot.sha(path),
                'expected_format': fmt} for path, fmt in
               ((arrow, 'arrow'), (grib, 'grib'), (tiny, None), (empty, None))]
    env = pilot.connection_environment(args.source_pid, args.database)
    env['IOTDBFS_NAMESPACE_LOCK'] = str(args.writer_lock.resolve())
    env['IOTDBFS_PUBLICATION_PROFILE_JSONL'] = str(output / 'publication-profile.jsonl')
    env['IOTDBFS_QUERY_PROFILE_JSONL'] = str(output / 'query-profile.jsonl')
    socket_path = Path('/tmp') / ('publication-smoke-' + uuid.uuid4().hex[:12] + '.sock')
    env['IOTDBFS_QUERY_SOCKET'] = str(socket_path)
    config = output / 'profile.properties'
    config.write_text(f'writer.transfer_chunk_bytes={args.transfer_bytes}\n'
                      'writer.manifest_batch_max_bytes=8388608\n'
                      'cache.read.full_prefetch_max_bytes=1\ncache.raw_file.max_bytes=1\n')
    env['IOTDB_CONFIG_FILE'] = str(config)
    os.environ['GEOS_FUSE_OPTIONS'] = 'attr_timeout=0,entry_timeout=0'
    report = {'protocol': 'iotdbfs-publication-batch-format-smoke/1', 'complete': False,
              'database': args.database, 'prefix': prefix, 'sources': sources,
              'transfer_bytes': args.transfer_bytes,
              'daemon_sha256': pilot.sha(args.daemon), 'driver_sha256': pilot.sha(Path(__file__)),
              'payload_verifier_sha256': pilot.sha(args.payload_verifier),
              'grib_generation': grib_generation, 'grib_messages': repetitions,
              'payload_verification': [], 'queries': {},
              'timing': 'copyfile only; all payload verification and queries run after all writes'}
    pilot.write(output / 'report.json', report)
    mount_path = output / 'mount'
    process, log = start_mount(args.daemon.resolve(), mount_path, env, socket_path)
    try:
        (mount_path / prefix[1:]).mkdir()
        for source in sources:
            destination = mount_path / source['logical_path'].lstrip('/')
            start = time.perf_counter_ns()
            shutil.copyfile(source['raw'], destination)
            source['publish_ns'] = time.perf_counter_ns() - start
            pilot.check(destination.stat().st_size == source['bytes'], 'published size differs')
            pilot.write(output / 'report.json', report)
        # Catalog captures immutable source versions, including pass-through files.
        for source in sources:
            response, _ = pilot.control(socket_path, {'kind': 'typed', 'request': {
                'sources': {'paths': [{'kind': 'exact', 'path': source['logical_path']}],
                            'versions': {'kind': 'latest'}}}})
            table = table_from_response(response)
            pilot.check(table.num_rows == 1, 'file catalog must identify one source')
            row = table.to_pylist()[0]
            pilot.check(row['source.path'] == source['logical_path'] and
                        row['source.payload_size'] == source['bytes'] and
                        row['source.format'] == source['expected_format'],
                        'format/source catalog differs')
            source['version'] = row['source.version']
            source['layout_size'] = row['source.layout_size']
            pilot.check((source['layout_size'] > 0) == (source['expected_format'] is not None),
                        'unexpected format layout size')
            completed = subprocess.run([str(args.payload_verifier.resolve()),
                source['logical_path'], str(source['version']), str(source['bytes']),
                source['sha256']], env=env, text=True, capture_output=True, timeout=300)
            (output / (Path(source['raw']).name + '.verify.log')).write_text(
                completed.stdout + completed.stderr)
            pilot.check(completed.returncode == 0, 'full payload verifier failed; see verifier log')
            verified = json.loads(completed.stdout.strip())
            pilot.check(verified['ok'] and verified['bytes'] == source['bytes'] and
                        verified['sha256'] == source['sha256'] and
                        verified['version'] == source['version'] and
                        verified['path'] == source['logical_path'], 'payload verifier identity differs')
            report['payload_verification'].append(verified)
            pilot.write(output / 'report.json', report)
        pilot.check(os.getxattr(mount_path / sources[0]['logical_path'].lstrip('/'),
                                'user.arrow.container') == b'stream', 'Arrow stream not registered')
        report['queries']['arrow'] = arrow_checks(socket_path, sources[0], arrow_values)
        report['queries']['grib'] = grib_checks(socket_path, sources[1], message, repetitions)
        # EOF is a filesystem property; verify it on every precise raw-version path.
        for source in sources:
            logical = Path(source['logical_path'])
            exact_name = '.' + logical.stem + '_' + str(source['version']) + logical.suffix
            exact = mount_path / str(logical.parent).lstrip('/') / exact_name
            with exact.open('rb') as stream:
                stream.seek(source['bytes'])
                pilot.check(stream.read(1) == b'', 'raw version has data beyond declared EOF')
            source['exact_version_eof'] = True
    finally:
        try:
            pilot.unmount(mount_path, process, log)
        finally:
            pilot.write(output / 'report.json', report)
    report['complete'] = True
    pilot.write(output / 'report.json', report)
    print(json.dumps({'complete': True, 'report': str(output / 'report.json'), 'files': len(sources)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--daemon', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-pid', type=int, required=True)
    parser.add_argument('--writer-lock', type=Path, required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--payload-verifier', type=Path, required=True)
    parser.add_argument('--grib-raw', type=Path)
    parser.add_argument('--transfer-bytes', type=int, default=MIB,
                        choices=(MIB, 2*MIB, 4*MIB, 8*MIB))
    main(parser.parse_args())
