#!/usr/bin/env python3
"""Repeat the existing GEOS publication workload with scoped SQL diagnostics."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
import subprocess

import geos_pilot
from geos_publication_profile_validate import TRANSFER_BYTES, validate_transfer_configuration


def sha(path):
    return geos_pilot.sha(path)


def main(args):
    transfer_bytes = 1048576 if args.transfer_bytes is None else args.transfer_bytes
    validate_transfer_configuration(transfer_bytes, args.manifest_batch_bytes)
    baseline = json.loads(args.baseline_pilot.read_text())
    geos_pilot.check(baseline['complete'] and len(baseline['sources']) == 16,
                     'expected the frozen 16-file baseline')
    total = sum(source['bytes'] for source in baseline['sources'])
    for source in baseline['sources']:
        geos_pilot.check(Path(source['raw']).stat().st_size == source['bytes'], 'raw size changed')
    geos_pilot.check(shutil.disk_usage(args.output.parent).free > 3 * total, 'insufficient space')
    args.output.mkdir(exist_ok=False)
    trace = args.output.resolve() / 'publication-profile.jsonl'
    os.environ['IOTDBFS_PUBLICATION_PROFILE_JSONL'] = str(trace)
    os.environ['GEOS_FUSE_OPTIONS'] = 'attr_timeout=0,entry_timeout=0'
    provenance = {
        'protocol': 'iotdbfs-geos-publication-measurement/1',
        'baseline_pilot': str(args.baseline_pilot.resolve()),
        'baseline_sha256': sha(args.baseline_pilot), 'daemon': str(args.daemon.resolve()),
        'daemon_sha256': sha(args.daemon), 'driver_sha256': sha(Path(geos_pilot.__file__)),
        'wrapper_sha256': sha(Path(__file__)), 'database': baseline['database'],
        'source_pid': args.source_pid, 'writer_lock': str(args.writer_lock.resolve()),
        'raw_bytes': total, 'files': 16, 'validation_passes': 1,
        'trace': str(trace), 'fuse_options': os.environ['GEOS_FUSE_OPTIONS'],
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'python': sys.executable, 'pyarrow_site': os.environ.get('GEOS_PYARROW_SITE'),
        'timing': 'unchanged sequential copyfile intervals; scoped API timings include session acquisition and client RPC wait',
        'manifest_batch_bytes': args.manifest_batch_bytes,
        'transfer_bytes': transfer_bytes,
        'transfer_bytes_override': args.transfer_bytes,
    }
    geos_pilot.write(args.output / 'provenance.json', provenance)
    shutil.copyfile(args.baseline_pilot, args.output / 'baseline-pilot.json')
    original_mount = geos_pilot.mount
    def configured_mount(daemon, path, env, socket_path):
        # The shared driver has already written its unchanged query settings.
        # Set explicit ingestion overrides only; omitted transfer preserves the
        # historical 1 MiB driver and older daemon configuration.
        if args.manifest_batch_bytes is not None or args.transfer_bytes is not None:
            with Path(env['IOTDB_CONFIG_FILE']).open('a') as config:
                if args.manifest_batch_bytes is not None:
                    config.write(f'writer.manifest_batch_max_bytes={args.manifest_batch_bytes}\n')
                if args.transfer_bytes is not None:
                    config.write(f'writer.transfer_chunk_bytes={args.transfer_bytes}\n')
        return original_mount(daemon, path, env, socket_path)
    geos_pilot.mount = configured_mount
    try:
        geos_pilot.main(argparse.Namespace(output=args.output / 'run-01', daemon=args.daemon,
                                         database=baseline['database'], source_pid=args.source_pid,
                                         writer_lock=args.writer_lock, passes=1,
                                         raw=[Path(s['raw']) for s in baseline['sources']]))
    finally:
        geos_pilot.mount = original_mount
    result = json.loads((args.output / 'run-01/pilot.json').read_text())
    geos_pilot.check(result['complete'] and len(result['observations']) == 64, 'validation incomplete')
    for old, new in zip(baseline['sources'], result['sources']):
        geos_pilot.check((old['raw'], old['sha256'], old['bytes']) ==
                         (new['raw'], new['sha256'], new['bytes']), 'raw identity changed')
    if args.payload_verifier:
        env = geos_pilot.connection_environment(args.source_pid, baseline['database'])
        verified = []
        for source in result['sources']:
            completed = subprocess.run([str(args.payload_verifier.resolve()),
                source['logical_path'], str(source['version']), str(source['bytes']),
                source['sha256']], env=env, text=True, capture_output=True, timeout=300)
            geos_pilot.check(completed.returncode == 0, 'full payload verification failed')
            rows = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith('{')]
            geos_pilot.check(len(rows) == 1 and rows[0]['ok'], 'invalid verification response')
            verified.extend(rows)
        geos_pilot.write(args.output / 'payload-verification.json', verified)
    provenance.update(complete=True, publish_ns=sum(s['publish_ns'] for s in result['sources']),
                      finished_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    geos_pilot.write(args.output / 'provenance.json', provenance)
    print(json.dumps({'complete': True, 'publish_s': provenance['publish_ns'] / 1e9, 'trace': str(trace)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', type=Path, required=True)
    parser.add_argument('--baseline-pilot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-pid', type=int, required=True)
    parser.add_argument('--writer-lock', type=Path, required=True)
    parser.add_argument('--manifest-batch-bytes', type=int, choices=(1048576, 8388608),
                        help='set the manifest checkpoint budget independently of transfer size')
    parser.add_argument('--transfer-bytes', type=int, choices=TRANSFER_BYTES,
                        help='explicit OBJECT transfer size; omitted preserves the 1 MiB default')
    parser.add_argument('--payload-verifier', type=Path,
                        help='verify every exact version in full after all timed publications and queries')
    main(parser.parse_args())
