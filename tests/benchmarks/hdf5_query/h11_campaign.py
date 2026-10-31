#!/usr/bin/env python3
"""Provision isolated H11 sources and restart a private daemon per campaign.

Credentials are inherited from an explicitly selected existing daemon, never
written into configuration/evidence. Existing mounts are never changed. The
H09 fixture's restrictive database/path guards also apply to this experiment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


VARIANTS = {
    'base': (1, 1, 32, 64<<20, 'retain', 'full', 'ABCDE'),
    'clients2': (2, 1, 32, 64<<20, 'retain', 'full', 'ABCDE'),
    'clients4': (4, 1, 32, 64<<20, 'retain', 'full', 'ABCDE'),
    'parallel2': (1, 2, 32, 64<<20, 'retain', 'full', 'BD'),
    'parallel4': (1, 4, 32, 64<<20, 'retain', 'full', 'BD'),
    'single_range': (1, 1, 1, 64<<20, 'retain', 'full', 'BD'),
    'parallel2_single': (1, 2, 1, 64<<20, 'retain', 'full', 'BD'),
    'parallel4_single': (1, 4, 1, 64<<20, 'retain', 'full', 'BD'),
    'evict': (1, 1, 32, 64<<20, 'evict_before_request', 'full', 'BD'),
    'encoded': (1, 1, 32, 64<<20, 'encoded', 'full', 'BD'),
    'pressure': (1, 1, 32, 4<<20, 'retain', 'pressure', 'BD'),
    'catalog': (1, 1, 32, 64<<20, 'retain', 'catalog', 'ABCDE'),
}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--prepared', type=Path, required=True)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--source-pid', type=int, required=True)
    parser.add_argument('--existing-fixture-report', type=Path,
                        help='Use an existing isolated H09 fixture namespace with new unique paths')
    parser.add_argument('--campaigns', type=int, default=3)
    parser.add_argument('--variants', default='base,clients2,clients4,parallel2,parallel4,single_range,parallel2_single,parallel4_single,evict,encoded,pressure,catalog')
    args = parser.parse_args()
    if not 1 <= args.campaigns <= 10:
        parser.error('campaign count must be 1..10')
    selected = args.variants.split(',')
    if any(v not in VARIANTS for v in selected) or len(set(selected)) != len(selected):
        parser.error('unknown or repeated variant')
    args.root = args.root.resolve()
    args.root.mkdir(parents=True, exist_ok=False)
    args.prepared = args.prepared.resolve()
    args.build = args.build.resolve()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'arrow_training'))
    from a12_migration_smoke import connection_environment
    database = 'h09h11_' + str(int(time.time())) + '_' + uuid.uuid4().hex[:6]
    namespace_lock = args.root / 'namespace.lock'
    if args.existing_fixture_report:
        previous = json.loads(args.existing_fixture_report.read_text())
        database = previous['database']
        if not database.startswith('h09probe') or not database.replace('_', '').isalnum():
            raise ValueError('existing namespace must be an isolated H09 probe database')
        namespace_lock = Path(previous['report']) / 'namespace.lock'
    env = connection_environment(args.source_pid, database)
    env['IOTDBFS_NAMESPACE_LOCK'] = str(namespace_lock)
    runner = Path(__file__).with_name('run_h11.py')
    fixture = args.build / 'bin/h09_migration_fixture'
    prepared = json.loads(args.prepared.read_text())
    inventory = []
    report = {'protocol': 'iotdbfs-hdf5-h11-campaign/1', 'database': database,
              'runs': [], 'sources': inventory, 'complete': False,
              'existing_fixture_report': str(args.existing_fixture_report) if args.existing_fixture_report else None,
              'namespace_lock': str(namespace_lock),
              'variant_settings': {v: VARIANTS[v] for v in selected},
              'expected_runs': [{'variant': v, 'campaign': c, 'directory': f'{v}-c{c}/run',
                                 'output': str(args.root/f'{v}-c{c}'/'run')}
                                for c in range(args.campaigns) for v in selected]}
    source_prefix = '/h09-h11-' + uuid.uuid4().hex[:12] + '-'
    save(args.root / 'campaign.json', report)
    try:
        # New namespace only. Retrying initial publication is safe here: every
        # attempt reserves a distinct version and all attempts remain in logs.
        for source in prepared['sources']:
            path = source_prefix + source['id'] + '.hdf5'
            command = [str(fixture), 'seed', path, source['local_path'], 'indexed']
            result = None
            for attempt in range(3):
                start = time.perf_counter_ns()
                completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
                save(args.root / f"seed-{source['id']}-{attempt}.json", {
                    'returncode': completed.returncode, 'stdout': completed.stdout,
                    'stderr': completed.stderr, 'wall_ns': time.perf_counter_ns()-start})
                if completed.returncode == 0:
                    rows = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith('{')]
                    if len(rows) != 1:
                        raise RuntimeError('seed did not publish exactly one source')
                    result = rows[0]
                    break
                if attempt < 2:
                    time.sleep(attempt + 1)
            if result is None:
                raise RuntimeError('source seed failed; see bounded attempt logs')
            if result['sha256'] != sha(Path(source['local_path'])) or result['format_id'] != 'hdf5':
                raise RuntimeError('published source identity mismatch')
            verify = subprocess.run([str(fixture), 'verify', path, str(result['version']), source['local_path']],
                                    env=env, capture_output=True, text=True, timeout=180)
            save(args.root / f"verify-{source['id']}.json", {'returncode': verify.returncode,
                 'stdout': verify.stdout, 'stderr': verify.stderr})
            if verify.returncode:
                raise RuntimeError('exact source verification failed')
            inventory.append(dict(result, id=source['id']))
            save(args.root / 'campaign.json', report)
        for campaign in range(args.campaigns):
            # Reverse variant order too; avoid conflating a setting with run time.
            for variant in (selected if campaign % 2 == 0 else selected[::-1]):
                clients, parallel, ranges, budget, policy, scenario, paths = VARIANTS[variant]
                work = args.root / f'{variant}-c{campaign}'
                work.mkdir()
                mount = work / 'mount'
                mount.mkdir()
                socket = Path('/tmp') / f'h11-{os.getpid()}-{campaign}-{variant}.sock'
                config = work / 'profile.properties'
                config.write_text(f'query.hdf5.read_parallelism={parallel}\nquery.cache.memory_budget_bytes={budget}\n'
                    'query.cache.max_entries=65536\ncache.read.full_prefetch_max_bytes=1\ncache.raw_file.max_bytes=1\n')
                current = env.copy()
                current.update(IOTDB_CONFIG_FILE=str(config), IOTDBFS_QUERY_SOCKET=str(socket),
                               IOTDBFS_QUERY_PROFILE_JSONL=str(work/'server.jsonl'))
                process = None
                started = time.perf_counter_ns()
                try:
                    with (work/'mount.log').open('wb') as log:
                        process = subprocess.Popen([str(args.build/'bin/iotdb_fs'), '-f', str(mount)],
                                                   env=current, stdout=log, stderr=subprocess.STDOUT)
                    deadline = time.monotonic()+60
                    while not (os.path.ismount(mount) and socket.exists()):
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError('private mount failed; see mount.log')
                        time.sleep(.1)
                    deployment = {'protocol': 'iotdbfs-hdf5-h11-deployment/1', 'config': str(config),
                                  'mount': str(mount), 'socket': str(socket), 'profile': str(work/'server.jsonl'),
                                  'sources': []}
                    for item in inventory:
                        stem, extension = os.path.splitext(item['path'][1:])
                        exact = mount / f'.{stem}_{item["version"]}{extension}'
                        deployment['sources'].append({'id': item['id'], 'logical_path': item['path'],
                                                      'version': item['version'], 'fuse_path': str(exact)})
                    save(work/'deployment.json', deployment)
                    save(work/'setup.json', {'daemon_ready_ns': time.perf_counter_ns()-started,
                                            'config_sha256': sha(config), 'fresh_typed_runtime': True,
                                            'raw_whole_file_cache_limit_bytes': 1, 'prefetch_limit_bytes': 1})
                    command = [sys.executable, str(runner), 'run', '--prepared', str(args.prepared),
                        '--deployment', str(work/'deployment.json'), '--probe', str(args.build/'bin/h11_query_probe'),
                        '--output', str(work/'run'), '--campaign-offset', str(campaign), '--campaigns', '1',
                        '--clients', str(clients), '--cache-bytes', str(budget), '--cache-policy', policy,
                        '--max-call-ranges', str(ranges), '--parallelism', str(parallel),
                        '--scenario', scenario, '--paths', paths, '--fresh-runtime']
                    with (work/'runner.log').open('w') as log:
                        subprocess.run(command, env=current, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
                    report['runs'].append({'variant': variant, 'campaign': campaign, 'output': str(work/'run'), 'directory': f'{variant}-c{campaign}/run'})
                    save(args.root/'campaign.json', report)
                finally:
                    if os.path.ismount(mount):
                        subprocess.run(['fusermount', '-u', str(mount)], check=True, timeout=20)
                    if process is not None:
                        try:
                            process.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            process.terminate()
                            process.wait(timeout=20)
        report['complete'] = True
    finally:
        save(args.root/'campaign.json', report)
    print(json.dumps({'complete': True, 'runs': len(report['runs']), 'database': database}))


if __name__ == '__main__':
    main()
