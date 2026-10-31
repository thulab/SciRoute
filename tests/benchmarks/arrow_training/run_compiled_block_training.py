#!/usr/bin/env python3
"""Production 64/128-block training validation with cold daemon and warm replay.

Cold means empty application fragment cache, not cold OS/database storage.
Every arm retains production registration, authorization, scheduling and IPC.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from compare_public_ab import compare
from run_cache_parallel_ablation import Mount


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir', 'manifest', 'trace', 'mount', 'socket', 'binary', 'fallback', 'output'):
        p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--daemon-pid', type=int, required=True)
    p.add_argument('--repetitions', type=int, default=2)
    p.add_argument('--models', nargs='+', default=['patchtst'])
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    scripts = Path(__file__).parent
    settings = {'query.cache.max_entries': 16384, 'query.cache.max_inflight': 256,
        'query.cache.worker_threads': 4, 'query.cache.memory_budget_bytes': 1073741824,
        'query.training.read_parallelism': 4, 'query.training.reuse_prepared_validation': 'true',
        'query.training.compiled_transform': 'true'}
    config = args.output / 'private.properties'
    config.write_text('\n'.join(line for line in Path(mount.original_config).read_text().splitlines()
        if line.split('=', 1)[0].strip() not in settings) + '\n' +
        '\n'.join(f'{k}={v}' for k, v in settings.items()) + '\n')
    config.chmod(0o600)
    results = []
    def execute(command, tag):
        with (args.output / (tag + '.log')).open('w') as log:
            subprocess.run(command, check=True, env=dict(os.environ, CUDA_VISIBLE_DEVICES='0'),
                stdout=log, stderr=subprocess.STDOUT, timeout=600)
        return json.loads((args.output / (tag + '.json')).read_text())
    def outputs(tag):
        return ['--output-dir', str(args.output / 'runs' / tag), '--report', str(args.output / (tag + '.json'))]
    try:
        for rep in range(args.repetitions):
            for model in args.models:
                tag = f'r{rep}-{model}-original-local'
                local = execute([sys.executable, str(scripts / 'train_hf.py'), '--work-dir', str(args.work_dir),
                    '--dataset', 'UCI_Electricity_370', '--backend', 'local', '--model', model,
                    '--max-steps', '50', '--batch-size', '2', '--eval-windows', '4', '--seed', '20260929'] + outputs(tag), tag)
                results.append(dict(tag=tag, train_wall_seconds=local['train_wall_seconds'],
                    fetch_seconds=local['fetch_seconds']['train'], passed=True))
                blocks = [128, 64] if rep % 2 == 0 else [64, 128]
                for block in blocks:
                    arm = f'r{rep}-{model}-sm{block}'
                    mount.start(config, args.output / (arm + '-profile.jsonl'), arm)
                    for warm in (False, True):
                        tag = arm + ('-warm' if warm else '-cold')
                        command = [sys.executable, str(scripts / 'train_electricity_derived.py'),
                            '--work-dir', str(args.work_dir), '--manifest', str(args.manifest),
                            '--mount', str(args.mount), '--socket', str(args.socket), '--helper', '/unused',
                            '--daemon-pid', str(mount.pid), '--layout', 'step_major', '--block-steps', str(block),
                            '--codec', 'lz4', '--mode', 'typed', '--model', model, '--max-steps', '50',
                            '--trace', str(args.trace)] + outputs(tag)
                        if warm:
                            command.append('--warm')
                        report = execute(command, tag)
                        check = compare(local, report)
                        (args.output / (tag + '-comparison.json')).write_text(json.dumps(check, indent=2) + '\n')
                        if not check['passed']:
                            raise ValueError(f'{tag}: {check["differences"]}')
                        results.append(dict(tag=tag, block_steps=block, temperature=report['temperature'],
                            train_wall_seconds=report['train_wall_seconds'], fetch_seconds=report['fetch_seconds']['train'],
                            preparation_seconds=report['backend_metrics']['train']['preparation_seconds'], passed=True))
                        (args.output / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
                        print('PASS', tag, report['train_wall_seconds'], flush=True)
    finally:
        mount.start(mount.original_config, None, 'restored', args.fallback)
        (args.output / 'restored.json').write_text(json.dumps(dict(pid=mount.pid, profile=False)) + '\n')

if __name__ == '__main__':
    main()
