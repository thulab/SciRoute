#!/usr/bin/env python3
"""Isolate sample versus batch client conversion on identical Electricity traces."""
import argparse
from pathlib import Path
from run_cache_parallel_ablation import Mount, save, train
from compare_public_ab import compare
from summarize_query_profile import read_server_log


from run_prepared_ablation import profile_run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir', 'mount', 'socket', 'binary', 'fallback', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--daemon-pid', type=int, required=True)
    parser.add_argument('--source-version', type=int, required=True)
    parser.add_argument('--gpu', default='0')
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--arms', nargs='+', choices=('sample', 'batch'), default=['sample', 'batch'])
    parser.add_argument('--temperatures', nargs='+', choices=('cold', 'warm'), default=['cold', 'warm'])
    args = parser.parse_args()
    if args.temperatures not in (['cold'], ['cold', 'warm']):
        parser.error('warm requires a preceding cold run')
    args.output.mkdir(parents=True, exist_ok=False)
    mount = Mount(args)
    settings = {'query.cache.max_entries': 16384, 'query.cache.max_inflight': 256,
                'query.cache.worker_threads': 4, 'query.cache.memory_budget_bytes': 1073741824,
                'query.training.read_parallelism': 4}
    base = '\n'.join(line for line in Path(mount.original_config).read_text().splitlines()
                     if line.split('=', 1)[0].strip() not in settings)
    config = args.output / 'experiment.properties'
    config.write_text(base + '\n' + '\n'.join(f'{k}={v}' for k,v in settings.items()) + '\n')
    config.chmod(0o600)
    results = []
    try:
        for repetition in range(1, args.repetitions + 1):
            for model in ('patchtst', 'patchtsmixer'):
                local = train(args, model, 'local', f'r{repetition}-{model}-local')
                arms = tuple(args.arms)
                for arm in arms if repetition % 2 else reversed(arms):
                    args.registration_mode = "prepared_compact"
                    args.conversion_mode = arm
                    tag = f'r{repetition}-{model}-{arm}'
                    profile = args.output / f'{tag}-profile.jsonl'
                    mount.start(config, profile, tag)
                    for temperature in args.temperatures:
                        run = f'{tag}-{temperature}'
                        report = train(args, model, 'iotdbfs', run)
                        if report['conversion_mode'] != arm or report['registration_mode'] != 'prepared_compact' or any(m['conversion_mode'] != arm for m in report['backend_metrics'].values()):
                            raise RuntimeError('client configuration mismatch')
                        comparison = compare(local, report)
                        save(args.output / f'{run}-comparison.json', comparison)
                        if not comparison['passed']:
                            raise RuntimeError(f'correctness failure: {run}')
                        summary, detail = profile_run(report, read_server_log(profile))
                        save(args.output / f'{run}-summary.json', summary)
                        for item in summary['requests']:
                            server = item['server']
                            if server['runtime_config']['max_entries'] != 16384 or server['server_training_execution']['read_parallelism'] != 4:
                                raise RuntimeError('configuration mismatch')
                        results.append(dict(run=run, model=model, arm=arm, repetition=repetition,
                            temperature=temperature, passed=True, train_wall_seconds=report['train_wall_seconds'],
                            local_train_wall_seconds=local['train_wall_seconds'],
                            train_fetch_seconds=report['fetch_seconds']['train'],
                            local_train_fetch_seconds=local['fetch_seconds']['train'],
                            ipc_bytes=report['backend_metrics']['train']['ipc_bytes'], **detail))
                        save(args.output / 'results.json', results)
    finally:
        try:
            mount.start(mount.original_config, None, 'restored')
        except Exception:
            mount.start(mount.original_config, None, 'fallback', args.fallback)
        print(f'RESTORED daemon pid={mount.pid}, original configuration, profile off', flush=True)


if __name__ == '__main__':
    main()
