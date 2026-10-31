#!/usr/bin/env python3
"""Paired full/prepared/compact Electricity runs, including first preparation."""
import argparse
import json
from pathlib import Path
from run_cache_parallel_ablation import Mount, save, train
from compare_public_ab import compare
from summarize_query_profile import client_requests, read_server_log, summarize


def profile_run(report, records):
    metrics = report['backend_metrics']['train']
    requests = client_requests(report, 'train')
    summary = summarize(records, requests)
    if summary['request_count'] != 51 or summary['excluded_count']:
        raise RuntimeError('incomplete or overlapping execution profile')
    prepared = metrics.get('preparation_timings', [])
    ids = {x['prepared_id'] for x in prepared}
    preparation = [x for x in records if x.get('operation') == 'window_prepare' and x.get('prepared_id') in ids]
    release = [x for x in records if x.get('operation') == 'window_result_release' and x.get('prepared_id') in ids]
    if len(preparation) != len(prepared) or (ids and len(release) != 51):
        raise RuntimeError('incomplete prepare/release profile')
    for row in preparation + release:
        if not row['ok'] or any(other is not row and other['pid'] == row['pid'] and
                other['steady_start_ns'] < row['steady_end_ns'] and
                row['steady_start_ns'] < other['steady_end_ns'] for other in records):
            raise RuntimeError('failed or overlapping control operation')
    stages = {k: sum(x.get('stages_ns', {}).get(k, 0) for x in preparation) / 1e9
              for k in {k for x in preparation for k in x.get('stages_ns', {})}}
    transport = {'execute': requests, 'prepare': prepared, 'result_release': metrics.get('release_timings', [])}
    return summary, {
        'request_bytes': {k: sum(x['request_json_bytes'] for x in rows) for k, rows in transport.items()},
        'control_client_seconds': {k: sum(x['registration_seconds'] for x in rows) for k, rows in transport.items()},
        'first_prepare_server_seconds': sum(x['server_total_ns'] for x in preparation) / 1e9,
        'result_release_server_seconds': sum(x['server_total_ns'] for x in release) / 1e9,
        'first_prepare_stages_seconds': stages,
        'server_seconds': {k: v['sum'] for k, v in summary['server_seconds'].items()},
        'client_seconds': {k: v['sum'] for k, v in summary['client_seconds'].items()},
        'runtime_delta': {k: v['sum'] for k, v in summary['runtime_delta'].items()},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work-dir', 'mount', 'socket', 'binary', 'fallback', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--daemon-pid', type=int, required=True)
    parser.add_argument('--source-version', type=int, required=True)
    parser.add_argument('--gpu', default='0')
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--arms', nargs='+', choices=('full', 'prepared', 'prepared_compact'), default=['full', 'prepared', 'prepared_compact'])
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
                    args.registration_mode = arm
                    tag = f'r{repetition}-{model}-{arm}'
                    profile = args.output / f'{tag}-profile.jsonl'
                    mount.start(config, profile, tag)
                    for temperature in args.temperatures:
                        run = f'{tag}-{temperature}'
                        report = train(args, model, 'iotdbfs', run)
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
                            local_train_wall_seconds=local['train_wall_seconds'], **detail))
                        save(args.output / 'results.json', results)
    finally:
        try:
            mount.start(mount.original_config, None, 'restored')
        except Exception:
            mount.start(mount.original_config, None, 'fallback', args.fallback)
        print(f'RESTORED daemon pid={mount.pid}, original configuration, profile off', flush=True)


if __name__ == '__main__':
    main()
