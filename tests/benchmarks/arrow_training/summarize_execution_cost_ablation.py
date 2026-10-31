#!/usr/bin/env python3
"""Verify and summarize the fixed five-arm execution-cost experiment."""
import argparse
import hashlib
import json
from pathlib import Path
from statistics import mean
from run_execution_cost_ablation import ARMS


def summarize(root):
    rows = json.loads((root / 'results.json').read_text())
    expected = {(model, arm, repetition, temperature)
                for model in ('patchtst', 'patchtsmixer') for arm in ARMS
                for repetition in (1, 2) for temperature in ('cold', 'warm')}
    actual = [(r['model'], r['arm'], r['repetition'], r['temperature']) for r in rows]
    assert len(actual) == len(set(actual)) and set(actual) == expected
    profiles = 0
    for r in rows:
        assert r['passed']
        assert (r['reuse_prepared_validation'], r['compiled_transform'], r['conversion_mode']) == ARMS[r['arm']]
        assert r['ipc_bytes'] == 85791200
        assert r['request_bytes'] == {'execute': 12022, 'prepare': 107320, 'result_release': 20196}
        d = r['runtime_delta']
        assert d['evictions'] == d['redecodes'] == d['decompressions'] == 0
        if r['temperature'] == 'cold':
            assert (d['payload_read_calls'], d['read_ranges'], d['read_bytes'], d['decode_count']) == (120, 5205, 109117440, 6660)
        else:
            assert d['payload_read_calls'] == d['read_ranges'] == d['read_bytes'] == d['decode_count'] == 0
        if r['reuse_prepared_validation']:
            assert r['server_seconds'].get('snapshot_fingerprint', 0) == 0
            assert r['server_seconds'].get('snapshot_view_validate', 0) == 0
        report = json.loads((root / (r['run'] + '.json')).read_text())
        assert report['source_version'] == 1790670177863
        assert report['source_sha256'] == '536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f'
        profile = json.loads((root / (r['run'] + '-summary.json')).read_text())
        assert profile['request_count'] == 51 and profile['excluded_count'] == 0
        profiles += profile['request_count']
    summary = []
    for model in ('all', 'patchtst', 'patchtsmixer'):
        for temperature in ('cold', 'warm'):
            for arm in ARMS:
                group = [r for r in rows if r['arm'] == arm and r['temperature'] == temperature and (model == 'all' or r['model'] == model)]
                reference = [r for r in rows if r['arm'] == 'baseline' and r['temperature'] == temperature and (model == 'all' or r['model'] == model)]
                wall = mean(r['train_wall_seconds'] for r in group)
                server = {k: mean(r['server_seconds'].get(k, 0) for r in group) for k in set().union(*(r['server_seconds'] for r in group))}
                client = {k: mean(r['client_seconds'].get(k, 0) for r in group) for k in set().union(*(r['client_seconds'] for r in group))}
                summary.append(dict(model=model, temperature=temperature, arm=arm, runs=len(group), wall_mean=wall,
                    wall_min=min(r['train_wall_seconds'] for r in group), wall_max=max(r['train_wall_seconds'] for r in group),
                    wall_reduction_percent=100 * (1-wall/mean(r['train_wall_seconds'] for r in reference)),
                    local_wall=mean(r['local_train_wall_seconds'] for r in group),
                    fetch_mean=mean(r['train_fetch_seconds'] for r in group),
                    first_prepare_mean=mean(r['control_client_seconds']['prepare'] for r in group),
                    server_seconds=server, client_seconds=client))
    (root/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    validation = dict(passed=True, training_runs=len(rows), local_runs=4, matched_training_profiles=profiles,
                      ipc_bytes_per_run=85791200, control_bytes_per_run=139538,
                      source_version=1790670177863,
                      cold_payload_calls=120, cold_ranges=5205, cold_bytes=109117440, cold_decodes=6660,
                      warm_payload_calls=0, evictions=0, redecodes=0,
                      results_sha256=hashlib.sha256((root/'results.json').read_bytes()).hexdigest(),
                      scope='50-step two-repeat experiment; cold decoded cache only, not cold OS/database; no factorial interaction estimate')
    (root/'validation.json').write_text(json.dumps(validation, indent=2)+'\n')
    for row in summary:
        if row['model'] == 'all':
            print(row['temperature'], row['arm'], round(row['wall_mean'], 3), round(row['wall_reduction_percent'], 1))
    print(json.dumps(validation, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    summarize(parser.parse_args().root)
