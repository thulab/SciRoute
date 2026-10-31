#!/usr/bin/env python3
"""Validate A–E completeness and produce per-campaign GRIB-style summaries."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics

from run_arrow_query_paths import trimmed


def summarize(records, repeats=10, campaigns=2):
    groups = defaultdict(list)
    for row in records:
        groups[(row['dataset'], row['case'], row['arm'], row['campaign'])].append(row)
    summaries = []
    for (dataset, case, arm, campaign), rows in sorted(groups.items()):
        cold = [r for r in rows if r['temperature'] == 'cold']
        warm = [r for r in rows if r['temperature'] == 'warm']
        if len(cold) != 1 or len(warm) != repeats:
            raise ValueError(f'incomplete {dataset}/{case}/{arm}/{campaign}')
        if len({(r['digest'], r['index_digest']) for r in rows}) != 1:
            raise ValueError('inconsistent result digest')
        summary = dict(dataset=dataset, case=case, arm=arm, campaign=campaign,
                       cold_ms=cold[0]['wall_ns']/1e6,
                       warm_trimmed_ms=trimmed([r['wall_ns']/1e6 for r in warm]),
                       warm_min_ms=min(r['wall_ns']/1e6 for r in warm),
                       warm_max_ms=max(r['wall_ns']/1e6 for r in warm))
        if arm in ('B', 'C', 'E'):
            def counters(row):
                return row['server']['runtime_delta'] if arm == 'E' else row
            for temp, selected in [('cold', cold), ('warm', warm)]:
                summary[temp + '_counters_mean'] = {
                    key: statistics.mean(counters(r)[key] for r in selected)
                    for key in ('read_bytes', 'read_ranges', 'payload_read_calls',
                                'payload_read_nanoseconds', 'decode_count', 'evictions')}
            if any(counters(r)['payload_read_calls'] for r in warm):
                raise ValueError('warm query unexpectedly read payload')
        if arm == 'E':
            if len({r['path'] for r in rows}) != len(rows):
                raise ValueError('reused registration path')
            summary['encoded_bytes'] = cold[0]['encoded_bytes']
            summary['warm_server_stages_ms'] = {
                key: trimmed([r['server']['stages_ns'].get(key, 0)/1e6 for r in warm])
                for key in set().union(*(r['server']['stages_ns'] for r in warm))}
            summary['warm_client_stages_ms'] = {
                key: trimmed([r[key]/1e6 for r in warm])
                for key in ('registration_ns', 'ipc_read_ns', 'consume_ns', 'close_ns')}
        else:
            summary['warm_stages_ms'] = {key: trimmed([r[key]/1e6 for r in warm])
                                        for key in ('prepare_ns', 'execute_ns', 'consume_ns')}
            summary['cold_stages_ms'] = {key: cold[0][key]/1e6
                                        for key in ('prepare_ns', 'execute_ns', 'consume_ns')}
            summary['cold_payload_bytes'] = cold[0]['payload_bytes']
            summary['cold_layout_bytes'] = cold[0]['layout_bytes']
            summary['warm_layout_bytes'] = statistics.mean(r['layout_bytes'] for r in warm)
            summary['native_message_body_bytes'] = cold[0]['native_message_body_bytes']
        summaries.append(summary)
    cases = {(r['dataset'], r['case']) for r in records}
    for dataset, case in cases:
        comparable = [r for r in records if r['dataset'] == dataset and r['case'] == case]
        if len({(r['digest'], r['index_digest']) for r in comparable}) != 1:
            raise ValueError('cross-path mismatch')
        for campaign in range(1, campaigns+1):
            for arm in ('A', 'A-read', 'B', 'C', 'D', 'E'):
                if (dataset, case, arm, campaign) not in groups:
                    raise ValueError('missing arm/campaign')
            cold = {r['arm']: r for r in comparable
                    if r['campaign'] == campaign and r['temperature'] == 'cold'}
            for key in ('read_bytes', 'read_ranges', 'payload_read_calls', 'decode_count'):
                if cold['B'][key] != cold['C'][key] or cold['C'][key] != cold['E']['server']['runtime_delta'][key]:
                    raise ValueError(f'B/C/E cold physical work differs: {dataset}/{case}/{key}')
    return {'passed': True, 'queries': len(records), 'cases': len(cases),
            'campaigns': campaigns, 'warm_repeats_per_campaign': repeats,
            'summary': summaries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = summarize(json.loads((args.directory/'measurements.json').read_text()))
    (args.directory/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
    for dataset, case in sorted({(r['dataset'], r['case']) for r in result['summary']}):
        row = [dataset, case]
        for arm in ('A', 'A-read', 'B', 'C', 'D', 'E'):
            runs = [r for r in result['summary'] if (r['dataset'], r['case'], r['arm']) == (dataset, case, arm)]
            row.append(f"{statistics.mean(r['warm_trimmed_ms'] for r in runs):.4f}")
        print(' | '.join(row))


if __name__ == '__main__':
    main()
