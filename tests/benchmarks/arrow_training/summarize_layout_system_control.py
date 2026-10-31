#!/usr/bin/env python3
"""Validate and summarize the paired Arrow layout/storage experiment.

The independent observation is a repetition: warm passes are averaged within
that repetition before any across-repetition statistics or paired contrasts.
Timers are reported at their recorded boundaries, never summed across nested
client/server/worker scopes. No significance or confidence claims are inferred.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import statistics

from summarize_query_profile import PREPARE, STAGES, read_server_log

LAYOUTS = ('original', 'cm8192', 'cm128', 'sm128')
ARMS = {(layout, mode) for layout in LAYOUTS for mode in ('local', 'read', 'iotdb')}
ARMS |= {('original', 'typed'), ('sm128', 'typed')}
DIRECT_ZERO = ('read_calls', 'read_ranges', 'read_bytes', 'batch_decodes', 'decoded_bytes')
TYPED_ZERO = ('read_bytes', 'read_ranges', 'payload_read_calls', 'decode_count', 'decoded_bytes')


def number(value, name, *, positive=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f'invalid {name}: expected finite {"positive" if positive else "nonnegative"} number')
    return value


def distribution(values):
    return dict(count=len(values), mean=statistics.mean(values), min=min(values), max=max(values),
                by_repetition=list(values))


def numeric_metrics(row):
    result = {key: number(row[key], key) for key in
              ('wall_seconds', 'setup_seconds', 'raw_window_seconds', 'normalization_seconds',
               'preparation_seconds') if key in row}
    for section in ('metrics', 'server_seconds', 'runtime_delta', 'preparation_server_seconds'):
        for key, value in row.get(section, {}).items():
            if type(value) in (int, float):
                result[f'{section}.{key}'] = number(value, f'{section}.{key}')
    return result



def enrich_preparation_profiles(rows, directory):
    """Join the separate initial window_prepare record without changing raw rows."""
    directory = Path(directory)
    enriched = copy.deepcopy(rows)
    logs = {}
    stages = set(STAGES)
    for row in enriched:
        if row['mode'] != 'typed':
            continue
        tag = row['tag']
        if Path(tag).name != tag:
            raise ValueError('invalid profile tag')
        if tag not in logs:
            logs[tag] = read_server_log(directory / (tag + '-profile.jsonl'))
            for record in logs[tag]:
                if record.get('operation') == 'window_prepare':
                    stages.update(record.get('stages_ns', {}))
    for row in enriched:
        if row['mode'] != 'typed':
            continue
        timings = row['metrics']['preparation_timings']
        expected = 1 if row['pass_index'] == 0 else 0
        if len(timings) != expected:
            raise ValueError('expected exactly one cold preparation and no warm preparation')
        values = {name: 0. for name in sorted(stages)}
        values.update(server_total=0., prepare_total=0.)
        if timings:
            prepared_id = timings[0].get('prepared_id')
            if not prepared_id:
                raise ValueError('preparation timing has no prepared_id')
            records = logs[row['tag']]
            matches = [record for record in records if record.get('operation') == 'window_prepare'
                       and record.get('prepared_id') == prepared_id]
            if len(matches) != 1:
                raise ValueError('missing or ambiguous preparation profile')
            record = matches[0]
            if record.get('ok') is not True:
                raise ValueError('preparation profile failed')
            if any(other is not record and other['pid'] == record['pid'] and
                   other['steady_start_ns'] < record['steady_end_ns'] and
                   record['steady_start_ns'] < other['steady_end_ns'] for other in records):
                raise ValueError('preparation profile overlaps another request')
            values.update({name: record.get('stages_ns', {}).get(name, 0) / 1e9 for name in stages})
            values['server_total'] = record['server_total_ns'] / 1e9
            values['prepare_total'] = sum(record.get('stages_ns', {}).get(name, 0) for name in PREPARE) / 1e9
        row['preparation_server_seconds'] = values
    return enriched


def summarize(rows, repetitions=5, warm_passes=3):
    if type(repetitions) is not int or repetitions <= 0 or type(warm_passes) is not int or warm_passes <= 0:
        raise ValueError('repetitions and warm_passes must be positive integers')
    indexed = {}
    for row in rows:
        if row.get('passed') is not True:
            raise ValueError('failed correctness observation')
        rep, repeat = row['repetition'], row['pass_index']
        if type(rep) is not int or not 0 <= rep < repetitions:
            raise ValueError('unexpected repetition')
        if type(repeat) is not int or not 0 <= repeat <= warm_passes:
            raise ValueError('unexpected pass_index')
        arm = (row['layout'], row['mode'])
        if arm not in ARMS:
            raise ValueError(f'unexpected arm {arm}')
        key = (*arm, rep, repeat)
        if key in indexed:
            raise ValueError(f'duplicate arm/repetition/pass {key}')
        expected_temperature = 'cold' if repeat == 0 else 'warm'
        if row['temperature'] != expected_temperature:
            raise ValueError('temperature disagrees with pass_index')
        wall = number(row['wall_seconds'], 'wall_seconds', positive=True)
        number(row['setup_seconds'], 'setup_seconds')
        times = row['request_seconds']
        if len(times) != 51:
            raise ValueError('requires 51 timed requests per pass')
        for value in times:
            number(value, 'request_seconds')
        if not math.isclose(sum(times), wall, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError('request_seconds do not sum to wall_seconds')
        typed = row['mode'] == 'typed'
        counters = row['runtime_delta'] if typed else row['metrics']
        for field in TYPED_ZERO if typed else DIRECT_ZERO:
            value = number(counters[field], field)
            if repeat > 0 and value != 0:
                raise ValueError(f'warm pass has actual reads or decodes: {field}')
        if typed:
            prep = number(row['preparation_seconds'], 'preparation_seconds')
            if not math.isclose(prep, row['metrics']['preparation_seconds'], rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError('preparation_seconds disagrees with metrics')
            if repeat > 0 and prep != 0:
                raise ValueError('warm typed pass repeated preparation')
            if row['metrics']['requests'] != 51 or row['metrics']['samples'] != 102:
                raise ValueError('typed pass has unexpected request/sample count')
            if len(row['metrics']['request_timings']) != 51:
                raise ValueError('typed profile has unexpected request count')
        else:
            if row['metrics']['read_windows_calls'] != 51:
                raise ValueError('reader has unexpected request count')
            raw = number(row['raw_window_seconds'], 'raw_window_seconds')
            normalization = number(row['normalization_seconds'], 'normalization_seconds')
            if not math.isclose(raw + normalization, wall, rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError('direct read/normalization timers do not sum to wall_seconds')
        numeric_metrics(row)
        indexed[key] = row
    expected = {(*arm, rep, repeat) for arm in ARMS for rep in range(repetitions)
                for repeat in range(warm_passes + 1)}
    missing = expected - indexed.keys()
    if missing:
        raise ValueError(f'incomplete experiment: {len(missing)} missing observations; first {sorted(missing)[0]}')

    groups = []
    values = {}
    for layout, mode in sorted(ARMS):
        for temperature in ('cold', 'warm'):
            passes = [0] if temperature == 'cold' else list(range(1, warm_passes + 1))
            per_rep = []
            for rep in range(repetitions):
                observations = [numeric_metrics(indexed[layout, mode, rep, repeat]) for repeat in passes]
                keys = observations[0].keys()
                if any(item.keys() != keys for item in observations[1:]):
                    raise ValueError('inconsistent metric fields within a repetition')
                per_rep.append({key: statistics.mean(item[key] for item in observations) for key in keys})
            if any(item.keys() != per_rep[0].keys() for item in per_rep[1:]):
                raise ValueError('inconsistent metric fields across repetitions')
            stats = {key: distribution([item[key] for item in per_rep]) for key in sorted(per_rep[0])}
            groups.append(dict(layout=layout, mode=mode, temperature=temperature,
                               raw_observations=repetitions * len(passes), independent_repetitions=repetitions,
                               metrics=stats))
            values[layout, mode, temperature] = stats['wall_seconds']['by_repetition']

    def contrast(name, left, right, *, operation='difference'):
        if operation == 'ratio':
            result = [a / b for a, b in zip(left, right)]
        else:
            result = [a - b for a, b in zip(left, right)]
        return dict(name=name, operation=operation, **distribution(result))

    factorial = {}
    auxiliary = {}
    production = {}
    for temperature in ('cold', 'warm'):
        a, b, c, d = [values[layout, mode, temperature] for layout, mode in
                      [('original', 'local'), ('sm128', 'local'), ('original', 'iotdb'), ('sm128', 'iotdb')]]
        factorial[temperature] = [
            contrast('local_layout_saving_seconds_A_minus_B', a, b),
            contrast('iotdb_layout_saving_seconds_C_minus_D', c, d),
            contrast('local_layout_speedup_A_over_B', a, b, operation='ratio'),
            contrast('iotdb_layout_speedup_C_over_D', c, d, operation='ratio'),
            contrast('original_backend_penalty_seconds_C_minus_A', c, a),
            contrast('derived_backend_penalty_seconds_D_minus_B', d, b),
            contrast('original_backend_ratio_C_over_A', c, a, operation='ratio'),
            contrast('derived_backend_ratio_D_over_B', d, b, operation='ratio'),
            contrast('interaction_seconds_C_minus_D_minus_A_plus_B',
                     [x-y for x,y in zip(c,d)], [x-y for x,y in zip(a,b)])]
        controls = []
        for mode in ('local', 'read', 'iotdb'):
            for left, right, label in [('original', 'cm8192', 'batch_packing'),
                                       ('cm8192', 'cm128', 'block_size'), ('cm128', 'sm128', 'orientation')]:
                controls.append(contrast(f'{mode}_{label}_saving_seconds', values[left,mode,temperature],
                                         values[right,mode,temperature]))
        for layout in LAYOUTS:
            controls.append(contrast(f'{layout}_read_minus_mmap_seconds', values[layout,'read',temperature],
                                     values[layout,'local',temperature]))
        auxiliary[temperature] = controls
        production[temperature] = [
            contrast('typed_layout_saving_seconds', values['original','typed',temperature],
                     values['sm128','typed',temperature])]
        for layout in ('original', 'sm128'):
            production[temperature].append(contrast(f'{layout}_typed_minus_direct_iotdb_seconds',
                values[layout,'typed',temperature], values[layout,'iotdb',temperature]))
    return dict(protocol='iotdbfs-layout-system-control-summary/1', passed=True,
        observations=len(rows), repetitions=repetitions, warm_passes=warm_passes,
        boundaries=dict(independent_unit='repetition; average all warm passes within repetition before contrasts',
            ranges='min/max of independent repetition values, not request-level or raw warm-pass extrema',
            contrasts='paired by repetition; ratio is mean of paired ratios, not ratio of aggregate means',
            cache='application cache cold; OS/IoTDB caches retained; warm actual reads and decodes verified zero',
            setup='reader setup outside wall; reported per reader, never multiplied by warm passes',
            preparation='typed preparation_seconds is inside cold wall; do not add it to wall',
            server='server_seconds covers window registration requests only, excluding the initial window_prepare; its server_total contains prepare_total and publish_route; publish_route contains window_execute, query_authorize, ipc_encode and result_publish',
            initial_prepare='preparation_server_seconds covers the separate window_prepare request, joined by prepared_id; server_total contains prepare_total, which contains disjoint prepare stages; nested detail timers must not be added to parents',
            execution='window_execute contains window_* and scheduler_* substage timers',
            client='registration includes synchronous server work; client decode includes validation/array details',
            workers='runtime nanoseconds are potentially concurrent worker durations; not additive to wall',
            statistics='descriptive means and ranges only; no statistical significance inferred',
            production='typed minus direct is a difference between complete implementations, not intrinsic IoTDB/FUSE overhead'),
        groups=groups, factorial=factorial, auxiliary=auxiliary, production=production)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--warm-passes', type=int, default=3)
    parser.add_argument('--profile-dir', type=Path, help='join separate initial window_prepare server profiles')
    args = parser.parse_args()
    rows = json.loads(args.input.read_text())
    if args.profile_dir is not None:
        rows = enrich_preparation_profiles(rows, args.profile_dir)
    result = summarize(rows, args.repetitions, args.warm_passes)
    result['inputs'] = dict(results=str(args.input), profile_dir=str(args.profile_dir) if args.profile_dir else None)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(passed=True, observations=result['observations'],
                          repetitions=result['repetitions'], output=str(args.output))))


if __name__ == '__main__':
    main()
