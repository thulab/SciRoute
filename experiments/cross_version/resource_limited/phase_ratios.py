"""Derive discovery Rd and value/delivery Rq from sealed, existing query rows.

No workload, database access or timing changes. The original analysis and raw
records remain immutable; this is a separate, source-hashed analysis artifact.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import statistics

from experiments.identity.registry import file_sha, read_json
from .analysis import summarize


FIELDS = {
    'native-ecCodes-SQLite-index': ('discovery_ns', 'value_read_ns'),
    'native-hdf5-SQLite-index': ('discovery_client_ns', 'selected_value_client_ns'),
    'iotdb-indexed-GRIB': ('discovery_client_ns', 'selected_value_client_ns'),
    'iotdb-indexed-HDF5': ('discovery_client_ns', 'selected_value_client_ns'),
}


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def stages(row):
    need(row['engine'] in FIELDS, 'unsupported phase clock engine')
    d_key, q_key = FIELDS[row['engine']]
    for key in (d_key, q_key):
        need(key in row and type(row[key]) is int and row[key] > 0,
             'missing or nonpositive integer phase clock: '+key)
    d, q = row[d_key], row[q_key]
    need(d + q == row['total_ns'], 'phase clocks do not partition total_ns exactly')
    return d, q


def stats(values):
    return dict(count=len(values), median_ns=statistics.median(values),
                mean_ns=statistics.mean(values), min_ns=min(values), max_ns=max(values), samples_ns=values)


def summarize_phases(records, design):
    """Same complete 144-row design and same ratio-of-medians as primary R."""
    original = summarize(records, design)  # Identity/protocol/pair/warmup gates are shared.
    for row in records:
        stages(row)  # Validate warmups too; never fill a missing phase by subtraction.
    cells = []
    for cell in original['cells']:
        arm_stats = {}
        per_repeat = {}
        for role, arm in cell['arms'].items():
            selected = arm['all_rows']
            per_repeat[role] = {row['repeat']: row for row in selected}
            d_key, q_key = FIELDS[selected[0]['engine']]
            result = dict(arm=arm['arm'], protocol=deepcopy(arm['protocol']),
                source_fields={'Td': d_key, 'Tq': q_key, 'T': 'total_ns'},
                discovery_clock_is_residual=selected[0]['engine']=='native-ecCodes-SQLite-index')
            for phase in ('warmup', 'measurement'):
                rows = [row for row in selected if row['phase']==phase]
                result[phase] = dict(Td=stats([stages(row)[0] for row in rows]),
                                     Tq=stats([stages(row)[1] for row in rows]),
                                     T=stats([row['total_ns'] for row in rows]))
            arm_stats[role] = result
        fs, io = arm_stats['fs']['measurement'], arm_stats['iotdb']['measurement']
        pairs = []
        for repeat in sorted(per_repeat['fs']):
            f, i = per_repeat['fs'][repeat], per_repeat['iotdb'][repeat]
            fd, fq = stages(f); id_, iq = stages(i)
            pairs.append(dict(repeat=repeat, phase=f['phase'],
                Td_fs_ns=fd, Td_iotdb_ns=id_, Tq_fs_ns=fq, Tq_iotdb_ns=iq,
                T_fs_ns=f['total_ns'], T_iotdb_ns=i['total_ns'],
                Rd_pair=id_/fd, Rq_pair=iq/fq, R_pair=i['total_ns']/f['total_ns'],
                fs_discovery_weight=fd/f['total_ns'],
                delta_Td_ns=id_-fd, delta_Tq_ns=iq-fq,
                delta_T_ns=i['total_ns']-f['total_ns']))
        cells.append({**{key: deepcopy(cell[key]) for key in
            ('scene_id','variant','block_id','case_id','corpus_id','resource_id','resource_identity')},
            'R':cell['R'], 'Rd':io['Td']['median_ns']/fs['Td']['median_ns'],
            'Rq':io['Tq']['median_ns']/fs['Tq']['median_ns'],
            'arms':arm_stats, 'pair_ratios':pairs, 'partition_verified_rows':24})
    return dict(protocol='iotdbfs-resource-limited-phase-ratios/1', complete=True,
        expected_design_sha256=original['expected_design_sha256'], query_records=len(records),
        warmup_records=original['warmup_records'], measured_records=original['measured_records'],
        definitions={'R':'median(T_IoTDB-FS) / median(T_FS-index)',
                     'Rd':'median(Td_IoTDB-FS) / median(Td_FS-index)',
                     'Rq':'median(Tq_IoTDB-FS) / median(Tq_FS-index)'},
        cells=cells, notes=[
            'Every original row has exactly T=Td+Tq; existing profile corrections are not subtracted again.',
            'Separate phase medians need not sum to the total median. R is neither Rd+Rq nor Rd*Rq.',
            'Per pair R_pair=w*Rd_pair+(1-w)*Rq_pair with w=Td_fs/T_fs; not an aggregate median identity.',
            'GRIB FS discovery is residual T minus timed value reads, not a continuous discovery-first interval.',
            'GRIB FS Tq ends at C++ owned buffers; IoTDB-FS Tq includes IPC through NumPy owned buffers.',
            'HDF5 both arms end at NumPy owned buffers. Rq alone does not isolate physical IO, decoding or IPC.',
            'All cases, run IDs, endpoints, sample counts and warmup exclusions are inherited unchanged.'])


def derive(run_dir, output):
    root = Path(run_dir).resolve(strict=True)
    output = Path(output).resolve()
    need(not output.exists(), 'phase analysis output already exists; preserve prior evidence')
    paths = {name:root/(name+'.json') for name in ('report','design','analysis')}
    paths['queries'] = root/'queries.jsonl'
    report = read_json(paths['report'])
    need(report.get('complete') is True and report.get('original_service_restored') is True and
         report.get('resources_restored') is True and report.get('query_records')==144,
         'a complete restored 144-query run is required')
    for name in ('design','queries','analysis'):
        need(file_sha(paths[name])==report[name+'_sha256'], 'original '+name+' digest differs')
    design = read_json(paths['design'])
    records = [json.loads(line) for line in paths['queries'].read_text().splitlines()]
    need(summarize(records, design)==read_json(paths['analysis']), 'original complete analysis differs')
    result = summarize_phases(records, design)
    result['source_artifacts'] = {name:dict(path=str(path),sha256=file_sha(path)) for name,path in paths.items()}
    result['analyzer_sha256'] = file_sha(__file__)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False);stream.write('\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = derive(args.run_dir, args.output)
    print(json.dumps(dict(complete=result['complete'], records=result['query_records'],
                         output=str(args.output), sha256=file_sha(args.output))))


if __name__=='__main__':main()
