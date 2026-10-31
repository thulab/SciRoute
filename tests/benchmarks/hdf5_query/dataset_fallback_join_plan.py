#!/usr/bin/env python3
"""Compile actual before/after SQL generators; EXPLAIN only in frozen GEOS DB."""
import argparse
import ast
import json
import os
from pathlib import Path
import re
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'build', 'ablation', 'base', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    probe = args.source / 'tests/benchmarks/hdf5_query/dataset_fallback_join_sql_probe.cpp'
    entries = json.loads((args.build / 'compile_commands.json').read_text())
    entry, = [e for e in entries if e['file'].endswith('/src/query/core/DatasetCatalogQuery.cpp')
              and 'CMakeFiles/iotdb_fs_core.dir/' in e['command']]
    command = shlex.split(entry['command'])
    obj = args.output / 'sql-probe.o'
    command[command.index('-o') + 1] = str(obj.resolve())
    command[command.index(entry['file'])] = str(probe.resolve())
    subprocess.run(command, cwd=entry['directory'], check=True)
    generators = {}
    for variant in ('before', 'after'):
        link = shlex.split((args.build / 'CMakeFiles/iotdb_fs.dir/link.txt').read_text())
        binary = args.output / ('sql-' + variant)
        link[link.index('-o') + 1] = str(binary.resolve())
        link[link.index('CMakeFiles/iotdb_fs.dir/src/main.cpp.o')] = str(obj.resolve())
        if variant == 'before':
            link[link.index('libiotdb_fs_core.a')] = str((args.ablation / 'before-core.a').resolve())
        subprocess.run(link, cwd=args.build, check=True)
        generators[variant] = binary
    pilot = json.loads((args.base / 'geos-time-20261007/ingest-scale-16/pilot.json').read_text())
    sources = pilot['sources']
    assert pilot['database'] == 'geostime20261007' and len(sources) == 16
    statements, sql_report = {}, {}
    for count in (1, 16):
        keys = args.output / f'keys-n{count}.tsv'
        # Full expected counts are previously verified frozen catalog facts.
        keys.write_text(''.join(f"{s['logical_path']}\t{s['version']}\t18\t194\n" for s in sources[:count]))
        for kind in ('lt', 'and', 'equality'):
            compared = {}
            for variant in ('before', 'after'):
                label = f'{variant}-n{count}-{kind}'
                output = subprocess.check_output([str(generators[variant]), str(keys), kind], text=True)
                fields = dict(line.split('\t', 1) for line in output.splitlines())
                sql_report[label] = fields
                compared[variant] = fields
                expected_attrs = (1 if kind != 'and' else 2) if variant == 'before' or kind == 'equality' else 0
                assert int(fields['attributes']) == expected_attrs
                assert ('LEFT JOIN blob_dataset_attribute' in fields['filter']) == bool(expected_attrs)
                statements[label] = 'EXPLAIN ' + fields['filter']
            assert compared['before']['entry_count'] == compared['after']['entry_count']
            assert compared['before']['attribute_count'] == compared['after']['attribute_count']
            if kind == 'equality':
                assert compared['before']['filter'] == compared['after']['filter']
    (args.output / 'generated-sql.json').write_text(json.dumps(sql_report, indent=2) + '\n')
    tsv = args.output / 'statements.tsv'
    tsv.write_text(''.join(name + '\t' + sql + '\n' for name, sql in statements.items()))
    helper = args.source / 'tests/benchmarks/arrow_training/a12_migration_smoke.py'
    definition = next(n for n in ast.parse(helper.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == 'connection_environment')
    namespace = {'os': os, 'pathlib': __import__('pathlib')}
    exec(compile(ast.Module(body=[definition], type_ignores=[]), str(helper), 'exec'), namespace)
    env = namespace['connection_environment'](1001546, pilot['database'])
    env.setdefault('IOTDB_HOST', '127.0.0.1')
    readonly = args.base / 'geos-query-sql-plan-20261010/readonly_sql_probe'
    with (args.output / 'raw.jsonl').open('w') as stdout, (args.output / 'stderr.log').open('w') as stderr:
        subprocess.run([str(readonly), str(tsv)], env=env, stdout=stdout, stderr=stderr, timeout=240, check=True)
    raw = [json.loads(line) for line in (args.output / 'raw.jsonl').read_text().splitlines()]
    assert len(raw) == 12
    assert all(r['ok'] or r['error'] == 'query output exceeded probe bounds' for r in raw)
    summary = {}
    for row in raw:
        text = '\n'.join(str(c) for r in row['rows'] for c in r if c is not None)
        (args.output / (row['label'] + '.txt')).write_text(text + '\n')
        cells = [c.strip() for line in text.splitlines() for c in line.split('│')
                 if c.strip() and not set(c.strip()) <= set('─┌┐└┘┬┴├┤┼')]
        (args.output / (row['label'] + '-cells.txt')).write_text('\n'.join(cells) + '\n')
        summary[row['label']] = {'complete': row['ok'], 'error': row['error'], 'rows': len(row['rows']),
                                'logical_join_nodes': sorted({c for c in cells if re.fullmatch(r'Join-\d+', c)}),
                                'join_cells': [c for c in cells if 'Join' in c],
                                'attribute_table_cells': [c for c in cells if 'blob_dataset_attribute' in c],
                                'sql_attribute_joins': int(sql_report[row['label']]['attributes'])}
    complete = all(r['ok'] for r in raw)
    (args.output / 'summary.json').write_text(json.dumps({'complete': complete, 'requests': len(raw), 'plans': summary}, indent=2) + '\n')
    print(json.dumps({'complete': complete, 'requests': len(raw), 'plans': summary}))


if __name__ == '__main__':
    main()
