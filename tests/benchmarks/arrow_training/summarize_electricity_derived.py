#!/usr/bin/env python3
"""Summarize paired derived-layout trace observations without mixing setup timers."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def summarize(rows):
    groups=defaultdict(list)
    for row in rows:
        if not row['passed']:raise ValueError('failed correctness observation')
        groups[(row['layout'],row['block_steps'],row['codec'],row['mode'],row['width'],row['temperature'])].append(row)
    result=[]
    for key,items in sorted(groups.items()):
        # Trim warm extrema within each independently cold-started replication.
        runs=defaultdict(list)
        for item in items:runs[item['repetition']].append(item)
        selected=[]
        for run in runs.values():
            run=sorted(run,key=lambda r:r['wall_seconds'])
            selected.extend(run[1:-1] if key[-1]=='warm' and len(run)>=3 else run)
        metrics=set.intersection(*(set(r['metrics']) for r in selected))
        result.append(dict(zip(('layout','block_steps','codec','mode','width','temperature'),key),
            observations=len(items),retained=len(selected),wall_seconds=statistics.mean(r['wall_seconds'] for r in selected),
            min_wall_seconds=min(r['wall_seconds'] for r in items),max_wall_seconds=max(r['wall_seconds'] for r in items),
            setup_seconds=statistics.mean(r['setup_seconds'] for r in selected),file_bytes=items[0]['file_bytes'],
            metrics={m:statistics.mean(r['metrics'][m] for r in selected) for m in sorted(metrics)}))
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('input',type=Path);p.add_argument('output',type=Path)
    args=p.parse_args();rows=json.loads(args.input.read_text());result=summarize(rows)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    for r in result:
        if r['width']==370 and r['mode'] in ('iotdb','original-local'):
            m=r['metrics']
            print(r['layout'],r['block_steps'],r['codec'],r['mode'],r['temperature'],round(r['wall_seconds'],4),
                  'read MB',round(m.get('read_bytes',0)/1e6,2),'calls',m.get('read_calls'), 'decodes',m['batch_decodes'])

if __name__=='__main__':main()
