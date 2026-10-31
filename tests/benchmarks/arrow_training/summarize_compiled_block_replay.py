#!/usr/bin/env python3
"""Attribute production replay stages without counting nested timers twice."""
import argparse
import json
from pathlib import Path
from summarize_query_profile import read_server_log, summarize


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory',type=Path)
    args=p.parse_args()
    results=[]
    for row in json.loads((args.directory/'results.json').read_text()):
        records=read_server_log(args.directory/(row['tag']+'-profile.jsonl'))
        summary=summarize(records,row['metrics']['request_timings'])
        if summary['excluded_count'] or summary['request_count']!=len(row['request_seconds']):
            raise ValueError('incomplete or overlapping server profiles')
        prepares=[r for r in records if r.get('operation')=='window_prepare' and r.get('prepared_id') in
            {t['prepared_id'] for t in row['metrics']['preparation_timings']}]
        result={k:row[k] for k in ('tag','temperature','wall_seconds','constructor_seconds','passed')}
        result.update(reused_context=row.get('reused_context',False),
            preparation_seconds=row['metrics']['preparation_seconds'],
            normalize_seconds=sum(t.get('normalize_seconds',0) for t in row['metrics']['preparation_timings']),
            post_prepare_seconds=row['wall_seconds']-row['metrics']['preparation_seconds'],
            ipc_bytes=row['metrics']['ipc_bytes'],
            server_seconds={k:v['sum'] for k,v in summary['server_seconds'].items()},
            runtime_delta={k:v['sum'] for k,v in summary['runtime_delta'].items()},
            preparation_stages_seconds={k:sum(r.get('stages_ns',{}).get(k,0) for r in prepares)/1e9
                for k in sorted({k for r in prepares for k in r.get('stages_ns',{})})})
        results.append(result)
    (args.directory/'stage-summary.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps(results,indent=2))

if __name__=='__main__':main()
