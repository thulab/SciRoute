#!/usr/bin/env python3
"""Validate complete compression/filter/concurrency coverage; summarize bursts."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def trimmed(values):
    ordered=sorted(values)
    return statistics.mean(ordered[1:-1] if len(ordered)>2 else ordered)


def percentile(values,p):
    ordered=sorted(values);rank=(len(ordered)-1)*p
    lo=int(rank);hi=min(lo+1,len(ordered)-1)
    return ordered[lo]+(ordered[hi]-ordered[lo])*(rank-lo)


def summarize(directory,repeats=10):
    queries=json.loads((directory/'measurements.json').read_text())
    waves=json.loads((directory/'waves.json').read_text())
    if not queries or not waves: raise ValueError('empty benchmark')
    def key(r):return (r['dataset'],r['codec'],r['clients'],r['case'],r['arm'])
    qgroups=defaultdict(list);wgroups=defaultdict(list)
    for row in queries:qgroups[key(row)].append(row)
    for row in waves:wgroups[key(row)].append(row)
    if set(qgroups)!=set(wgroups):raise ValueError('request/wave group mismatch')
    summaries=[]
    counterkeys=('read_bytes','read_ranges','payload_read_calls','payload_read_nanoseconds',
                 'decode_count','decoded_bytes','decompressions','waits','evictions','redecodes')
    for identity,rows in sorted(qgroups.items()):
        dataset,codec,clients,case,arm=identity
        ws=wgroups[identity]
        if len(ws)!=repeats+1 or {w['repeat'] for w in ws}!=set(range(repeats+1)):
            raise ValueError(f'incomplete waves {identity}')
        if len(rows)!=(repeats+1)*clients or len({(r['repeat'],r['client_id']) for r in rows})!=len(rows):
            raise ValueError(f'incomplete queries {identity}')
        cold=next(w for w in ws if w['repeat']==0);warm=[w for w in ws if w['repeat']>0]
        latencies=[r['wall_ns']/1e6 for r in rows if r['repeat']>0]
        wall=trimmed([w['wave_wall_ns']/1e6 for w in warm])
        item=dict(dataset=dataset,codec=codec,clients=clients,case=case,arm=arm,
                  cold_wave_ms=cold['wave_wall_ns']/1e6,warm_wave_trimmed_ms=wall,
                  warm_burst_qps=clients*1000/wall,latency_p50_ms=percentile(latencies,.5),
                  latency_p95_ms=percentile(latencies,.95),rows_per_client=[r['rows'] for r in sorted(rows,key=lambda r:r['client_id']) if r['repeat']==0])
        if arm in ('B','C'):
            item['cold_counters']={k:cold[k] for k in counterkeys}
            item['warm_counters_mean']={k:statistics.mean(w[k] for w in warm) for k in counterkeys}
            if any(w['payload_read_calls'] or w['decompressions'] for w in warm):
                raise ValueError(f'warm payload/decompression {identity}')
        if arm=='E':
            if len({r['path'] for r in rows})!=len(rows):raise ValueError('E reused route')
            if any(r['server']['runtime_delta']['payload_read_calls'] or r['server']['runtime_delta']['decompressions'] for r in rows if r['repeat']>0):
                raise ValueError('E warm payload/decompression')
            item['encoded_bytes_mean']=statistics.mean(r['encoded_bytes'] for r in rows)
            if clients==1:
                item['cold_counters']={k:next(r for r in rows if r['repeat']==0)['server']['runtime_delta'][k] for k in counterkeys}
            else:item['counter_attribution']='not summed: overlapping global deltas'
            item['client_stages_ms']={k:trimmed([r[k]/1e6 for r in rows if r['repeat']>0]) for k in ('registration_ns','ipc_read_ns','consume_ns')}
            item['server_stages_ms']={k:trimmed([r['server']['stages_ns'].get(k,0)/1e6 for r in rows if r['repeat']>0]) for k in ('typed_prepare','typed_encode','query_authorize','result_publish')}
            intervals=[(r['server']['steady_start_ns'],r['server']['steady_end_ns']) for r in rows]
            events=sorted([(a,1) for a,b in intervals]+[(b,-1) for a,b in intervals]);active=peak=0
            for t,delta in events:active+=delta;peak=max(peak,active)
            item['server_peak_overlapping_requests']=peak
        summaries.append(item)
    # Every cell has A–E plus native read pairing; all codecs and clientcounts
    # preserve the same per-client logical output under the fixed request.
    digests=defaultdict(set)
    for row in queries:digests[(row['dataset'],row['case'],row['client_id'])].add((row['digest'],row['index_digest'],row['rows']))
    if any(len(values)!=1 for values in digests.values()):raise ValueError('cross-arm/codec/client mismatch')
    for dataset,codec,clients,case in {k[:4] for k in qgroups}:
        for arm in ('A','A-read','B','C','D','E'):
            if (dataset,codec,clients,case,arm) not in qgroups:raise ValueError('missing arm')
        bc=[next(w for w in wgroups[(dataset,codec,clients,case,a)] if w['repeat']==0) for a in ('B','C')]
        for k in ('read_bytes','read_ranges','payload_read_calls','decode_count','decompressions'):
            if bc[0][k]!=bc[1][k]:raise ValueError(f'B/C physical work differs {dataset}/{codec}/{clients}/{case}/{k}')
        if clients == 1:
            e = next(r for r in qgroups[(dataset,codec,clients,case,'E')] if r['repeat']==0)['server']['runtime_delta']
            for k in ('read_bytes','read_ranges','payload_read_calls','decode_count','decompressions'):
                if e[k]!=bc[1][k]:raise ValueError('serial E physical work differs from C')
        for arm in ('B','C'):
            baseline=wgroups.get((dataset,codec,1,case,arm))
            if baseline is None:continue
            one=next(w for w in baseline if w['repeat']==0)
            many=next(w for w in wgroups[(dataset,codec,clients,case,arm)] if w['repeat']==0)
            if case=='c7-disjoint-batches':
                for k in ('payload_read_calls','decode_count','decompressions'):
                    if many[k]!=clients*one[k]:raise ValueError('disjoint cold work did not scale with batches')
            else:
                for k in ('read_bytes','payload_read_calls','decode_count','decompressions'):
                    if many[k]!=one[k]:raise ValueError('shared cold load was duplicated')
    return dict(passed=True,queries=len(queries),waves=len(waves),groups=len(qgroups),
                cells=len(qgroups)//6,warm_repeats=repeats,summary=summaries)


def main():
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path);p.add_argument('--repeats',type=int,default=10)
    args=p.parse_args();result=summarize(args.directory,args.repeats)
    (args.directory/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='summary'}))

if __name__=='__main__':main()
