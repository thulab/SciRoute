"""Strict complete-M1 coverage and clock analysis of archived runner evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

ARMS=('FS-scan','FS-index','IoTDB-FS')


def stats(values, divisor=1e6):
    return dict(n=len(values),mean=statistics.mean(values)/divisor,median=statistics.median(values)/divisor,
                min=min(values)/divisor,max=max(values)/divisor,samples=[v/divisor for v in values])


def analyze(root):
    root=Path(root); report=json.loads((root/'report.json').read_text()); frozen=json.loads((root/'frozen.json').read_text())
    if not report['complete'] or frozen['pilot'] or report.get('failure'):raise ValueError('complete formal run without failure required')
    supervisor=json.loads((root.parent/'supervisor-report.json').read_text())
    restored=json.loads((root.parent/'resources-restored.json').read_text())
    if not supervisor['complete'] or not supervisor['resources_restored'] or restored['phase']!='closed' or not restored['database_restored']:raise ValueError('resource restoration incomplete')
    snapshots=[json.loads((root.parent/'resource-before.json').read_text())]
    snapshots.extend(json.loads(line) for line in (root.parent/'resource-samples.jsonl').read_text().splitlines())
    snapshots.append(json.loads((root.parent/'resource-after.json').read_text()))
    database_pid=snapshots[0]['groups']['database']['cgroup.procs'].strip()
    if not database_pid.isdecimal():raise ValueError('one live DataNode required')
    for snap in snapshots:
        groups=snap['groups']
        if groups['parent']['cpu.max']!='200000 100000' or groups['parent']['cpuset.cpus.partition']!='root':raise ValueError('shared CPU policy changed')
        if groups['database']['cgroup.procs'].strip()!=database_pid:raise ValueError('DataNode left shared group')
        for name in ('query','database'):
            if groups[name]['cpuset.cpus.effective']!='2-3' or groups[name]['cpu.max']!='max 100000':raise ValueError('child CPU policy changed')
        if groups['query']['memory.max']!='2147483648' or groups['query']['memory.swap.max']!='0':raise ValueError('query memory policy changed')
    times=[s['time_ns'] for s in snapshots]
    if times!=sorted(times):raise ValueError('resource sample ordering differs')
    for pid,before in frozen['services_before'].items():
        if report['services_after'][pid]['start_ticks']!=before['start_ticks']:raise ValueError('shared service restarted')
    rows=[json.loads(Path(p).read_text()) for p in report['traces']]
    keys=[(r['format'],r['campaign'],r['q'],r['arm']) for r in rows]
    expected={(f,c,q,a) for f in ('grib','hdf5') for c in range(3) for q in (1,10,100) for a in ARMS}
    if len(keys)!=54 or set(keys)!=expected:raise ValueError('coverage mismatch')
    cells=[]
    def fields(text):return {line.split()[0].rstrip(':'):int(line.split()[1]) for line in text.splitlines()}
    def delta(r,field,key):return fields(r['resource_after'][field])[key]-fields(r['resource_before'][field])[key]
    def db_io(r,key):return fields(r['database_after']['io'])[key]-fields(r['database_before']['io'])[key]
    for r in rows:
        if not r['complete'] or not r['reopen_verified']:raise ValueError('unverified trace')
        for where in ('resource_before','resource_after'):
            z=r[where]
            if z['parent_cpu_max']!='200000 100000' or z['cpu.max']!='max 100000' or z['memory.max']!='2147483648' or z['cpuset.cpus.effective']!='2-3' or z['parent_partition']!='root':raise ValueError('resource policy mismatch')
            if any(fields(z['memory.events']).get(k,0) for k in ('oom','oom_kill','oom_group_kill')):raise ValueError('OOM invalidates trace')
        if r['database_before']['start_ticks']!=r['database_after']['start_ticks']:raise ValueError('database process changed')
        if len(r['publications'])!=24 or len(r['queries'])!=16*r['q']:raise ValueError('trace counts differ')
        if r['total_ns']!=r['foreground_ns']+r['explicit_flush_ns']+r['close_ns']:raise ValueError('clock partition differs')
        seen={(p['file_id'],p['ordinal']) for p in r['publications']}
        if seen!={(f,v) for f in range(8) for v in (1,2,3)}:raise ValueError('publication coverage differs')
        if {(x['update'],x['query']) for x in r['queries']}!={(u,i) for u in range(16) for i in range(r['q'])}:raise ValueError('query coverage differs')
        for x in r['queries']:
            if not x['oracle_verified'] or x['total_ns']!=x['discovery_ns']+x['value_ns']:raise ValueError('query validation or clock differs')
            want='v1' if r['q']>=10 and x['query']%10==0 else 'latest'
            if x['kind']!=want:raise ValueError('history fraction differs')
        if r['arm']!='IoTDB-FS':
            if r['checkpoint']['busy'] or r['checkpoint']['wal_bytes']!=0:raise ValueError('checkpoint incomplete')
            if r['status']['sources']!=24 or r['status']['units']!=(96 if r['arm']=='FS-index' else 0):raise ValueError('final source/unit count differs')
            if r['status']['orphan_files'] or r['status']['staging_files']:raise ValueError('unfinished source publication')
    for fmt in ('grib','hdf5'):
        for q in (1,10,100):
            arms={}
            for campaign in range(3):
                manifests={r['source_manifest_sha256'] for r in rows if (r['format'],r['q'],r['campaign'])==(fmt,q,campaign)}
                if len(manifests)!=1:raise ValueError('arms do not share frozen input manifest')
            for arm in ARMS:
                rs=sorted([r for r in rows if (r['format'],r['q'],r['arm'])==(fmt,q,arm)],key=lambda r:r['campaign'])
                arms[arm]=dict(total_seconds=stats([r['total_ns'] for r in rs],1e9),
                    initialization_ms=stats([r['initialization_ns'] for r in rs]),
                    initial_publications_ms=stats([sum(p['total_ns'] for p in r['publications'][:8]) for r in rs]),
                    updates_ms=stats([sum(p['total_ns'] for p in r['publications'][8:]) for r in rs]),
                    query_sum_ms=stats([sum(x['total_ns'] for x in r['queries']) for r in rs]),
                    finish_ms=stats([r['explicit_flush_ns']+r['close_ns'] for r in rs]),queries={},query_classes={},
                    query_group_cpu_ms=stats([delta(r,'cpu.stat','usage_usec') for r in rs],1000),
                    datanode_cpu_ms=stats([(r['database_after']['cpu_ticks']-r['database_before']['cpu_ticks'])*10000000 for r in rs]),
                    datanode_average_cores=stats([(r['database_after']['cpu_ticks']-r['database_before']['cpu_ticks'])*1e7/r['total_ns'] for r in rs],1),
                    datanode_read_bytes=stats([db_io(r,'read_bytes') for r in rs],1),
                    datanode_write_bytes=stats([db_io(r,'write_bytes') for r in rs],1))
                classes={'post_update_first':lambda x:x['query']==0,
                         'first_latest_after_v1':lambda x:q>=10 and x['query']%10==1,
                         'remaining':lambda x:x['query']>0 and not(q>=10 and x['query']%10==1)}
                for label,predicate in classes.items():
                    batches=[[x for x in r['queries'] if predicate(x)] for r in rs]
                    if all(batches):arms[arm]['query_classes'][label]={k:stats([statistics.median(x[k] for x in b) for b in batches]) for k in ('total_ns','discovery_ns','value_ns')}
                for kind in ('latest','v1'):
                    # Preserve each campaign median, do not pool thousands of correlated repetitions.
                    medians=[statistics.median(x['total_ns'] for x in r['queries'] if x['kind']==kind) for r in rs if any(x['kind']==kind for x in r['queries'])]
                    if medians:arms[arm]['queries'][kind]=dict(campaign_median_ms=stats(medians),count=sum(x['kind']==kind for r in rs for x in r['queries']))
            cells.append(dict(format=fmt,q=q,arms=arms))
    return dict(complete=True,traces=len(rows),publications=sum(len(r['publications']) for r in rows),queries=sum(len(r['queries']) for r in rows),
                cells=cells,datanode_clock_ticks_per_second=100,resources_restored=True,
                resource_samples=len(snapshots),largest_resource_sample_gap_s=max(b-a for a,b in zip(times,times[1:]))/1e9,
                statistic='whole-trace mean/median/range across three campaigns; query summaries across campaign medians',
                report_sha256=hashlib.sha256((root/'report.json').read_bytes()).hexdigest(),frozen=frozen)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args()
    out=analyze(a.root);(a.root/'analysis.json').write_text(json.dumps(out,indent=2,sort_keys=True)+'\n')
    print(json.dumps({k:out[k] for k in ('complete','traces','publications','queries')}))
