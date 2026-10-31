"""Explain archived M1 publication cost without changing or rerunning samples."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics


def summarize(root):
    groups=defaultdict(list); receipts=defaultdict(list); identities={}
    for path in sorted(root.glob('*-IoTDB-FS/mount/publication.jsonl')):
        trace=json.loads((path.parent.parent/'trace.json').read_text())
        rows=[json.loads(line) for line in path.read_text().splitlines()]
        assert trace['complete'] and len(rows)==48 and all(r['ok'] for r in rows)
        full=[r for r in rows if r['source_bytes']>0]
        empty_create=[r for r in rows if r['kind']=='empty_create' and r['source_bytes']==0]
        empty_commit=[r for r in rows if r['kind']=='content_commit' and r['source_bytes']==0]
        assert (len(full),len(empty_create),len(empty_commit))==(24,8,16)
        assert {r['version'] for r in full}=={p['version'] for p in trace['publications']}
        identities[str(path.relative_to(root))]=hashlib.sha256(path.read_bytes()).hexdigest()
        for r in rows:
            groups[(trace['format'],r['kind'],r['source_bytes']==0)].append(r)
        receipts[trace['format']].extend(p['receipt'] for p in trace['publications'])
    assert len(identities)==18
    summaries=[]
    for (fmt,kind,empty),rows in sorted(groups.items()):
        stages=set().union(*(r['profile']['stages'] for r in rows))
        summaries.append(dict(format=fmt,kind=kind,empty=empty,count=len(rows),
            mean_ms=statistics.mean(r['elapsed_ns'] for r in rows)/1e6,
            stage_mean_ms={s:statistics.mean(r['profile']['stages'].get(s,{}).get('wall_nanoseconds',0) for r in rows)/1e6 for s in sorted(stages)}))
    return dict(complete=True,traces=len(identities),business_publications=432,backend_publications=864,
                groups=summaries,receipt_mean_ms={fmt:{k:statistics.mean(r[k] for r in rows)/1e6 for k in rows[0]} for fmt,rows in receipts.items()},
                source_sha256=identities,
                boundary='public API implementation overhead; empty publications are extra backend states, not extra scientific versions in the requested workload; no concurrent atomic-visibility claim')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args()
    result=summarize(a.root)
    (a.root/'publication-analysis.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps({k:result[k] for k in ('complete','traces','business_publications','backend_publications')}))
