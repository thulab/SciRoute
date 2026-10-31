"""Read-only actual count SQL plans, exact-time-envelope and batch-size probes.

No schema/data/configuration writes. Fresh native sessions per campaign. SELECT
counts must equal the frozen raw manifest oracle in every variant. The envelope
retains all original (path,version) pairs; independent IN lists are not admitted.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import statistics
import subprocess
import hashlib
import small_runner as q


def build_probe(source, build, output, name):
    obj = output/(name+'.o')
    probe = source/'experiments/multi_file/native'/f'{name}.cpp'
    if (build/'compile_commands.json').exists():
        entries = json.loads((build/'compile_commands.json').read_text())
        entry, = [e for e in entries if e['file'].endswith('/src/query/core/DatasetCatalogQuery.cpp')
                  and 'CMakeFiles/iotdb_fs_core.dir/' in e['command']]
        cmd = shlex.split(entry['command'])
        cmd[cmd.index('-o')+1] = str(obj.resolve())
        cmd[cmd.index(entry['file'])] = str(probe.resolve())
        cwd = entry['directory']
    else:
        flags = dict(line.split(' = ',1) for line in (build/'CMakeFiles/iotdb_fs_core.dir/flags.make').read_text().splitlines()
                     if line.startswith(('CXX_DEFINES = ', 'CXX_INCLUDES = ', 'CXX_FLAGS = ')))
        compiler = shlex.split((build/'CMakeFiles/iotdb_fs.dir/link.txt').read_text())[0]
        cmd = [compiler]+shlex.split(' '.join(flags.values()))+['-c',str(probe.resolve()),'-o',str(obj.resolve())]
        cwd = build
    subprocess.run(cmd, cwd=cwd, check=True, capture_output=True)
    link = shlex.split((build/'CMakeFiles/iotdb_fs.dir/link.txt').read_text())
    binary = output/name
    link[link.index('-o')+1] = str(binary.resolve())
    link[link.index('CMakeFiles/iotdb_fs.dir/src/main.cpp.o')] = str(obj.resolve())
    subprocess.run(link, cwd=build, check=True, capture_output=True)
    return binary


def exact_envelope(sql, versions):
    return sql.replace(' WHERE ', f' WHERE time>={min(versions)} AND time<={max(versions)} AND ', 1)


def cohort(scale, n):
    prior=json.loads((scale/f'n{n}-d4/report.json').read_text())
    manifest=json.loads((scale/f'n{n}-d4/hdf5-data-manifest.json').read_text())
    versions=prior['preparation']['hdf5']['versions']
    prefix=prior['logical_prefix']+'/hdf5/'
    result=[]
    for source in manifest['sources']:
        path=prefix+source['file']
        result.append((path,versions[path],len(source['units']),sum(len(u['attributes']) for u in source['units'])))
    assert len(result)==n and len(set(r[:2] for r in result))==n
    return sorted(result)


def main(args):
    args.output.mkdir(parents=True, exist_ok=args.reuse_output)
    environment=json.loads(args.environment.read_text())
    env=dict(os.environ, **q.connection_environment(1001546, 'h09probe1790939889dbfc1f'))
    env['LD_LIBRARY_PATH']=environment['runtime_library_path']
    generator=build_probe(args.source,args.build,args.output,'catalog_integrity_sql')
    reader=build_probe(args.source,args.build,args.output,'catalog_readonly_sql')
    statements={}; expected={}; compile_stats={}
    for n in (32,128):
        rows=cohort(args.scale_root,n)
        chunks={'production':[rows], 'envelope':[rows], 'split32':[rows[i:i+32] for i in range(0,n,32)]}
        for variant,batches in chunks.items():
            compile_stats[f'n{n}-{variant}']=[]
            for b,keys in enumerate(batches):
                path=args.output/f'keys-n{n}-{variant}-b{b}.tsv'
                path.write_text(''.join('\t'.join(map(str,r))+'\n' for r in keys))
                output=subprocess.check_output([str(generator),str(path)],env=env,text=True)
                (args.output/f'generator-n{n}-{variant}-b{b}.tsv').write_text(output)
                timings=[]; fields={}
                for line in output.splitlines():
                    name,value=line.split('\t',1)
                    if name=='compile_ns': timings.append(int(value))
                    else: fields[name]=value
                compile_stats[f'n{n}-{variant}'].append(statistics.median(timings[1:]))
                for table in ('entry_count','attribute_count'):
                    label=f'n{n}-{variant}-{table}-b{b}'
                    sql=fields[table]
                    if variant=='envelope': sql=exact_envelope(sql,[r[1] for r in keys])
                    statements[label]=sql
                    index=2 if table=='entry_count' else 3
                    expected[label]={(r[0],r[1]):r[index] for r in keys}
    (args.output/'generated-sql.json').write_text(json.dumps(statements,indent=2)+'\n')
    raw=[]
    def run(filename, pairs, timeout):
        tsv=args.output/(filename+'.tsv'); tsv.write_text(''.join(k+'\t'+v+'\n' for k,v in pairs))
        if args.reuse_output and (args.output/(filename+'.jsonl')).exists():
            saved=[json.loads(line) for line in (args.output/(filename+'.jsonl')).read_text().splitlines()]
            if len(saved)==len(pairs) and all(row['ok'] for row in saved):
                return saved
        proc=subprocess.run([str(reader),str(tsv)],env=env,text=True,capture_output=True,timeout=timeout,check=True)
        (args.output/(filename+'.jsonl')).write_text(proc.stdout)
        return [json.loads(line) for line in proc.stdout.splitlines()]
    plans=[]
    for prefix in ('EXPLAIN ', 'EXPLAIN ANALYZE '):
        pairs=[(label,prefix+sql) for label,sql in statements.items() if '-split32-' not in label]
        plans+=run('plans' if prefix=='EXPLAIN ' else 'analyze',pairs,240)
    for i,row in enumerate(plans):
        text='\n'.join(str(c) for r in row['rows'] for c in r if c is not None)
        (args.output/(('explain-' if i<8 else 'analyze-')+row['label']+'.txt')).write_text(text+'\n')
    for campaign in range(3):
        labels=list(statements)
        if campaign%2: labels.reverse()
        pairs=[(f'c{campaign}-r{repeat}-'+label,statements[label])
               for repeat in range(5) for label in (labels if repeat%2==0 else list(reversed(labels)))]
        results=run(f'timing-c{campaign}',pairs,240)
        assert len(results)==len(pairs)
        for row in results:
            _,repeat,label=row['label'].split('-',2)
            assert row['ok'],row['error']
            actual={(r[0],r[1]):r[2] for r in row['rows']}
            assert len(actual)==len(row['rows']) and actual==expected[label],(label,'count mismatch')
            row.update(campaign=campaign,repeat=int(repeat[1:]),case=label)
            raw.append(row)
        print(json.dumps({'campaign':campaign,'passed':True,'selects':len(results)}),flush=True)
    summary=[]
    for n in (32,128):
        for variant in ('production','envelope','split32'):
            for table in ('entry_count','attribute_count'):
                totals=[]
                for c in range(3):
                    for r in range(1,5):
                        total=sum(x['elapsed_ns'] for x in raw if x['campaign']==c and x['repeat']==r
                                  and x['case'].startswith(f'n{n}-{variant}-{table}-'))
                        totals.append(total)
                summary.append({'N':n,'variant':variant,'table':table,'warm_samples':len(totals),
                    'median_ms':statistics.median(totals)/1e6,'min_ms':min(totals)/1e6,'max_ms':max(totals)/1e6,
                    'compile_median_ms':sum(compile_stats[f'n{n}-{variant}'])/1e6})
    build_source=Path(next(line.split('=',1)[1] for line in (args.build/'CMakeCache.txt').read_text().splitlines()
                               if line.startswith('CMAKE_HOME_DIRECTORY:INTERNAL=')))
    report={'complete':True,'protocol':'iotdbfs-catalog-integrity-plan/1','readonly':True,
        'database':'h09probe1790939889dbfc1f','records':raw,'summary':summary,
        'plans_complete':all(r['ok'] for r in plans),'plan_errors':[r['error'] for r in plans if not r['ok']],
        'code_sha256':{str(p.relative_to(args.source)):q.sha_file(p) for p in (Path(__file__),
            args.source/'experiments/multi_file/native/catalog_integrity_sql.cpp',
            args.source/'experiments/multi_file/native/catalog_readonly_sql.cpp',
            )},
        'compiled_header_sha256':q.sha_file(build_source/'include/iotdbfs/IoTDBPublishedCatalogSql.h'),
        'compiled_source':str(build_source),
        'linked_core_sha256':q.sha_file(args.build/'libiotdb_fs_core.a'),
        'query_observations_reused':args.reuse_output,
        'scope':'SELECT count acquisition/consumption, operation close and bounded JSON row formatting; session open outside timing; no end-to-end cache change'}
    q.sf.save(args.output/'report.json',report)
    print(json.dumps({'complete':True,'selects':len(raw),'plans_complete':report['plans_complete'],'summary':summary}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source','build','scale-root','environment','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--reuse-output',action='store_true',help='Revalidate saved successful plan/SELECT rows; do not resubmit those queries')
    main(parser.parse_args())
