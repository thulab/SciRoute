#!/usr/bin/env python3
"""Verify real prior /3 ingestion -> new /4 version using retained H09 binary."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid


def main(a):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent/'arrow_training'))
    from a12_migration_smoke import connection_environment
    assert a.database.startswith('h09probe')
    root=a.output.resolve();root.mkdir(parents=True,exist_ok=False)
    env=connection_environment(a.source_pid,a.database)
    env['IOTDBFS_NAMESPACE_LOCK']=str(a.writer_lock.resolve())
    env['IOTDB_CONFIG_FILE']='/nonexistent/sc-migration.properties'
    path='/h09-sc-legacy-'+uuid.uuid4().hex[:12]+'.nc'
    steps=[]
    def run(argv):
        completed=subprocess.run([str(x) for x in argv],env=env,cwd=root,capture_output=True,text=True,timeout=300)
        steps.append({'argv':[str(x) for x in argv],'returncode':completed.returncode,'stdout':completed.stdout,'stderr':completed.stderr})
        (root/'commands.json').write_text(json.dumps(steps,indent=2)+'\n')
        assert completed.returncode==0, str(argv)+': see commands.json'
        return [json.loads(line) for line in completed.stdout.splitlines() if line.startswith('{')]
    old=run([a.old_fixture,'seed',path,a.raw,'indexed'])[0]
    assert old['layout_protocol']=='IOTDBFS-HDF5-LAYOUT/3',old
    report=root/'migration'
    cli=a.build.resolve()/'bin/iotdb_fs'
    run([cli,'hdf5-migrate','inventory','--report',report,'--path',path])
    run([cli,'hdf5-migrate','run','--report',report])
    run([cli,'hdf5-migrate','resume','--report',report])
    # Old fixture's verify command only reads exact stored bytes/metadata, so it
    # remains an independent observer for both protocols.
    manifest=(report/'report.manifest').read_text()
    outcomes=[json.loads(x) for x in (report/'results.jsonl').read_text().splitlines()]
    assert len(outcomes)==1 and outcomes[0]['state']=='verified',outcomes
    new=run([a.old_fixture,'verify',path,outcomes[0]['destination_version'],a.raw])[0]
    assert new['layout_protocol']=='IOTDBFS-HDF5-LAYOUT/4',new
    assert new['sha256']==old['sha256'] and new['version']!=old['version'],new
    old_after=run([a.old_fixture,'verify',path,old['version'],a.raw])[0]
    assert old_after==old,(old_after,old)
    result={'ok':True,'protocol':'iotdbfs-hdf5-sc-migration/1','path':path,'old_version':old['version'],
            'old_protocol':old['layout_protocol'],'original_sha256':old['sha256'],
            'new_version':new['version'],'new_protocol':new['layout_protocol'],'old_version_unchanged':True,'run_and_resume_succeeded':True,'manifest':manifest}
    (root/'report.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'ok':True,'report':str(root/'report.json')}))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ['build','old-fixture','raw','output','writer-lock']:p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--database',required=True);p.add_argument('--source-pid',type=int,required=True)
    main(p.parse_args())
