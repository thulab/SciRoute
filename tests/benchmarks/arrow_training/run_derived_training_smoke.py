#!/usr/bin/env python3
"""Paired 50-step PatchTST validation of the experimental derived reader."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from compare_public_ab import compare


def main():
    p=argparse.ArgumentParser()
    for n in ('work-dir','manifest','mount','socket','helper','trace','output'):
        p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--daemon-pid',type=int,required=True)
    p.add_argument('--repetitions',type=int,default=2)
    args=p.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    scripts=Path(__file__).parent
    results=[]
    env=dict(os.environ,CUDA_VISIBLE_DEVICES='0')
    def execute(command,tag):
        with (args.output/f'{tag}.log').open('w') as log:
            subprocess.run(command,env=env,check=True,stdout=log,stderr=subprocess.STDOUT,timeout=600)
        report=json.loads((args.output/f'{tag}.json').read_text())
        print('PASS',tag,round(report['train_wall_seconds'],4),report['train_loss'],flush=True)
        return report
    for rep in range(args.repetitions):
        tag=f'r{rep}-original-local'
        local=execute([sys.executable,str(scripts/'train_hf.py'),'--work-dir',str(args.work_dir),'--dataset','UCI_Electricity_370',
            '--backend','local','--model','patchtst','--max-steps','50','--batch-size','2','--eval-windows','4','--seed','20260929',
            '--cache-bytes',str(1024**3),'--output-dir',str(args.output/'runs'/tag),'--report',str(args.output/f'{tag}.json')],tag)
        results.append(dict(tag=tag,mode='original-local',temperature='cold',train_wall_seconds=local['train_wall_seconds'],fetch_seconds=local['fetch_seconds']['train'],passed=True))
        arms=[('local',128,'lz4'),('iotdb',128,'lz4'),('iotdb',8192,'none')]
        if rep%2:arms.reverse()
        for mode,block,codec in arms:
            for warm in (False,True):
                tag=f'r{rep}-{mode}-sm{block}-{codec}-'+('warm' if warm else 'cold')
                command=[sys.executable,str(scripts/'train_electricity_derived.py'),'--work-dir',str(args.work_dir),
                    '--manifest',str(args.manifest),'--mount',str(args.mount),'--socket',str(args.socket),
                    '--helper',str(args.helper),'--daemon-pid',str(args.daemon_pid),'--layout','step_major','--block-steps',str(block),
                    '--codec',codec,'--mode',mode,'--model','patchtst','--max-steps','50','--trace',str(args.trace),
                    '--output-dir',str(args.output/'runs'/tag),'--report',str(args.output/f'{tag}.json')]
                if warm:command.append('--warm')
                report=execute(command,tag)
                # All physical variants still reference canonical PB00/PB01 IDs.
                comparison=compare(local,report) if mode=='iotdb' else compare(local,dict(report,backend='iotdbfs',source_version=report['derived_artifact']['version']))
                (args.output/f'{tag}-comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
                if not comparison['passed']:raise ValueError(f'training correctness failed: {tag}')
                result=dict(tag=tag,mode=mode,block_steps=block,codec=codec,temperature=report['temperature'],
                    train_wall_seconds=report['train_wall_seconds'],fetch_seconds=report['fetch_seconds']['train'],passed=True,
                    setup_seconds=report['backend_metrics']['setup_seconds'])
                results.append(result)
                (args.output/'summary.json').write_text(json.dumps(results,indent=2)+'\n')

if __name__=='__main__':main()
