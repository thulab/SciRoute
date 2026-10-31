#!/usr/bin/env python3
"""Whitelist experimental evidence; never archive daemon env/configuration/logs."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile


def sha(path):
    result=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024**2),b''):result.update(b)
    return result.hexdigest()


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();root=args.root
    files=[]
    for folder in ('electricity-derived-20260930','electricity-derived-tuning-20260930'):
        files.extend((root/folder).glob('*.json'))
    for folder in ('electricity-derived-replay-20260930','electricity-derived-tuning-replay-20260930',
        'electricity-derived-typed-20260930','electricity-derived-typed-coarse-20260930','electricity-derived-training-20260930'):
        for pattern in ('*.json','*.jsonl'):files.extend((root/folder).glob(pattern))
    for name in ('electricity-derived-build-final.log','electricity-derived-control-build.log','electricity-derived-tuning-build.log',
        'electricity-derived-publish.log','electricity-derived-publish-final.log','electricity-derived-tuning-publish.log',
        'electricity-derived-tests.log','electricity-derived-typed.log','electricity-derived-typed-coarse.log','electricity-derived-training.log'):
        files.append(root/name)
    scripts=('derived_window_reader.py','prepare_electricity_derived.py','run_electricity_derived.py','replay_derived_typed.py',
        'train_electricity_derived.py','run_derived_training_smoke.py','summarize_electricity_derived.py',
        'test_prepare_electricity_derived.py','test_derived_window_reader.py','train_hf.py')
    files.extend(root/name for name in scripts)
    cpp=root/'a05-decoded-cache-20260927/IoTDB-FS-C/tests/benchmarks/arrow_training/derived_range_bridge.cpp'
    files.append(cpp)
    bins=[root/'a05-decoded-cache-20260927/build/bin'/n for n in ('arrow_derived_range_bridge','iotdb_fs')]
    provenance={'files':{str(f.relative_to(root)):sha(f) for f in files},'binaries':{str(f.relative_to(root)):sha(f) for f in bins},
        'excluded':'all private properties, /proc environ, daemon/bridge logs, large Arrow artifacts and model runtime directories',
        'source_version':1790670177863}
    path=root/'electricity-derived-provenance.json';path.write_text(json.dumps(provenance,indent=2)+'\n');files.append(path)
    with tarfile.open(args.output,'w:gz') as tar:
        for f in files:tar.add(f,arcname=str(f.relative_to(root)),recursive=False)
    print(json.dumps({'files':len(files),'bytes':args.output.stat().st_size,'sha256':sha(args.output)}))

if __name__=='__main__':main()
