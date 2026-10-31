#!/usr/bin/env python3
"""Build a one-object old-behavior diagnostic without modifying source/build.

The production binary must already have been built and its complete unit suite
passed. Both binaries share all objects except DatasetCatalogQuery.cpp.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    original = args.source / 'src/query/core/DatasetCatalogQuery.cpp'
    content = original.read_text()
    token = '    out.attributes.clear();'
    if content.count(token) != 1:
        raise ValueError('expected one reviewed fallback attribute clear')
    overlay = args.output / 'DatasetCatalogQuery.cpp'
    overlay.write_text(content.replace(token, '    // Diagnostic: preserve old SQL attribute dependencies.'))
    entries = json.loads((args.build / 'compile_commands.json').read_text())
    entry, = [e for e in entries if Path(e['file']).resolve() == original.resolve()
              and 'CMakeFiles/iotdb_fs_core.dir/' in e['command']]
    command = shlex.split(entry['command'])
    obj = args.output / 'DatasetCatalogQuery.cpp.o'
    command[command.index('-o') + 1] = str(obj.resolve())
    command[command.index(entry['file'])] = str(overlay.resolve())
    subprocess.run(command, cwd=entry['directory'], check=True)
    archive = args.output / 'before-core.a'
    shutil.copy2(args.build / 'libiotdb_fs_core.a', archive)
    members = subprocess.check_output(['ar', 't', str(archive)], text=True).splitlines()
    if members.count(obj.name) != 1:
        raise ValueError('ambiguous core archive member')
    subprocess.run(['ar', 'r', str(archive.resolve()), str(obj.resolve())], check=True)
    before = args.output / 'iotdb_fs-before'
    link = shlex.split((args.build / 'CMakeFiles/iotdb_fs.dir/link.txt').read_text())
    link[link.index('-o') + 1] = str(before.resolve())
    link[link.index('libiotdb_fs_core.a')] = str(archive.resolve())
    subprocess.run(link, cwd=args.build, check=True)
    after = args.output / 'iotdb_fs-after'
    shutil.copy2(args.build / 'bin/iotdb_fs', after)
    paths = {'source': original, 'overlay': overlay, 'before_core': archive,
             'after_core': args.build / 'libiotdb_fs_core.a', 'before': before, 'after': after}
    report = {'protocol': 'iotdbfs-fallback-join-ablation/1',
              'only_behavior_difference': 'TRUE fallback retains old attribute JOIN dependencies',
              'sha256': {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in paths.items()},
              'paths': {k: str(p.resolve()) for k, p in paths.items()}}
    (args.output / 'build-report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
