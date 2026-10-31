"""Audit-complete runner package; deployment writes only four new Python files.

Package mode includes sealed dependency bytes for audit. Verify/deploy mode reads
all existing dependency paths, including frozen work trees, but never writes them.
The product build and scientific data stay outside this script-only overlay.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import os

DEPLOY = ('experiments/fixed_matrix/hdf5_batch_client.py',
          'experiments/fixed_matrix/hdf5_cache_batch.py',
          'experiments/fixed_matrix/hdf5_cache_batch_package.py',
          'experiments/fixed_matrix/tests/test_hdf5_cache_batch.py')
PROTOCOL='iotdbfs-hdf5-cache-batch-runner-package/1'


def need(ok,message):
    if not ok:raise RuntimeError(message)


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def relative(name):
    p=Path(name)
    need(not p.is_absolute() and '..' not in p.parts and str(p)==name and name!='.', 'unsafe relative path')
    return p


def read(path):return json.loads(Path(path).read_text())


def package(repo,accepted,output):
    from .hdf5_cache_batch import closure, CURRENT_PINS
    repo=Path(repo).resolve(strict=True);accepted=Path(accepted).resolve(strict=True);output=Path(output)
    for name,expected in CURRENT_PINS.items():need(sha(accepted/name)==expected,'accepted evidence changed: '+name)
    original=read(accepted/'prepared.json')['sources'];sources={};payload={};origins={};local_drift={}
    for name,expected in original.items():
        relative(name);path=accepted/'source'/name
        need(sha(path)==expected,'accepted source snapshot changed: '+name)
        sources[name]=expected;payload[name]=path;origins[name]='accepted immutable executed source'
    for name,expected in closure(repo).items():
        relative(name);path=repo/name
        if name not in DEPLOY and name in sources and sources[name]!=expected:
            local_drift[name]=dict(local_sha256=expected,accepted_sha256=sources[name],policy='retain accepted bytes; server must already match')
            continue
        sources[name]=expected;payload[name]=path;origins[name]='current canonical source'
    for name in DEPLOY:
        path=repo/name;need(path.is_file() and not path.is_symlink(),'deployment script missing')
        sources[name]=sha(path);payload[name]=path;origins[name]='new script overlay'
    output.mkdir(mode=0o700,parents=True,exist_ok=False)
    manifest=dict(protocol=PROTOCOL,complete=True,deployment_files=list(DEPLOY),sources=sources,origins=origins,
        accepted_prepared_sha256=sha(accepted/'prepared.json'),accepted_evidence_pins=CURRENT_PINS,local_nondeployment_drift=local_drift,
        policy='Only deployment_files may be written. All other closure files must already match; archived work is read-only.')
    raw=json.dumps(manifest,sort_keys=True,indent=2).encode()+b'\n'
    (output/'manifest.json').write_bytes(raw)
    archive=output/'runner.tar.gz'
    with archive.open('xb') as file,gzip.GzipFile(fileobj=file,mode='wb',mtime=0) as zipped,tarfile.open(fileobj=zipped,mode='w') as tar:
        for name,data in [('manifest.json',raw)]+[('source/'+k,payload[k].read_bytes()) for k in sorted(payload)]:
            info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o600;info.mtime=0
            tar.addfile(info,io.BytesIO(data))
    result=dict(complete=True,archive=str(archive.resolve()),archive_sha256=sha(archive),
        manifest_sha256=sha(output/'manifest.json'),closure_files=len(sources),deployment_files=list(DEPLOY))
    (output/'package-receipt.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
    return result


def verify(repo,extracted,*,apply=False):
    repo=Path(repo).resolve(strict=True);extracted=Path(extracted).resolve(strict=True)
    manifest=read(extracted/'manifest.json')
    need(manifest.get('protocol')==PROTOCOL and manifest.get('complete') is True,'package protocol differs')
    need(tuple(manifest.get('deployment_files',[]))==DEPLOY,'deployment allowlist differs')
    sources=manifest.get('sources',{});need(isinstance(sources,dict) and set(DEPLOY)<=set(sources),'closure incomplete')
    for name,expected in sources.items():
        path=relative(name);source=extracted/'source'/path
        need(not source.is_symlink() and source.resolve(strict=True).is_relative_to(extracted) and sha(source)==expected,
             'package source differs: '+name)
        target=repo/path
        need(target.resolve(strict=False).is_relative_to(repo),'target escapes repository')
        need(not target.is_symlink(),'repository source is a symlink')
        if name not in DEPLOY:need(target.is_file() and sha(target)==expected,'existing dependency differs: '+name)
        elif target.exists():need(target.is_file() and sha(target)==expected,'new script exists with different bytes: '+name)
    if apply:
        for name in DEPLOY:
            target=repo/name;target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():continue
            fd,tmp=tempfile.mkstemp(prefix='.cache-batch-deploy-',dir=target.parent)
            try:
                with os.fdopen(fd,'wb') as stream:stream.write((extracted/'source'/name).read_bytes());stream.flush();os.fsync(stream.fileno())
                os.chmod(tmp,0o644);os.replace(tmp,target)
            finally:
                if Path(tmp).exists():Path(tmp).unlink()
            need(sha(target)==sources[name],'deployed bytes differ')
    return dict(complete=True,applied=apply,closure_files=len(sources),deployment_files=list(DEPLOY),
                existing_dependencies_unchanged=True,archived_work_written=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=('package','verify','deploy'));parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--accepted',type=Path);parser.add_argument('--output',type=Path);parser.add_argument('--extracted',type=Path)
    args=parser.parse_args()
    if args.mode=='package':
        need(args.accepted is not None and args.output is not None,'package requires --accepted --output')
        result=package(args.repo,args.accepted,args.output)
    else:
        need(args.extracted is not None,'verify/deploy requires --extracted')
        result=verify(args.repo,args.extracted,apply=args.mode=='deploy')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
