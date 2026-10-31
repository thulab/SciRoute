"""Read-only existing namespace inspector, with private SDK diagnostics."""
import json,subprocess,time
from pathlib import Path

def head(binary,prefix,env,root):
    root=Path(root);root.mkdir(mode=0o700)
    outpath=root/'head.stdout';errpath=root/'head.stderr'
    begin=time.perf_counter_ns()
    with outpath.open('x') as out,errpath.open('x') as err:
        outpath.chmod(0o600);errpath.chmod(0o600)
        result=subprocess.run([str(binary),'head','--prefix',prefix],env=env,stdin=subprocess.DEVNULL,stdout=out,stderr=err,text=True,timeout=60)
    if result.returncode:raise RuntimeError('read-only head inspector failed; private diagnostics retained')
    values=[json.loads(line) for line in outpath.read_text().splitlines() if line.startswith('{')]
    if len(values)!=1:raise RuntimeError('head inspector response missing')
    return values[0],dict(wall_ns=time.perf_counter_ns()-begin)
